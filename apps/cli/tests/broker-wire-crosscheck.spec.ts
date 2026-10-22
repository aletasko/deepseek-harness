import { spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, describe, expect, it } from 'vitest'
import { BrokerControlClient } from '../src/broker-client.ts'
import { parseInbound, refusalOf, toAssignmentGrant, toBindingGrant } from '../src/broker-wire.ts'
import { broker, cleanups, type FakeServer, type Frame } from './socket-broker.ts'

/**
 * Cross-check: the frames Core's real client puts on a real socket are handed to the root broker's own parser
 * (`regia/broker/protocol.py`) under `python3 -I -B`. The broker checkout is `DSH_BROKER_REGIA_DIR`, or the `regia`
 * directory next to this repository. Without python3, or without that checkout, the suite is skipped and says why;
 * it never passes silently. `DSH_BROKER_S6_STRICT=1` additionally refuses a parser that predates contract point 3
 * (a `child` bind with neither `cwd` nor `orca`).
 */
const here = dirname(fileURLToPath(import.meta.url))
const probe = join(here, 'broker_parser_probe.py')
const regia = process.env['DSH_BROKER_REGIA_DIR'] ?? resolve(here, '..', '..', '..', '..', 'regia')
const python = spawnSync('python3', ['-I', '-B', '--version'], { encoding: 'utf8' })
const missing: string | undefined = python.error !== undefined || python.status !== 0
  ? 'python3 is not available'
  : existsSync(join(regia, 'broker', 'protocol.py')) ? undefined : `no broker checkout at ${regia} (set DSH_BROKER_REGIA_DIR)`

interface Verdict { name: string; accepted: boolean; error?: string }

function brokerParses(cases: ReadonlyArray<{ name: string; frame: unknown }>): Map<string, Verdict> {
  const run = spawnSync('python3', ['-I', '-B', probe, regia], { input: JSON.stringify(cases), encoding: 'utf8', timeout: 60_000 })
  if (run.status !== 0) throw new Error(`broker parser probe failed (${run.status}): ${run.stderr}`)
  return new Map((JSON.parse(run.stdout) as Verdict[]).map(entry => [entry.name, entry]))
}

afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup() })

/** Drive the real client through every request kind and return the frames the server saw, by name. */
async function coreFrames(server: FakeServer): Promise<Map<string, Frame>> {
  const control = new BrokerControlClient(server.socketPath)
  cleanups.push(() => { control.closeControl() })
  const reply = async (type: string, nth: number, answer: (frame: Frame) => object): Promise<Frame> => {
    const frame = await server.next(type, nth)
    server.send(answer(frame))
    return frame
  }
  const seated = control.bind({
    sessionId: 'session-1', workspaceId: 'workspace-1', kind: 'room-seat', cwd: '/srv/worktrees/a', orca: { instanceId: 'instance-9' },
    orgId: 'org-1', room: { origine: 'dsh', stanzaId: 'room-1', agente: 'agent-1', installazione: 'inst-1' },
  })
  const roomSeat = await reply('bind', 1, f => ({ type: 'bound', requestId: f.requestId, bindingId: 'b1', revision: 1 }))
  await seated
  const attached = control.bind({ sessionId: 'session-2', workspaceId: 'workspace-2', kind: 'attach', cwd: '/srv/worktrees/b', orca: { instanceId: 'instance-8' } })
  const attach = await reply('bind', 2, f => ({ type: 'bound', requestId: f.requestId, bindingId: 'b2', revision: 2 }))
  await attached
  const kid = control.bind({ sessionId: 'session-3', workspaceId: 'workspace-1', kind: 'child', parentBindingId: 'b1' })
  const child = await reply('bind', 3, f => ({ type: 'bound', requestId: f.requestId, bindingId: 'b3', revision: 3 }))
  await kid
  const leased = control.assign({ sessionId: 'session-1', bindingId: 'b1', revision: 1 })
  const lease = await reply('lease', 1, f => ({ type: 'leased', requestId: f.requestId, assignmentId: 'opaque-1', revision: 1, ttlMs: 5000 }))
  await leased
  const started = control.start({ assignmentId: 'opaque-1', argv: ['/bin/echo', 'hi'], cwd: '/srv/worktrees/a', env: { A: 'b' }, graceMs: 200 })
  const start = await reply('start', 1, f => ({ type: 'started', requestId: f.requestId, pid: 7, workerId: 'w1' }))
  await started
  const revoked = control.revoke({ sessionId: 'session-1', bindingId: 'b1' })
  const revoke = await reply('revoke', 1, f => ({ type: 'revoked', requestId: f.requestId, bindingId: 'b1', count: 2 }))
  await revoked
  const status = control.status()
  const statusFrame = await reply('status', 1, f => ({ type: 'status', requestId: f.requestId, bindings: 0, leases: 0, undrained: 0, epoch: 7, workers: [] }))
  await status
  return new Map<string, Frame>([
    ['register-core', await server.next('register-core')], ['bind room-seat', roomSeat], ['bind attach', attach], ['bind child', child],
    ['lease', lease], ['start', start], ['revoke', revoke], ['status', statusFrame],
  ])
}

// A skip that says why in its own title, so a report cannot show it as a pass.
if (missing !== undefined) {
  describe('frames Core produces, parsed by the broker (python3 -I -B)', () => {
    it.skip(`SKIPPED: ${missing}`, () => {})
  })
}

describe.skipIf(missing !== undefined)('frames Core produces, parsed by the broker (python3 -I -B, regia/broker/protocol.py)', () => {
  it('accepts every request kind Core sends', async () => {
    const frames = await coreFrames(await broker())
    const verdicts = brokerParses([...frames].map(([name, frame]) => ({ name, frame })))
    const strict = process.env['DSH_BROKER_S6_STRICT'] === '1'
    for (const [name] of frames) {
      const verdict = verdicts.get(name)!
      if (name === 'bind child' && !verdict.accepted && !strict) {
        // A parser from before S6 contract point 3 insists on `orca` for a child. Everything else about the frame
        // must still be right: the same frame with a locator added is accepted.
        console.warn(`broker-wire-crosscheck: this broker predates contract point 3 (child without orca): ${verdict.error}`)
        const withOrca = brokerParses([{ name, frame: { ...frames.get(name), orca: { instanceId: 'instance-8' } } }]).get(name)!
        expect(withOrca, 'child frame with an orca locator added').toMatchObject({ accepted: true })
        continue
      }
      expect(verdict, `${name}: ${verdict.error ?? ''}`).toMatchObject({ accepted: true })
    }
  })

  it('rejects the frames Core used to produce, so the oracle can tell the difference', async () => {
    const frames = await coreFrames(await broker())
    const get = (name: string): Frame => frames.get(name)!
    const cases = [
      { name: 'coreEpoch on bind', frame: { ...get('bind room-seat'), coreEpoch: 'epoch-A' } },
      { name: 'Core room names', frame: { ...get('bind room-seat'), room: { origine: 'dsh', stanzaId: 'r', agente: 'a', installazione: 'i' } } },
      { name: 'sessionId on revoke', frame: { ...get('revoke'), sessionId: 'session-1' } },
      { name: 'coreEpoch on revoke', frame: { ...get('revoke'), coreEpoch: 'e' } },
      { name: 'lease without mode', frame: Object.fromEntries(Object.entries(get('lease')).filter(([key]) => key !== 'mode')) },
      { name: 'sessionId on lease', frame: { ...get('lease'), sessionId: 'session-1' } },
      { name: 'cwd on child', frame: { ...get('bind child'), cwd: '/srv/worktrees/a' } },
      { name: 'attach without cwd', frame: Object.fromEntries(Object.entries(get('bind attach')).filter(([key]) => key !== 'cwd')) },
      { name: 'attach without orca', frame: Object.fromEntries(Object.entries(get('bind attach')).filter(([key]) => key !== 'orca')) },
      { name: 'stdio pipe on start is parsed but not v1', frame: { ...get('start'), mode: 'terminal' } },
    ]
    const verdicts = brokerParses(cases)
    for (const { name } of cases.filter(entry => entry.name !== 'stdio pipe on start is parsed but not v1')) {
      expect(verdicts.get(name), name).toMatchObject({ accepted: false })
    }
  })
})

describe.skipIf(missing !== undefined)('frames the broker\'s own daemon code builds, parsed by Core', () => {
  it('accepts every answer kind and translates it', async () => {
    const frames = await coreFrames(await broker())
    const run = spawnSync('python3', ['-I', '-B', join(here, 'broker_response_probe.py'), regia], {
      input: JSON.stringify(Object.fromEntries(frames)), encoding: 'utf8', timeout: 60_000,
    })
    if (run.status !== 0) throw new Error(`broker response probe failed (${run.status}): ${run.stderr}`)
    const answers = JSON.parse(run.stdout) as Record<string, unknown>

    const bound = parseInbound(answers['bind room-seat'])
    expect(bound.type === 'bound' && toBindingGrant(bound)).toEqual({ ok: true, bindingId: 'b0ffee00'.repeat(4), revision: 3 })
    expect(bound).toMatchObject({ requestId: frames.get('bind room-seat')!.requestId })
    const leased = parseInbound(answers['lease'])
    expect(leased.type === 'leased' && toAssignmentGrant(leased)).toEqual({ ok: true, assignmentId: 'Zm9vYmFy_-1', revision: 1 })
    expect(parseInbound(answers['revoke'])).toMatchObject({ type: 'revoked', bindingId: 'b1' })
    expect(parseInbound(answers['start'])).toMatchObject({ type: 'started', pid: 4242, workerId: 'a1b2c3d4e5f6' })
    expect(parseInbound(answers['status'])).toMatchObject({ type: 'status', status: { epoch: 7, bindings: 2 } })
    expect(parseInbound(answers['registered'])).toMatchObject({ type: 'registered', revision: 7 })
    if (answers['worker-exited code'] !== undefined) {
      expect(parseInbound(answers['worker-exited code'])).toMatchObject({ code: 0, signal: null })
      expect(parseInbound(answers['worker-exited SIGKILL'])).toMatchObject({ code: null, signal: 'SIGKILL' })
      expect(parseInbound(answers['worker-exited SIG65'])).toMatchObject({ code: null, signal: 'SIG65' })
    } else expect(process.env['DSH_BROKER_S6_STRICT'], 'a broker without worker-exited predates contract point 7').not.toBe('1')

    const refusals: Array<[string, string]> = [
      ['bind refused', 'cwd-mismatch'], ['start refused', 'stdio-unsupported'], ['revoke unconfirmed', 'timeout'],
      ['busy', 'too-many-in-flight'], ['control-active', 'control-active'], ['peer denied', 'peer-denied'], ['invalid frame', 'invalid-frame'],
    ]
    for (const [name, reason] of refusals) {
      const frame = parseInbound(answers[name])
      expect(frame.type === 'error' && refusalOf(frame), name).toEqual({ ok: false, reason })
    }
  })
})
