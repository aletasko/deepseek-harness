import { describe, expect, it } from 'vitest'
import type { BindingRequest } from '../src/broker-binding.ts'
import {
  bindFrame, leaseFrame, parseInbound, refusalOf, registerFrame, revokeFrame, signalFrame, startFrame, startRequestProblem, statusFrame,
  toAssignmentGrant, toBindingGrant, WireViolation,
} from '../src/broker-wire.ts'

const ID = '3f2b8f0e-9c1d-4b57-8a43-1d2e3f4a5b6c'

/**
 * Golden frames: the exact objects Core puts on the wire for each request, written out by hand from the S6 contract
 * and the broker parser (`regia/broker/protocol.py`). A change to a builder that alters one of these must be a
 * deliberate change of the contract.
 */
describe('frames Core sends (S6 contract 2-5)', () => {
  it('register-core carries only type, version and requestId; there is no coreEpoch', () => {
    expect(registerFrame(ID)).toEqual({ type: 'register-core', version: 1, requestId: ID })
  })

  it('bind room-seat: top-level cwd, orca locator, room renamed at the edge', () => {
    const request: BindingRequest = {
      sessionId: 'session-1', workspaceId: 'workspace-1', kind: 'room-seat', cwd: '/srv/worktrees/a',
      orca: { instanceId: 'instance-9' }, orgId: 'org-1',
      room: { origine: 'dsh', stanzaId: 'room-1', agente: 'agent-1', installazione: 'inst-1' },
    }
    expect(bindFrame(request, ID)).toEqual({
      type: 'bind', version: 1, requestId: ID, sessionId: 'session-1', workspaceId: 'workspace-1', kind: 'room-seat',
      cwd: '/srv/worktrees/a', orca: { instanceId: 'instance-9' }, orgId: 'org-1',
      room: { origin: 'dsh', roomId: 'room-1', agentId: 'agent-1', installationId: 'inst-1' },
    })
  })

  it('bind attach: cwd and orca, nothing else', () => {
    const frame = bindFrame({ sessionId: 's', workspaceId: 'w', kind: 'attach', cwd: '/x', orca: { instanceId: 'i-1' } }, ID)
    expect(frame).toEqual({
      type: 'bind', version: 1, requestId: ID, sessionId: 's', workspaceId: 'w', kind: 'attach', cwd: '/x', orca: { instanceId: 'i-1' },
    })
  })

  it('bind child: parentBindingId, and neither cwd nor orca', () => {
    const frame = bindFrame({ sessionId: 'kid', workspaceId: 'w', kind: 'child', parentBindingId: 'binding-parent' }, ID)
    expect(frame).toEqual({
      type: 'bind', version: 1, requestId: ID, sessionId: 'kid', workspaceId: 'w', kind: 'child', parentBindingId: 'binding-parent',
    })
    expect('cwd' in frame).toBe(false)
    expect('orca' in frame).toBe(false)
  })

  it('copies named fields only: nothing extra on a request reaches the wire', () => {
    const forged = {
      sessionId: 's', workspaceId: 'w', kind: 'attach', cwd: '/x', orca: { instanceId: 'i', path: '/etc' }, coreEpoch: 'e',
      assignmentId: 'forged', room: undefined, env: { A: 'b' },
    } as unknown as BindingRequest
    expect(bindFrame(forged, ID)).toEqual({
      type: 'bind', version: 1, requestId: ID, sessionId: 's', workspaceId: 'w', kind: 'attach', cwd: '/x', orca: { instanceId: 'i' },
    })
  })

  it('revoke is {bindingId} with no sessionId and no epoch', () => {
    expect(revokeFrame({ sessionId: 'session-1', bindingId: 'b-1' }, ID)).toEqual({ type: 'revoke', version: 1, requestId: ID, bindingId: 'b-1' })
  })

  it('lease is {bindingId, revision, mode: "process"}; terminal cannot be asked for', () => {
    const frame = leaseFrame({ sessionId: 'session-1', bindingId: 'b-1', revision: 3 }, ID)
    expect(frame).toEqual({ type: 'lease', version: 1, requestId: ID, bindingId: 'b-1', revision: 3, mode: 'process' })
    expect(leaseFrame({ sessionId: 's', bindingId: 'b', revision: 1, mode: 'terminal' } as never, ID).mode).toBe('process')
  })

  it('start is mode process with ignore for all three streams (v1)', () => {
    const frame = startFrame({ assignmentId: 'opaque_1-A', argv: ['/bin/echo', 'x'], cwd: '/srv/wt', env: { A: 'b' }, graceMs: 250 }, ID)
    expect(frame).toEqual({
      type: 'start', version: 1, requestId: ID, mode: 'process', assignmentId: 'opaque_1-A', argv: ['/bin/echo', 'x'], cwd: '/srv/wt',
      env: { A: 'b' }, stdio: { stdin: { mode: 'ignore' }, stdout: { mode: 'ignore' }, stderr: { mode: 'ignore' } }, graceMs: 250,
    })
  })

  it('status is just the envelope', () => {
    expect(statusFrame(ID)).toEqual({ type: 'status', version: 1, requestId: ID })
  })
})

describe('start requests the broker would reject by shape are refused before they are sent', () => {
  const base = { assignmentId: 'a-1', argv: ['/bin/true'], cwd: '/srv', env: {}, graceMs: 100 }
  it('accepts the baseline', () => { expect(startRequestProblem(base)).toBeUndefined() })
  it.each([
    ['assignment id with a slash', { ...base, assignmentId: 'a/b' }, 'invalid-assignment'],
    ['empty argv', { ...base, argv: [] }, 'invalid-argv'],
    ['empty program', { ...base, argv: [''] }, 'invalid-argv'],
    ['too many arguments', { ...base, argv: Array.from({ length: 129 }, () => 'x') }, 'invalid-argv'],
    ['relative cwd', { ...base, cwd: 'srv' }, 'invalid-cwd'],
    ['NUL in cwd', { ...base, cwd: '/srv\0' }, 'invalid-cwd'],
    ['bad env name', { ...base, env: { 'A-B': 'x' } }, 'invalid-env'],
    ['over-long env value', { ...base, env: { A: 'x'.repeat(4097) } }, 'invalid-env'],
    ['DSH_SESSION_ID', { ...base, env: { dsh_session_id: 'x' } }, 'invalid-env'],
    ['DSH_BROKER_ prefix', { ...base, env: { DSH_BROKER_X: 'x' } }, 'invalid-env'],
    ['zero grace', { ...base, graceMs: 0 }, 'invalid-grace'],
    ['frame over 256 KiB', { ...base, argv: Array.from({ length: 128 }, () => 'x'.repeat(4096)), env: Object.fromEntries(Array.from({ length: 256 }, (_, i) => [`V${i}`, 'y'.repeat(4096)])) }, 'frame-too-large'],
  ])('%s', (_name, request, problem) => { expect(startRequestProblem(request)?.reason).toBe(problem) })
})

describe('answers from the broker (S6 contract 6, fail closed)', () => {
  it('translates bound, leased and revoked into Core grant types', () => {
    const bound = parseInbound({ type: 'bound', requestId: ID, bindingId: 'b0ff', revision: 2 })
    expect(bound.type === 'bound' && toBindingGrant(bound)).toEqual({ ok: true, bindingId: 'b0ff', revision: 2 })
    const leased = parseInbound({ type: 'leased', requestId: ID, assignmentId: 'Zm9v-_9', revision: 2, ttlMs: 5000 })
    expect(leased.type === 'leased' && toAssignmentGrant(leased)).toEqual({ ok: true, assignmentId: 'Zm9v-_9', revision: 2 })
    expect(parseInbound({ type: 'revoked', requestId: ID, bindingId: 'b0ff', count: 1 })).toEqual({ type: 'revoked', requestId: ID, bindingId: 'b0ff' })
  })

  it('translates started, registered, worker-exited and status', () => {
    expect(parseInbound({ type: 'registered', requestId: ID, revision: 7 })).toEqual({ type: 'registered', requestId: ID, revision: 7 })
    expect(parseInbound({ type: 'started', requestId: ID, pid: 99, workerId: 'a1b2c3' })).toEqual({ type: 'started', requestId: ID, pid: 99, workerId: 'a1b2c3' })
    expect(parseInbound({ type: 'worker-exited', workerId: 'a1b2c3', bindingId: 'b0ff', code: 0, signal: null }))
      .toEqual({ type: 'worker-exited', workerId: 'a1b2c3', bindingId: 'b0ff', code: 0, signal: null })
    // The broker reports a signal death as a number or a name; Core reports the name.
    expect(parseInbound({ type: 'worker-exited', workerId: 'a1', bindingId: 'b0ff', code: null, signal: 9 }))
      .toMatchObject({ code: null, signal: 'SIGKILL' })
    expect(parseInbound({ type: 'worker-exited', workerId: 'a1', bindingId: 'b0ff', code: null, signal: 'SIGTERM' }))
      .toMatchObject({ signal: 'SIGTERM' })
    expect(parseInbound({ type: 'status', requestId: ID, bindings: 1, leases: 0, undrained: 0, epoch: 3, workers: [] }))
      .toMatchObject({ type: 'status', status: { bindings: 1, epoch: 3, workers: [] } })
  })

  it('turns an error frame into a refusal carrying the broker reason, or the code when there is none', () => {
    const withReason = parseInbound({ type: 'error', code: 'bind-refused', message: 'bind refused', requestId: ID, reason: 'cwd-mismatch' })
    expect(withReason.type === 'error' && refusalOf(withReason)).toEqual({ ok: false, reason: 'cwd-mismatch' })
    const bare = parseInbound({ type: 'error', code: 'invalid-frame', message: 'invalid start frame' })
    expect(bare.type === 'error' && refusalOf(bare)).toEqual({ ok: false, reason: 'invalid-frame' })
  })

  it.each([
    ['an unknown frame type', { type: 'granted', requestId: ID }],
    ['no type', { requestId: ID }],
    ['an unknown field on bound', { type: 'bound', requestId: ID, bindingId: 'b', revision: 1, ok: true }],
    ['a missing field on bound', { type: 'bound', requestId: ID, bindingId: 'b' }],
    ['revision 0', { type: 'bound', requestId: ID, bindingId: 'b', revision: 0 }],
    ['a float revision', { type: 'bound', requestId: ID, bindingId: 'b', revision: 1.5 }],
    ['a binding id outside the label shape', { type: 'bound', requestId: ID, bindingId: 'b/../x', revision: 1 }],
    ['a request id that is not a UUIDv4', { type: 'bound', requestId: 'req-1', bindingId: 'b', revision: 1 }],
    ['an unknown field on leased', { type: 'leased', requestId: ID, assignmentId: 'a', revision: 1, mode: 'process' }],
    ['an assignment id with a slash', { type: 'leased', requestId: ID, assignmentId: 'a/b', revision: 1 }],
    ['pid 0', { type: 'started', requestId: ID, pid: 0, workerId: 'w' }],
    ['an unknown field on started', { type: 'started', requestId: ID, pid: 1, workerId: 'w', cgroup: '/x' }],
    ['a worker-exited with a request id', { type: 'worker-exited', requestId: ID, workerId: 'w', bindingId: 'b', code: 0, signal: null }],
    ['a worker-exited with a made-up signal', { type: 'worker-exited', workerId: 'w', bindingId: 'b', code: null, signal: 'KILL; rm' }],
    ['an error with free text as its code', { type: 'error', code: 'No way!', message: 'x' }],
    ['an error with an unknown field', { type: 'error', code: 'x', message: 'x', detail: '/etc' }],
    ['a registered frame without an epoch', { type: 'registered', requestId: ID }],
    ['an array', []],
    ['null', null],
  ])('refuses %s', (_name, frame) => { expect(() => parseInbound(frame)).toThrow(WireViolation) })
})

describe('signal and signalled (S6 contract 11)', () => {
  it('signal carries only type, version, requestId, workerId and the signal name', () => {
    expect(signalFrame('w1', 'TERM', ID)).toEqual({ type: 'signal', version: 1, requestId: ID, workerId: 'w1', signal: 'TERM' })
    expect(signalFrame('w1', 'KILL', ID)).toEqual({ type: 'signal', version: 1, requestId: ID, workerId: 'w1', signal: 'KILL' })
  })

  it('parses signalled, with or without version, and refuses a malformed one', () => {
    expect(parseInbound({ type: 'signalled', requestId: ID, workerId: 'w1' })).toEqual({ type: 'signalled', requestId: ID, workerId: 'w1' })
    expect(parseInbound({ type: 'signalled', version: 1, requestId: ID, workerId: 'a1b2c3d4e5f6' })).toEqual({ type: 'signalled', requestId: ID, workerId: 'a1b2c3d4e5f6' })
    for (const frame of [
      { type: 'signalled', requestId: ID },
      { type: 'signalled', requestId: ID, workerId: 'w/1' },
      { type: 'signalled', requestId: ID, workerId: 'w1', signal: 'KILL' },
      { type: 'signalled', requestId: 'req-1', workerId: 'w1' },
    ]) expect(() => parseInbound(frame)).toThrow(WireViolation)
  })
})

describe('version on answers (F3)', () => {
  const answers: Array<[string, Record<string, unknown>]> = [
    ['registered', { type: 'registered', requestId: ID, revision: 7 }],
    ['bound', { type: 'bound', requestId: ID, bindingId: 'b0ff', revision: 2 }],
    ['leased', { type: 'leased', requestId: ID, assignmentId: 'Zm9v-_9', revision: 2, ttlMs: 5000 }],
    ['revoked', { type: 'revoked', requestId: ID, bindingId: 'b0ff', count: 1 }],
    ['started', { type: 'started', requestId: ID, pid: 99, workerId: 'a1b2c3' }],
    ['signalled', { type: 'signalled', requestId: ID, workerId: 'a1b2c3' }],
    ['status', { type: 'status', requestId: ID, bindings: 1, workers: [] }],
    ['error', { type: 'error', code: 'bind-refused', reason: 'cwd-mismatch', message: 'bind refused', requestId: ID }],
    ['error without a request id', { type: 'error', code: 'peer-denied', reason: 'peer-not-authorised', message: 'peer denied' }],
    ['worker-exited', { type: 'worker-exited', workerId: 'a1', bindingId: 'b0ff', code: 0, signal: null }],
  ]

  it.each(answers)('%s: version 1 is accepted, and the parsed frame is the same as without it', (_name, frame) => {
    expect(parseInbound({ ...frame, version: 1 })).toEqual(parseInbound(frame))
  })

  it.each(answers)('%s: any other version is a violation', (_name, frame) => {
    for (const version of [0, 2, '1', 1.5, null, true, [1], { v: 1 }]) {
      expect(() => parseInbound({ ...frame, version }), JSON.stringify(version)).toThrow(WireViolation)
    }
  })

  it('does not leak version into the relayed status', () => {
    expect(parseInbound({ type: 'status', version: 1, requestId: ID, bindings: 1, epoch: 3, workers: [] }))
      .toEqual({ type: 'status', requestId: ID, status: { bindings: 1, epoch: 3, workers: [] } })
  })

  it('still refuses an unknown field next to a valid version', () => {
    expect(() => parseInbound({ type: 'bound', version: 1, requestId: ID, bindingId: 'b', revision: 1, ok: true })).toThrow(WireViolation)
  })
})

describe('the reason a start is refused says what to change and never repeats a value (F4)', () => {
  const base = { assignmentId: 'a-1', argv: ['/bin/true'], cwd: '/srv', env: {}, graceMs: 100 }

  it('names the argument, its length and the limit', () => {
    expect(startRequestProblem({ ...base, argv: ['/bin/sh', '-c', 'x'.repeat(5000)] })).toEqual({
      reason: 'invalid-argv',
      detail: expect.stringMatching(
        /^argument 2 is 5000 characters long; the broker accepts at most 4096 per argument \(.*file in the workspace/u,
      ) as string,
    })
  })

  it('names the environment variable, not its value', () => {
    const problem = startRequestProblem({ ...base, env: { 'BASH_FUNC_x%%': 'hunter2' } })
    expect(problem?.reason).toBe('invalid-env')
    expect(problem?.detail).toContain('"BASH_FUNC_x%%"')
    expect(problem?.detail).not.toContain('hunter2')
    const long = startRequestProblem({ ...base, env: { A: 'hunter2'.repeat(1000) } })
    expect(long?.detail).toContain('"A"')
    expect(long?.detail).not.toContain('hunter2')
    expect(startRequestProblem({ ...base, env: { DSH_BROKER_X: 'x' } })?.detail).toMatch(/reserved for the broker/u)
  })

  it('says how many arguments or variables there are against the limit', () => {
    expect(startRequestProblem({ ...base, argv: Array.from({ length: 129 }, () => 'x') })?.detail).toMatch(/129 arguments; the broker accepts 1 to 128/u)
    expect(startRequestProblem({ ...base, env: Object.fromEntries(Array.from({ length: 257 }, (_, i) => [`V${i}`, 'y'])) })?.detail).toMatch(/257 variables.*at most 256/u)
  })

  it('does not put an argument in the detail', () => {
    const problem = startRequestProblem({ ...base, argv: ['/bin/sh', 'secret-token\0'] })
    expect(problem?.reason).toBe('invalid-argv')
    expect(problem?.detail).not.toContain('secret-token')
  })
})

