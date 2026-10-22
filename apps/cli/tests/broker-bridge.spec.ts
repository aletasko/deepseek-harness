/**
 * The whole bridge, with no fake in the middle of Core: the local subprocess runtime asks the launcher's installed
 * broker launcher, which leases and starts over the real control client, against a broker on a Unix socket that
 * answers in the S6 wire format. The broker side is a script; everything on Core's side is the shipped code.
 */
import { realpathSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { Context } from '@deepseek-ai/cordis'
import LocalSubprocessRuntime from '@deepseek-ai/dsh-subprocess-local'
import { afterEach, describe, expect, it } from 'vitest'
import { createBindingHost, type BindingHost } from '../src/broker-binding.ts'
import { BrokerControlClient } from '../src/broker-client.ts'
import { installBrokerLauncher } from '../src/broker-capabilities.ts'
import { broker, cleanups, type FakeServer, type Frame } from './socket-broker.ts'

afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup() })

const CWD = realpathSync(tmpdir())

interface Rig { ctx: Context; host: BindingHost; runtime: LocalSubprocessRuntime; server: FakeServer; client: BrokerControlClient }

/** Answer the way the S6 broker does; `onStart` decides what a start does. */
async function rig(onStart: (frame: Frame, server: FakeServer) => void): Promise<Rig> {
  const server = await broker()
  const answers: Record<string, (frame: Frame) => object> = {
    bind: frame => ({ type: 'bound', requestId: frame.requestId, bindingId: 'b0ffee11', revision: 1 }),
    lease: frame => ({ type: 'leased', requestId: frame.requestId, assignmentId: 'Zm9v-1', revision: 1, ttlMs: 5000 }),
    revoke: frame => ({ type: 'revoked', requestId: frame.requestId, bindingId: frame.bindingId, count: 1 }),
  }
  const ctx = new Context()
  ctx.provide('loader', { entries: () => [] } as never)
  ctx.provide('subprocessBrokerConfig', { socketPath: server.socketPath })
  const session = { header: { cwd: CWD } }
  ctx.provide('sessions', { get: (id: string) => id === 'session-7' ? session : undefined } as never)
  ctx.provide('tools', {
    currentActiveExecution: () => ({ active: true }),
    attestActiveExecution: () => ({ direct: true, sessionId: 'session-7' }),
  } as never)
  const client = new BrokerControlClient(server.socketPath)
  const host = createBindingHost({ ctx, broker: { socketPath: server.socketPath }, client })
  installBrokerLauncher(ctx, host)
  host.gate.arm()
  const fiber = await ctx.plugin(LocalSubprocessRuntime)
  cleanups.push(async () => { await fiber.dispose(); client.closeControl(); await ctx.fiber.dispose() })
  // The script: react to each frame as it arrives.
  void (async () => {
    for (let seen = 0; ; ) {
      const frames = server.frames.slice(seen)
      seen = server.frames.length
      for (const frame of frames) {
        if (frame.type === 'start') onStart(frame, server)
        else if (answers[frame.type] !== undefined) server.send(answers[frame.type]!(frame))
      }
      await new Promise(resolve => setTimeout(resolve, 5))
      if (client.closed) return
    }
  })()
  return { ctx, host, runtime: ctx.subprocess as LocalSubprocessRuntime, server, client }
}

const spec = { argv: ['/bin/echo', 'hello'], cwd: CWD, env: { A: 'b' }, stdio: { stdin: 'ignore' as const, stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } }, graceMs: 100 }

describe('runtime to launcher to control client to broker', () => {
  it('leases and starts over the one connection and settles from worker-exited', async () => {
    const { host, runtime, server } = await rig((frame, self) => {
      self.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' })
      setTimeout(() => { self.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ffee11', code: 5, signal: null }) }, 30)
    })
    expect(await host.issuer.issue({ sessionId: 'session-7', kind: 'attach', orcaInstanceId: 'instance-1' })).toEqual({ ok: true, bindingId: 'b0ffee11', revision: 1 })
    const handle = runtime.spawn(spec)
    await expect(handle.done).resolves.toEqual({ exitCode: 5, signal: null })
    expect(server.connections).toHaveLength(1)
    expect(server.frames.map(frame => frame.type)).toEqual(['register-core', 'bind', 'lease', 'start'])
    const [, bind, lease, start] = server.frames
    expect(bind).toMatchObject({ sessionId: 'session-7', kind: 'attach', cwd: CWD, orca: { instanceId: 'instance-1' } })
    expect(lease).toMatchObject({ bindingId: 'b0ffee11', revision: 1, mode: 'process' })
    expect(start).toMatchObject({
      mode: 'process', assignmentId: 'Zm9v-1', argv: ['/bin/echo', 'hello'], cwd: CWD,
      stdio: { stdin: { mode: 'ignore' }, stdout: { mode: 'ignore' }, stderr: { mode: 'ignore' } },
    })
    expect(JSON.stringify(start)).not.toContain('session-7')
  })

  it('rejects the handle with the broker reason when the broker refuses the start', async () => {
    const { host, runtime } = await rig((frame, self) => {
      self.send({ type: 'error', code: 'start-refused', message: 'start refused', requestId: frame.requestId, reason: 'stdio-unsupported' })
    })
    await host.issuer.issue({ sessionId: 'session-7', kind: 'attach', orcaInstanceId: 'instance-1' })
    await expect(runtime.spawn(spec).done).rejects.toThrow(/broker refused the launch \(stdio-unsupported\)/)
  })

  it('rejects the handle when the connection drops while the process runs', async () => {
    const { host, runtime, server } = await rig((frame, self) => {
      self.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' })
      setTimeout(() => { server.peer().destroy() }, 30)
    })
    await host.issuer.issue({ sessionId: 'session-7', kind: 'attach', orcaInstanceId: 'instance-1' })
    await expect(runtime.spawn(spec).done).rejects.toThrow(/connection ended before the process was reported done \(control-closed\)/)
  })

  it('rejects the handle with no-binding, and sends no lease or start, for a session that was never bound', async () => {
    const { runtime, server } = await rig(() => {})
    await expect(runtime.spawn(spec).done).rejects.toThrow(/broker refused the launch \(no-binding\)/)
    await new Promise(resolve => setTimeout(resolve, 20))
    expect(server.frames.map(frame => frame.type)).toEqual(['register-core'])
  })
})
