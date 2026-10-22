/**
 * The whole bridge, with no fake in the middle of Core: the local subprocess runtime asks the launcher's installed
 * broker launcher, which leases and starts over the real control client, against a broker on a Unix socket that
 * answers in the S6 wire format. The broker side is a script; everything on Core's side is the shipped code.
 */
import { realpathSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { Context } from '@deepseek-ai/cordis'
import LocalSubprocessRuntime from '@deepseek-ai/dsh-subprocess-local'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { createBindingHost, type BindingHost } from '../src/broker-binding.ts'
import { BrokerControlClient } from '../src/broker-client.ts'
import { installBrokerLauncher } from '../src/broker-capabilities.ts'
import { broker, cleanups, type FakeServer, type Frame } from './socket-broker.ts'

afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup() })

const CWD = realpathSync(tmpdir())

interface Rig {
  ctx: Context
  host: BindingHost
  runtime: LocalSubprocessRuntime
  server: FakeServer
  client: BrokerControlClient
  fiber: Awaited<ReturnType<Context['plugin']>>
}

/** Answer the way the S6 broker does; `onStart` decides what a start does. */
interface RigOptions {
  /** Frame types the script does not answer by itself; the test answers them. */
  readonly hold?: readonly string[]
  /** Called for each `signal` frame, after it was answered `signalled`. */
  readonly onSignal?: (frame: Frame, server: FakeServer) => void
  readonly client?: ConstructorParameters<typeof BrokerControlClient>[1]
}

async function rig(onStart: (frame: Frame, server: FakeServer) => void, options: RigOptions = {}): Promise<Rig> {
  const server = await broker()
  const answers: Record<string, (frame: Frame) => object> = {
    bind: frame => ({ type: 'bound', requestId: frame.requestId, bindingId: 'b0ffee11', revision: 1 }),
    lease: frame => ({ type: 'leased', requestId: frame.requestId, assignmentId: 'Zm9v-1', revision: 1, ttlMs: 5000 }),
    revoke: frame => ({ type: 'revoked', requestId: frame.requestId, bindingId: frame.bindingId, count: 1 }),
    signal: frame => ({ type: 'signalled', version: 1, requestId: frame.requestId, workerId: frame.workerId }),
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
  const client = new BrokerControlClient(server.socketPath, options.client)
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
        if (frame.type === 'start') { if (!options.hold?.includes('start')) onStart(frame, server) }
        else if (answers[frame.type] !== undefined && !options.hold?.includes(frame.type)) {
          server.send(answers[frame.type]!(frame))
          if (frame.type === 'signal') options.onSignal?.(frame, server)
        }
      }
      await new Promise(resolve => setTimeout(resolve, 5))
      if (client.closed) return
    }
  })()
  return { ctx, host, runtime: ctx.subprocess as LocalSubprocessRuntime, server, client, fiber }
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

const signalsOf = (server: FakeServer): string[] => server.frames.filter(frame => frame.type === 'signal').map(frame => `${String(frame.workerId)}:${String(frame.signal)}`)
const quiet = async (ms: number): Promise<void> => { await new Promise(resolve => setTimeout(resolve, ms)) }

describe('stopping a brokered worker (S6 contract 11)', () => {
  const bindSession = async (host: BindingHost): Promise<void> => {
    expect(await host.issuer.issue({ sessionId: 'session-7', kind: 'attach', orcaInstanceId: 'instance-1' })).toMatchObject({ ok: true })
  }

  it('never starts the worker when the caller aborts before the lease is answered', async () => {
    const { host, runtime, server } = await rig(() => { throw new Error('start must not be sent') }, { hold: ['lease'] })
    await bindSession(host)
    const abort = new AbortController()
    const handle = runtime.spawn({ ...spec, signal: abort.signal })
    const lease = await server.next('lease')
    abort.abort(new Error('tool cancelled'))
    server.send({ type: 'leased', requestId: lease.requestId, assignmentId: 'Zm9v-1', revision: 1 })
    await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
    await quiet(150)
    expect(server.frames.map(frame => frame.type)).toEqual(['register-core', 'bind', 'lease'])
  })

  it('never starts it when terminate() is called before the lease is answered', async () => {
    const { host, runtime, server } = await rig(() => { throw new Error('start must not be sent') }, { hold: ['lease'] })
    await bindSession(host)
    const handle = runtime.spawn(spec)
    const lease = await server.next('lease')
    handle.terminate()
    server.send({ type: 'leased', requestId: lease.requestId, assignmentId: 'Zm9v-1', revision: 1 })
    await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
    await quiet(150)
    expect(server.frames.some(frame => frame.type === 'start')).toBe(false)
  })

  it('sends TERM, then KILL after graceMs, to a running worker, and worker-exited settles the handle', async () => {
    const { host, runtime, server } = await rig(
      (frame, self) => { self.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' }) },
      { onSignal: (frame, self) => { if (frame.signal === 'KILL') self.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ffee11', code: null, signal: 'SIGKILL' }) } },
    )
    await bindSession(host)
    const abort = new AbortController()
    const handle = runtime.spawn({ ...spec, graceMs: 120, signal: abort.signal })
    await server.next('start')
    await quiet(30)
    abort.abort(new Error('tool timeout'))
    await vi.waitFor(() => { expect(signalsOf(server)).toEqual(['w1:TERM']) })
    await quiet(60)
    expect(signalsOf(server)).toEqual(['w1:TERM'])
    await vi.waitFor(() => { expect(signalsOf(server)).toEqual(['w1:TERM', 'w1:KILL']) })
    await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGKILL' })
    expect(server.frames.filter(frame => frame.type === 'signal').every(frame => /^[0-9a-f-]{36}$/u.test(frame.requestId) && frame.version === 1)).toBe(true)
  })

  it('does not send KILL when the worker exits after TERM', async () => {
    const { host, runtime, server } = await rig(
      (frame, self) => { self.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' }) },
      { onSignal: (frame, self) => { if (frame.signal === 'TERM') self.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ffee11', code: null, signal: 'SIGTERM' }) } },
    )
    await bindSession(host)
    const handle = runtime.spawn({ ...spec, graceMs: 80 })
    await server.next('start')
    await quiet(20)
    handle.terminate()
    await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
    await handle.waitForExit()
    await quiet(200)
    expect(signalsOf(server)).toEqual(['w1:TERM'])
  })

  it('delivers an abort that came while the broker was still starting the worker, once it has started', async () => {
    const { host, runtime, server } = await rig(() => {}, {
      hold: ['start'],
      onSignal: (frame, self) => { if (frame.signal === 'TERM') self.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ffee11', code: null, signal: 'SIGTERM' }) },
    })
    await bindSession(host)
    const handle = runtime.spawn({ ...spec, graceMs: 2000 })
    const start = await server.next('start')
    handle.terminate()
    await quiet(40)
    expect(signalsOf(server)).toEqual([])
    server.send({ type: 'started', requestId: start.requestId, pid: 4321, workerId: 'w1' })
    await vi.waitFor(() => { expect(signalsOf(server)).toEqual(['w1:TERM']) })
    await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
  })

  it('stops a live worker when the runtime is disposed, and the disposal ends', async () => {
    const { host, runtime, server, fiber } = await rig(
      (frame, self) => { self.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' }) },
      { onSignal: (frame, self) => { if (frame.signal === 'TERM') self.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ffee11', code: null, signal: 'SIGTERM' }) } },
    )
    await bindSession(host)
    const handle = runtime.spawn({ ...spec, graceMs: 2000 })
    void handle.done.catch(() => {})
    await server.next('start')
    await quiet(20)
    const began = Date.now()
    await fiber.dispose()
    expect(Date.now() - began).toBeLessThan(1500)
    expect(signalsOf(server)).toEqual(['w1:TERM'])
  })

  it('refuses a launch the broker would reject by shape, says which limit, never sends it, and never repeats a value', async () => {
    const { host, runtime, server } = await rig(() => { throw new Error('start must not be sent') })
    await bindSession(host)
    await expect(runtime.spawn({ ...spec, argv: ['/bin/sh', '-c', 'x'.repeat(5000)] }).done)
      .rejects.toThrow(/\(invalid-argv: argument 2 is 5000 characters long; the broker accepts at most 4096 per argument/u)
    const secret = 'hunter2-top-secret'
    const bad = runtime.spawn({ ...spec, env: { 'BASH_FUNC_x%%': secret } })
    await expect(bad.done).rejects.toThrow(/\(invalid-env: environment variable name "BASH_FUNC_x%%" is not accepted/u)
    await bad.done.catch((error: unknown) => { expect((error as Error).message).not.toContain(secret) })
    expect(server.frames.some(frame => frame.type === 'start')).toBe(false)
  })

  it('tells the issuer when the connection is lost: the binding is unusable at once, and nothing hangs', async () => {
    const { host, runtime, server } = await rig((frame, self) => { self.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' }) })
    await bindSession(host)
    const handle = runtime.spawn(spec)
    void handle.done.catch(() => {})
    await server.next('start')
    await quiet(80)
    expect(host.issuer.lookup('session-7')).toMatchObject({ bindingId: 'b0ffee11' })
    server.peer().destroy()
    await expect(handle.done).rejects.toThrow(/connection ended before the process was reported done \(control-closed\)/u)
    expect(host.issuer.lookup('session-7')).toEqual({ ok: false, reason: 'no-binding' })
    expect(await runtime.spawn(spec).done.catch((error: unknown) => (error as Error).message)).toMatch(/refused the launch \(no-binding\)/u)
  })
})
