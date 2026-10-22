import { once } from 'node:events'
import { createServer, type Socket } from 'node:net'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import LocalSubprocessRuntime, {
  BROKER_LAUNCHER_KEY, type BrokerLaunchRequest, type BrokerLaunchResult, type BrokerWorkerExit, type SubprocessBrokerLauncher,
} from '@deepseek-ai/dsh-subprocess-local'
import type { SubprocessSpawnSpec } from '@deepseek-ai/dsh-subprocess'

/** Define the launcher the way the CLI does: on the root, own, data, non-writable and non-configurable. */
function installLauncher(ctx: Context, launcher: SubprocessBrokerLauncher): void {
  Object.defineProperty(ctx.root, BROKER_LAUNCHER_KEY, {
    value: launcher, writable: false, configurable: false, enumerable: false,
  })
}

/** A launcher that records what it was asked and answers from a script. */
function scriptedLauncher(answer?: (request: BrokerLaunchRequest) => Promise<BrokerLaunchResult> | BrokerLaunchResult): {
  launcher: SubprocessBrokerLauncher
  requests: BrokerLaunchRequest[]
} {
  const requests: BrokerLaunchRequest[] = []
  const launcher: SubprocessBrokerLauncher = Object.freeze({
    launch: async (request: BrokerLaunchRequest): Promise<BrokerLaunchResult> => {
      requests.push(request)
      return answer === undefined ? exitingWith({ ok: true, code: 0, signal: null }) : await answer(request)
    },
  })
  return { launcher, requests }
}

function exitingWith(exit: BrokerWorkerExit): BrokerLaunchResult {
  return { ok: true, pid: 4242, workerId: 'w1', exited: Promise.resolve(exit), signal: () => Promise.resolve({ ok: true }) }
}

interface WireMessage { type: string; [key: string]: unknown }
interface Peer { socket: Socket; messages: WireMessage[]; waitFor(type: string): Promise<WireMessage>; send(value: object): void }

async function fakeBroker(onMessage: (message: WireMessage, peer: Peer) => void, registrationReply: 'valid' | 'invalid' | 'none' = 'valid'): Promise<{
  socketPath: string
  peer: Peer
  close(): Promise<void>
}> {
  const directory = mkdtempSync(join(tmpdir(), 'dsh-broker-client-'))
  const socketPath = join(directory, 'broker.sock')
  const messages: WireMessage[] = []
  const waiters = new Map<string, Array<(message: WireMessage) => void>>()
  let peerSocket: Socket | undefined
  let buffer = ''
  const peer: Peer = {
    get socket() { if (peerSocket === undefined) throw new Error('broker has no peer'); return peerSocket },
    messages,
    waitFor(type) {
      const existing = messages.find(message => message.type === type)
      if (existing !== undefined) return Promise.resolve(existing)
      return new Promise(resolve => {
        const entries = waiters.get(type) ?? []
        entries.push(resolve); waiters.set(type, entries)
      })
    },
    send(value) { peer.socket.write(`${JSON.stringify(value)}\n`) },
  }
  const server = createServer(socket => {
    peerSocket = socket
    socket.setEncoding('utf8')
    socket.on('data', (chunk: string) => {
      buffer += chunk
      while (true) {
        const end = buffer.indexOf('\n')
        if (end < 0) break
        const message = JSON.parse(buffer.slice(0, end)) as WireMessage
        buffer = buffer.slice(end + 1)
        messages.push(message)
        for (const resolve of waiters.get(message.type) ?? []) resolve(message)
        waiters.delete(message.type)
        if (message.type === 'register-core') {
          if (registrationReply !== 'none') peer.send({ type: 'registered', requestId: registrationReply === 'valid' ? message.requestId : 'wrong-request', revision: 1 })
          continue
        }
        onMessage(message, peer)
      }
    })
  })
  server.listen(socketPath)
  await once(server, 'listening')
  return {
    socketPath, peer,
    async close() {
      peerSocket?.destroy()
      await new Promise<void>(resolve => server.close(() => resolve()))
      rmSync(directory, { recursive: true, force: true })
    },
  }
}

function processSpec(overrides: Partial<SubprocessSpawnSpec> = {}): SubprocessSpawnSpec {
  return {
    argv: ['/bin/echo', 'hello'], cwd: '/assigned/tree',
    env: { DSH_EXAMPLE: 'label' },
    stdio: { stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' }, graceMs: 100,
    ...overrides,
  }
}

async function withRuntime<T>(
  socketPath: string,
  run: (ctx: Context, fiber: Awaited<ReturnType<Context['plugin']>>) => Promise<T>,
  sessionId: string | null = 'session-7',
  launcher: SubprocessBrokerLauncher = scriptedLauncher().launcher,
): Promise<T> {
  const ctx = new Context()
  ctx.provide('subprocessBrokerConfig', { socketPath })
  const execution = sessionId === null ? undefined : { active: true }
  ctx.provide('tools', {
    currentActiveExecution: () => execution,
    attestActiveExecution: () => ({ direct: true, sessionId }),
  })
  installLauncher(ctx, launcher)
  const fiber = await ctx.plugin(LocalSubprocessRuntime)
  try { return await run(ctx, fiber) } finally { await fiber.dispose() }
}

describe('local subprocess broker with the dynamic Host runner', () => {
  const REFUSAL = /broker mode cannot start while the dynamic Cordis Host runner is present/

  it('refuses construction before any broker connection when the runner service already exists', async () => {
    const broker = await fakeBroker(() => {})
    try {
      const ctx = new Context()
      ctx.provide('subprocessBrokerConfig', { socketPath: broker.socketPath })
      ctx.reflect.provide('dynamicCordisRunner', {})
      await expect((async () => { await ctx.plugin(LocalSubprocessRuntime) })()).rejects.toThrow(REFUSAL)
      expect(ctx.reflect.get('subprocess', false)).toBeUndefined()
      expect(broker.peer.messages).toHaveLength(0)
    } finally { await broker.close() }
  })

  it('refuses construction when the broker config provider is registered but not yet active', async () => {
    const ctx = new Context()
    let release!: () => void
    const gate = new Promise<void>(resolve => { release = resolve })
    const provider = ctx.plugin({
      name: 'pending-broker-config',
      apply: async (inner: Context) => {
        inner.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
        await gate
      },
    })
    try {
      await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
      expect(ctx.reflect.get('subprocessBrokerConfig')).toBeUndefined()
      await expect((async () => { await ctx.plugin(LocalSubprocessRuntime) })())
        .rejects.toThrow(/registered but not active; refusing local spawn/)
      expect(ctx.reflect.get('subprocess', false)).toBeUndefined()
    } finally {
      release()
      await (await provider).dispose()
    }
  })

  it('starts normally with a broker config and no runner', async () => {
    const broker = await fakeBroker(() => {})
    try {
      await withRuntime(broker.socketPath, async ctx => {
        expect(ctx.subprocess).toBeInstanceOf(LocalSubprocessRuntime)
      })
    } finally { await broker.close() }
  })

  it('starts normally with the runner and no broker config', async () => {
    const ctx = new Context()
    ctx.reflect.provide('dynamicCordisRunner', {})
    const fiber = await ctx.plugin(LocalSubprocessRuntime)
    try {
      expect(ctx.subprocess).toBeInstanceOf(LocalSubprocessRuntime)
    } finally { await fiber.dispose() }
  })
})

describe('local subprocess broker with the plugin manager', () => {
  const REFUSAL = /broker mode cannot start while the plugin manager is present/

  it('refuses construction before any broker connection when the manager service already exists', async () => {
    const broker = await fakeBroker(() => {})
    try {
      const ctx = new Context()
      ctx.provide('subprocessBrokerConfig', { socketPath: broker.socketPath })
      ctx.reflect.provide('pluginManager', {})
      await expect((async () => { await ctx.plugin(LocalSubprocessRuntime) })()).rejects.toThrow(REFUSAL)
      expect(ctx.reflect.get('subprocess', false)).toBeUndefined()
      expect(broker.peer.messages).toHaveLength(0)
    } finally { await broker.close() }
  })

  it('refuses construction when the manager service is registered but not yet active', async () => {
    const broker = await fakeBroker(() => {})
    const ctx = new Context()
    let release!: () => void
    const gate = new Promise<void>(resolve => { release = resolve })
    ctx.provide('subprocessBrokerConfig', { socketPath: broker.socketPath })
    const provider = ctx.plugin({
      name: 'pending-plugin-manager',
      apply: async (inner: Context) => {
        inner.provide('pluginManager', {})
        await gate
      },
    })
    try {
      await vi.waitFor(() => { expect(ctx.reflect.get('pluginManager', false)).toBeDefined() })
      expect(ctx.reflect.get('pluginManager')).toBeUndefined()
      await expect((async () => { await ctx.plugin(LocalSubprocessRuntime) })()).rejects.toThrow(REFUSAL)
      expect(ctx.reflect.get('subprocess', false)).toBeUndefined()
      expect(broker.peer.messages).toHaveLength(0)
    } finally {
      release()
      await (await provider).dispose()
      await broker.close()
    }
  })

  it('starts normally with the manager and no broker config', async () => {
    const ctx = new Context()
    ctx.reflect.provide('pluginManager', {})
    const fiber = await ctx.plugin(LocalSubprocessRuntime)
    try {
      expect(ctx.subprocess).toBeInstanceOf(LocalSubprocessRuntime)
    } finally { await fiber.dispose() }
  })
})

/** A started runtime that was constructed before any broker config existed. */
async function constructedWithoutBroker(): Promise<{
  ctx: Context
  runtime: LocalSubprocessRuntime
  localSpawn: ReturnType<typeof vi.fn>
  stop: () => Promise<void>
}> {
  const ctx = new Context()
  ctx.provide('tools', {
    currentActiveExecution: () => ({ active: true }),
    attestActiveExecution: () => ({ direct: true, sessionId: 'session-7' }),
  })
  installLauncher(ctx, scriptedLauncher().launcher)
  const fiber = await ctx.plugin(LocalSubprocessRuntime)
  const runtime = ctx.subprocess as LocalSubprocessRuntime
  const localSpawn = vi.fn()
  runtime.internals.spawn = localSpawn as never
  runtime.terminalInspector = { snapshot: vi.fn() } as never
  return { ctx, runtime, localSpawn, stop: async () => { await fiber.dispose() } }
}

const TERMINAL_SPEC = { argv: ['/bin/sh'], cwd: '/assigned/tree', rows: 24, cols: 80, terminalType: 'xterm', graceMs: 100 }

describe('local subprocess broker configured after construction', () => {
  it('routes spawn through a config provided after the constructor, and refuses spawnTerminal in broker mode', async () => {
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    try {
      ctx.reflect.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
      await expect(runtime.spawn(processSpec()).done).resolves.toEqual({ exitCode: 0, signal: null })
      await expect(runtime.spawnTerminal(TERMINAL_SPEC)).rejects.toThrow(/terminal sessions are not available in broker mode/)
      expect(localSpawn).not.toHaveBeenCalled()
    } finally { await stop() }
  })

  it('denies spawn and spawnTerminal while a late config provider is registered but not yet active', async () => {
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const provider = ctx.plugin({
      name: 'late-pending-broker-config',
      apply: async (inner: Context) => {
        inner.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
        await gate
      },
    })
    try {
      await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
      expect(ctx.reflect.get('subprocessBrokerConfig')).toBeUndefined()
      expect(() => runtime.spawn(processSpec())).toThrow(/broker configuration is not active; refusing local spawn/)
      await expect(runtime.spawnTerminal(TERMINAL_SPEC)).rejects.toThrow(/broker configuration is not active; refusing local spawn/)
      expect(localSpawn).not.toHaveBeenCalled()
    } finally {
      release()
      await (await provider).dispose()
      await stop()
    }
  })

  it('keeps denying after the provider is disposed and follows a replacement provider', async () => {
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    try {
      const disposeFirst = ctx.reflect.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/first.sock' })
      await expect(runtime.spawn(processSpec()).done).resolves.toEqual({ exitCode: 0, signal: null })
      await disposeFirst()
      expect(() => runtime.spawn(processSpec())).toThrow(/broker configuration is not active/)
      await expect(runtime.spawnTerminal(TERMINAL_SPEC)).rejects.toThrow(/broker configuration is not active/)
      ctx.reflect.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/second.sock' })
      await expect(runtime.spawn(processSpec()).done).resolves.toEqual({ exitCode: 0, signal: null })
      expect(localSpawn).not.toHaveBeenCalled()
    } finally { await stop() }
  })

  it.each([
    ['spawn', (runtime: LocalSubprocessRuntime) => { runtime.spawn(processSpec()) }],
    ['spawnTerminal', async (runtime: LocalSubprocessRuntime) => { await runtime.spawnTerminal(TERMINAL_SPEC) }],
  ] as const)('denies %s after a provider was provided and disposed with no launch in between', async (_api, launch) => {
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    try {
      const dispose = ctx.reflect.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
      await dispose()
      expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeUndefined()

      await expect((async () => { await launch(runtime) })()).rejects.toThrow(/broker configuration is not active; refusing local spawn/)
      expect(localSpawn).not.toHaveBeenCalled()
    } finally { await stop() }
  })

  it.each([
    ['spawn', (runtime: LocalSubprocessRuntime) => { runtime.spawn(processSpec()) }],
    ['spawnTerminal', async (runtime: LocalSubprocessRuntime) => { await runtime.spawnTerminal(TERMINAL_SPEC) }],
  ] as const)('denies %s after a provider that never became active was disposed with no launch in between', async (_api, launch) => {
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const provider = ctx.plugin({
      name: 'late-pending-then-disposed-broker-config',
      apply: async (inner: Context) => {
        inner.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
        await gate
      },
    })
    try {
      await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
      release()
      await (await provider).dispose()
      expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeUndefined()

      await expect((async () => { await launch(runtime) })()).rejects.toThrow(/broker configuration is not active; refusing local spawn/)
      expect(localSpawn).not.toHaveBeenCalled()
    } finally {
      release()
      await stop()
    }
  })

  it('denies launches of a runtime constructed while a provider was active once that provider is disposed', async () => {
    const ctx = new Context()
    ctx.provide('tools', {
      currentActiveExecution: () => ({ active: true }),
      attestActiveExecution: () => ({ direct: true, sessionId: 'session-7' }),
    })
    const dispose = ctx.reflect.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
    const fiber = await ctx.plugin(LocalSubprocessRuntime)
    const runtime = ctx.subprocess as LocalSubprocessRuntime
    const localSpawn = vi.fn()
    runtime.internals.spawn = localSpawn as never
    try {
      await dispose()
      expect(() => runtime.spawn(processSpec())).toThrow(/broker configuration is not active; refusing local spawn/)
      await expect(runtime.spawnTerminal(TERMINAL_SPEC)).rejects.toThrow(/broker configuration is not active; refusing local spawn/)
      expect(localSpawn).not.toHaveBeenCalled()
    } finally { await fiber.dispose() }
  })

  it('relies on Cordis keeping the declared service name after the provider is disposed', async () => {
    const ctx = new Context()
    const dispose = ctx.reflect.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
    await dispose()

    expect(ctx.reflect.props['subprocessBrokerConfig']).toEqual({ type: 'service' })
    expect(new Context().reflect.props['subprocessBrokerConfig']).toBeUndefined()
  })

  it('denies a late config that is not an absolute socket path', async () => {
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    try {
      ctx.reflect.provide('subprocessBrokerConfig', { socketPath: 'relative.sock' })
      expect(() => runtime.spawn(processSpec())).toThrow(/invalid trusted broker configuration/)
      await expect(runtime.spawnTerminal(TERMINAL_SPEC)).rejects.toThrow(/invalid trusted broker configuration/)
      expect(localSpawn).not.toHaveBeenCalled()
    } finally { await stop() }
  })

  it('keeps the ordinary local path when broker mode was never declared', { timeout: 30_000 }, async () => {
    const { runtime, stop } = await constructedWithoutBroker()
    delete runtime.internals.spawn
    try {
      const handle = runtime.spawn(processSpec({
        argv: [process.execPath, '-e', ''], cwd: process.cwd(),
        stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } },
      }))
      await expect(handle.done).resolves.toEqual({ exitCode: 0, signal: null })
    } finally { await stop() }
  })
})

describe('local subprocess broker route (S6 bridge: lease and start through the launcher, no connection of its own)', () => {
  async function inRuntime<T>(
    launcher: SubprocessBrokerLauncher,
    run: (ctx: Context, runtime: LocalSubprocessRuntime, localSpawn: ReturnType<typeof vi.fn>) => Promise<T> | T,
    sessionId: string | null = 'session-7',
  ): Promise<T> {
    return await withRuntime('/run/dsh-broker/broker.sock', async (ctx) => {
      const runtime = ctx.subprocess as LocalSubprocessRuntime
      const localSpawn = vi.fn()
      runtime.internals.spawn = localSpawn as never
      return await run(ctx, runtime, localSpawn)
    }, sessionId, launcher)
  }

  it('hands the launcher the attested execution and the literal request, and settles from the broker exit', async () => {
    const { launcher, requests } = scriptedLauncher(() => exitingWith({ ok: true, code: 7, signal: null }))
    await inRuntime(launcher, async (_ctx, runtime, localSpawn) => {
      const handle = runtime.spawn(processSpec({ env: { A: 'b' }, graceMs: 250 }))
      await expect(handle.done).resolves.toEqual({ exitCode: 7, signal: null })
      expect(requests).toEqual([{ execution: { active: true }, argv: ['/bin/echo', 'hello'], cwd: '/assigned/tree', env: expect.objectContaining({ A: 'b' }) as object, graceMs: 250, signal: expect.any(AbortSignal) as AbortSignal }])
      expect(localSpawn).not.toHaveBeenCalled()
    })
  })

  it('reports a signal death by name', async () => {
    const { launcher } = scriptedLauncher(() => exitingWith({ ok: true, code: null, signal: 'SIGKILL' }))
    await inRuntime(launcher, async (_ctx, runtime) => {
      await expect(runtime.spawn(processSpec()).done).resolves.toEqual({ exitCode: null, signal: 'SIGKILL' })
    })
  })

  it('does not name the session to the launcher: the request carries the execution and nothing that says who it is', async () => {
    const { launcher, requests } = scriptedLauncher()
    await inRuntime(launcher, async (_ctx, runtime) => {
      await runtime.spawn(processSpec()).done
      expect(JSON.stringify(Object.keys(requests[0]!).sort())).toBe(JSON.stringify(['argv', 'cwd', 'env', 'execution', 'graceMs', 'signal']))
      expect(JSON.stringify(requests[0])).not.toContain('session-7')
    })
  })

  it('returns a pending handle at once: lease and start finish behind it, then the exit', async () => {
    const started = Promise.withResolvers<BrokerLaunchResult>()
    const { launcher } = scriptedLauncher(() => started.promise)
    await inRuntime(launcher, async (_ctx, runtime) => {
      const handle = runtime.spawn(processSpec())
      let settled = false
      void handle.done.then(() => { settled = true }, () => { settled = true })
      await new Promise(resolve => setTimeout(resolve, 30))
      expect(settled).toBe(false)
      const exit = Promise.withResolvers<BrokerWorkerExit>()
      started.resolve({ ok: true, pid: 9, workerId: 'w', exited: exit.promise, signal: () => Promise.resolve({ ok: true }) })
      await new Promise(resolve => setTimeout(resolve, 30))
      expect(settled).toBe(false)
      exit.resolve({ ok: true, code: 0, signal: null })
      await expect(handle.done).resolves.toEqual({ exitCode: 0, signal: null })
    })
  })

  it('rejects the handle when the broker refuses, with the reason, and never runs locally', async () => {
    const { launcher } = scriptedLauncher(() => ({ ok: false, reason: 'no-binding' }))
    await inRuntime(launcher, async (_ctx, runtime, localSpawn) => {
      await expect(runtime.spawn(processSpec()).done).rejects.toThrow(/broker refused the launch \(no-binding\)/)
      expect(localSpawn).not.toHaveBeenCalled()
    })
  })

  it('rejects the handle when the connection is lost before the broker reported the end of the process', async () => {
    const { launcher } = scriptedLauncher(() => exitingWith({ ok: false, reason: 'control-closed' }))
    await inRuntime(launcher, async (_ctx, runtime) => {
      await expect(runtime.spawn(processSpec()).done).rejects.toThrow(/connection ended before the process was reported done \(control-closed\)/)
    })
  })

  it('treats a launcher that throws as a refusal of the handle', async () => {
    const { launcher } = scriptedLauncher(() => { throw new Error('boom') })
    await inRuntime(launcher, async (_ctx, runtime) => {
      await expect(runtime.spawn(processSpec()).done).rejects.toThrow(/boom/)
    })
  })

  it('loses stdout, stderr and stdin in v1: collected output is empty, the streams end, input is discarded, the status is real', async () => {
    const { launcher } = scriptedLauncher(() => exitingWith({ ok: true, code: 3, signal: null }))
    await inRuntime(launcher, async (_ctx, runtime) => {
      const piped = runtime.spawn(processSpec())
      piped.stdin?.write('discarded')
      const chunks: Buffer[] = []
      piped.stdout?.on('data', (chunk: Buffer) => { chunks.push(chunk) })
      piped.stderr?.resume()
      await expect(piped.done).resolves.toEqual({ exitCode: 3, signal: null })
      await piped.waitForExit()
      // The streams end with the worker, so a reader that waits for the end is not left hanging.
      await vi.waitFor(() => { expect(piped.stdout?.readableEnded).toBe(true) })
      await vi.waitFor(() => { expect(piped.stderr?.readableEnded).toBe(true) })
      expect(Buffer.concat(chunks).toString()).toBe('')
      const collected = runtime.spawn(processSpec({ stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } } }))
      await collected.done
      await collected.waitForExit()
      expect(collected.stdout?.readableEnded ?? true).toBe(true)
    })
  })

  it('says once that output is not captured', async () => {
    const { launcher } = scriptedLauncher()
    await inRuntime(launcher, async (ctx, runtime) => {
      const warn = vi.spyOn(ctx.logger, 'warn')
      await runtime.spawn(processSpec()).done
      await runtime.spawn(processSpec()).done
      expect(warn.mock.calls.filter(([message]) => /not captured/.test(String(message)))).toHaveLength(1)
    })
  })

  /** A launcher whose worker records the signals it is sent and exits when the script says so. */
  function workerLauncher(): {
    launcher: SubprocessBrokerLauncher
    requests: BrokerLaunchRequest[]
    signals: string[]
    exit: PromiseWithResolvers<BrokerWorkerExit>
    started: PromiseWithResolvers<BrokerLaunchResult>
    start: () => void
  } {
    const exit = Promise.withResolvers<BrokerWorkerExit>()
    const started = Promise.withResolvers<BrokerLaunchResult>()
    const signals: string[] = []
    const { launcher, requests } = scriptedLauncher(() => started.promise)
    const start = (): void => {
      started.resolve({
        ok: true, pid: 9, workerId: 'w', exited: exit.promise,
        signal: (name) => { signals.push(name); return Promise.resolve({ ok: true }) },
      })
    }
    return { launcher, requests, signals, exit, started, start }
  }

  it('terminate() sends TERM through the launcher, then KILL after graceMs, and the broker exit still settles the handle', async () => {
    const { launcher, signals, exit, start } = workerLauncher()
    start()
    await inRuntime(launcher, async (_ctx, runtime) => {
      const handle = runtime.spawn(processSpec({ graceMs: 60 }))
      await new Promise(resolve => setTimeout(resolve, 20))
      expect(signals).toEqual([])
      handle.terminate()
      await vi.waitFor(() => { expect(signals).toEqual(['TERM']) })
      await new Promise(resolve => setTimeout(resolve, 30))
      expect(signals).toEqual(['TERM'])
      await vi.waitFor(() => { expect(signals).toEqual(['TERM', 'KILL']) })
      exit.resolve({ ok: true, code: null, signal: 'SIGKILL' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGKILL' })
    })
  })

  it('does not escalate to KILL when the worker exits after TERM', async () => {
    const { launcher, signals, exit, start } = workerLauncher()
    start()
    await inRuntime(launcher, async (_ctx, runtime) => {
      const handle = runtime.spawn(processSpec({ graceMs: 80 }))
      await new Promise(resolve => setTimeout(resolve, 20))
      handle.terminate()
      await vi.waitFor(() => { expect(signals).toEqual(['TERM']) })
      exit.resolve({ ok: true, code: null, signal: 'SIGTERM' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
      await handle.waitForExit()
      await new Promise(resolve => setTimeout(resolve, 150))
      expect(signals).toEqual(['TERM'])
    })
  })

  it('an abort of the caller signal stops the worker the same way', async () => {
    const { launcher, signals, exit, start } = workerLauncher()
    start()
    await inRuntime(launcher, async (_ctx, runtime) => {
      const abort = new AbortController()
      const handle = runtime.spawn(processSpec({ graceMs: 500, signal: abort.signal }))
      await new Promise(resolve => setTimeout(resolve, 20))
      abort.abort(new Error('tool timeout'))
      await vi.waitFor(() => { expect(signals).toEqual(['TERM']) })
      exit.resolve({ ok: true, code: null, signal: 'SIGTERM' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
    })
  })

  it('aborts the launch request when termination is asked before the worker exists, and a launcher that gives up ends the handle as terminated', async () => {
    const { launcher, requests, started } = workerLauncher()
    await inRuntime(launcher, async (_ctx, runtime) => {
      const handle = runtime.spawn(processSpec({ graceMs: 500 }))
      expect(requests[0]!.signal!.aborted).toBe(false)
      handle.terminate()
      expect(requests[0]!.signal!.aborted).toBe(true)
      started.resolve({ ok: false, reason: 'aborted' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
    })
  })

  it('an abort of the caller signal also aborts the launch request', async () => {
    const { launcher, requests, started } = workerLauncher()
    await inRuntime(launcher, async (_ctx, runtime) => {
      const abort = new AbortController()
      const handle = runtime.spawn(processSpec({ graceMs: 500, signal: abort.signal }))
      abort.abort(new Error('cancelled'))
      expect(requests[0]!.signal!.aborted).toBe(true)
      started.resolve({ ok: false, reason: 'aborted' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
    })
  })

  it('delivers a termination asked for while the worker was starting, as soon as there is a worker, and KILL wins', async () => {
    const { launcher, signals, exit, started, start } = workerLauncher()
    await inRuntime(launcher, async (_ctx, runtime) => {
      const handle = runtime.spawn(processSpec({ graceMs: 40 }))
      await new Promise(resolve => setTimeout(resolve, 10))
      handle.terminate()
      // TERM and then, after graceMs, KILL both come due before the broker has answered the start.
      await new Promise(resolve => setTimeout(resolve, 100))
      expect(signals).toEqual([])
      start()
      await vi.waitFor(() => { expect(signals).toEqual(['KILL']) })
      exit.resolve({ ok: true, code: null, signal: 'SIGKILL' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGKILL' })
      expect(started).toBeDefined()
    })
  })

  it('reports SIGKILL for a launch that never started when KILL had been asked', async () => {
    const { launcher, started } = workerLauncher()
    await inRuntime(launcher, async (_ctx, runtime) => {
      const handle = runtime.spawn(processSpec({ graceMs: 20 }))
      handle.terminate()
      await new Promise(resolve => setTimeout(resolve, 80))
      started.resolve({ ok: false, reason: 'aborted' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGKILL' })
    })
  })

  it('a signal that cannot be delivered does not throw out of the termination request', async () => {
    const exit = Promise.withResolvers<BrokerWorkerExit>()
    const { launcher } = scriptedLauncher(() => ({
      ok: true, pid: 9, workerId: 'w', exited: exit.promise,
      signal: () => Promise.reject(new Error('socket closed')),
    }))
    await inRuntime(launcher, async (_ctx, runtime) => {
      const handle = runtime.spawn(processSpec({ graceMs: 30 }))
      await new Promise(resolve => setTimeout(resolve, 10))
      expect(() => { handle.terminate() }).not.toThrow()
      await new Promise(resolve => setTimeout(resolve, 80))
      exit.resolve({ ok: true, code: null, signal: 'SIGKILL' })
      await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGKILL' })
    })
  })

  describe('disposal with a live worker (F5)', () => {
    async function liveRuntime(launcher: SubprocessBrokerLauncher, marginMs: number): Promise<{ runtime: LocalSubprocessRuntime; fiber: Awaited<ReturnType<Context['plugin']>> }> {
      const ctx = new Context()
      ctx.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
      ctx.provide('tools', { currentActiveExecution: () => ({ active: true }), attestActiveExecution: () => ({ direct: true, sessionId: 'session-7' }) })
      installLauncher(ctx, launcher)
      const fiber = await ctx.plugin(LocalSubprocessRuntime)
      const runtime = ctx.subprocess as LocalSubprocessRuntime
      runtime.internals.brokerExitMarginMs = marginMs
      return { runtime, fiber }
    }

    it('signals the worker and finishes once the broker reports it gone', async () => {
      const { launcher, signals, exit, start } = workerLauncher()
      start()
      const { runtime, fiber } = await liveRuntime(launcher, 2000)
      const handle = runtime.spawn(processSpec({ graceMs: 50 }))
      void handle.done.catch(() => {})
      await new Promise(resolve => setTimeout(resolve, 20))
      const disposing = fiber.dispose()
      await vi.waitFor(() => { expect(signals).toEqual(['TERM']) })
      exit.resolve({ ok: true, code: null, signal: 'SIGTERM' })
      await disposing
    })

    it('escalates to KILL, and gives up with an error when the worker is never reported gone', async () => {
      const { launcher, signals, start } = workerLauncher()
      start()
      const { runtime, fiber } = await liveRuntime(launcher, 100)
      const handle = runtime.spawn(processSpec({ graceMs: 50 }))
      void handle.done.catch(() => {})
      await new Promise(resolve => setTimeout(resolve, 20))
      const began = Date.now()
      handle.terminate()
      await expect(handle.waitForExit()).rejects.toThrow(/did not report the process gone within 150 ms/)
      expect(Date.now() - began).toBeLessThan(2000)
      expect(signals).toEqual(['TERM', 'KILL'])
      // Disposal reports that error and ends; it does not wait for ever.
      await fiber.dispose()
    })

    it('is bounded as well when the broker never even answered the start', async () => {
      const { launcher } = workerLauncher()
      const { runtime, fiber } = await liveRuntime(launcher, 100)
      const handle = runtime.spawn(processSpec({ graceMs: 50 }))
      void handle.done.catch(() => {})
      await new Promise(resolve => setTimeout(resolve, 20))
      const began = Date.now()
      await fiber.dispose()
      // 50 ms of grace and 100 ms of margin: ended by the bound, not by the (never answering) broker.
      expect(Date.now() - began).toBeGreaterThanOrEqual(100)
      expect(Date.now() - began).toBeLessThan(3000)
    })

    it('does not wait at all, and does not time anything out, for a worker that is already done', async () => {
      const { launcher } = scriptedLauncher(() => exitingWith({ ok: true, code: 0, signal: null }))
      const { runtime, fiber } = await liveRuntime(launcher, 100)
      await runtime.spawn(processSpec()).done
      await fiber.dispose()
    })
  })

  it('says why the broker refused a launch it could not take, with the limit and no value', async () => {
    const { launcher } = scriptedLauncher(() => ({
      ok: false, reason: 'invalid-argv', detail: 'argument 2 is 5000 characters long; the broker accepts at most 4096 per argument',
    }))
    await inRuntime(launcher, async (_ctx, runtime) => {
      await expect(runtime.spawn(processSpec()).done).rejects.toThrow(
        'subprocess-local: the broker refused the launch (invalid-argv: argument 2 is 5000 characters long; the broker accepts at most 4096 per argument)',
      )
    })
  })

  it('refuses a control channel, a terminal and a DSH_SESSION_ID override before the launcher is asked', async () => {
    const { launcher, requests } = scriptedLauncher()
    await inRuntime(launcher, async (_ctx, runtime, localSpawn) => {
      expect(() => runtime.spawn(processSpec({ stdio: { stdin: 'ignore', stdout: 'pipe', stderr: 'pipe', control: 'pipe' } })))
        .toThrow(/no control channel/)
      expect(() => runtime.spawn(processSpec({ env: { DSH_SESSION_ID: 'session-8' } }))).toThrow(/DSH_SESSION_ID cannot be set for broker launches/)
      await expect(runtime.spawnTerminal(TERMINAL_SPEC)).rejects.toThrow(/terminal sessions are not available in broker mode/)
      expect(requests).toHaveLength(0)
      expect(localSpawn).not.toHaveBeenCalled()
    })
  })

  it('denies a missing active identity, and an attestation with no session, without asking the launcher', async () => {
    const { launcher, requests } = scriptedLauncher()
    await inRuntime(launcher, (ctx) => {
      expect(() => ctx.subprocess.spawn(processSpec())).toThrow(/requires an active ToolRuntime execution/)
    }, null)
    const ctx = new Context()
    ctx.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
    ctx.provide('tools', { currentActiveExecution: () => ({ active: true }), attestActiveExecution: () => ({ direct: true, sessionId: '' }) })
    installLauncher(ctx, launcher)
    const fiber = await ctx.plugin(LocalSubprocessRuntime)
    try {
      expect(() => ctx.subprocess.spawn(processSpec())).toThrow(/Core-attested agent session/)
    } finally { await fiber.dispose() }
    expect(requests).toHaveLength(0)
  })

  it('opens no connection of its own: the broker socket is never touched by this package', async () => {
    const broker = await fakeBroker(() => {})
    try {
      const { launcher } = scriptedLauncher()
      await withRuntime(broker.socketPath, async (ctx) => {
        await ctx.subprocess.spawn(processSpec()).done
      }, 'session-7', launcher)
      expect(broker.peer.messages).toHaveLength(0)
    } finally { await broker.close() }
  })
})

describe('the broker launcher belongs to the launcher, not to a plugin (S3 review, S2 part B; S6 bridge)', () => {
  /** A runtime in broker mode whose root has no launcher unless `prepare` defines one. */
  async function runtimeWith(
    prepare: (ctx: Context) => void,
    run: (ctx: Context, runtime: LocalSubprocessRuntime) => void | Promise<void>,
  ) {
    const ctx = new Context()
    ctx.provide('subprocessBrokerConfig', { socketPath: '/run/dsh-broker/broker.sock' })
    ctx.provide('tools', {
      currentActiveExecution: () => ({ active: true }),
      attestActiveExecution: () => ({ direct: true, sessionId: 'session-7' }),
    })
    prepare(ctx)
    const fiber = await ctx.plugin(LocalSubprocessRuntime)
    try {
      const runtime = ctx.subprocess as LocalSubprocessRuntime
      runtime.internals.spawn = vi.fn() as never
      await run(ctx, runtime)
    } finally { await fiber.dispose() }
  }

  it('never asks a launcher or resolver a plugin provides under a Context name, with or without the real one', async () => {
    const hostile = { launch: vi.fn(() => Promise.resolve({ ok: true })), resolve: vi.fn(() => 'attacker-assignment') }
    await runtimeWith(() => {}, (ctx, runtime) => {
      for (const name of ['subprocessBrokerLauncher', 'subprocessBrokerAssignmentResolver']) ctx.reflect.provide(name as never, hostile as never)
      expect(() => runtime.spawn(processSpec())).toThrow(/launcher is unavailable/)
    })
    const real = scriptedLauncher(() => ({ ok: false, reason: 'no-binding' }))
    await runtimeWith(ctx => { installLauncher(ctx, real.launcher) }, async (ctx, runtime) => {
      for (const name of ['subprocessBrokerLauncher', 'subprocessBrokerAssignmentResolver']) ctx.reflect.provide(name as never, hostile as never)
      await expect(runtime.spawn(processSpec()).done).rejects.toThrow(/no-binding/)
    })
    expect(hostile.launch).not.toHaveBeenCalled()
    expect(hostile.resolve).not.toHaveBeenCalled()
  })

  it('accepts only an own, locked data property of the root: not a replaceable, accessor or inherited one', async () => {
    const hostile = { launch: vi.fn(() => Promise.resolve({ ok: true })) }
    const define = (ctx: Context, descriptor: PropertyDescriptor): void => {
      Object.defineProperty(ctx.root, BROKER_LAUNCHER_KEY, descriptor)
    }
    for (const descriptor of [
      { value: hostile, writable: true, configurable: false },
      { value: hostile, writable: false, configurable: true },
      { get: () => hostile, configurable: false },
    ] as PropertyDescriptor[]) {
      await runtimeWith(ctx => { define(ctx, descriptor) }, (_ctx, runtime) => {
        expect(() => runtime.spawn(processSpec())).toThrow(/launcher is unavailable/)
      })
    }
    await runtimeWith(ctx => {
      Object.defineProperty(Object.getPrototypeOf(ctx.root) as object, BROKER_LAUNCHER_KEY, {
        value: hostile, writable: false, configurable: true,
      })
    }, (ctx, runtime) => {
      try { expect(() => runtime.spawn(processSpec())).toThrow(/launcher is unavailable/) }
      finally { Reflect.deleteProperty(Object.getPrototypeOf(ctx.root) as object, BROKER_LAUNCHER_KEY) }
    })
    await runtimeWith(ctx => { define(ctx, { value: { launch: 'not a function' }, writable: false, configurable: false }) }, (_ctx, runtime) => {
      expect(() => runtime.spawn(processSpec())).toThrow(/launcher is unavailable/)
    })
    expect(hostile.launch).not.toHaveBeenCalled()
  })

  it('cannot be replaced after the launcher defined it', async () => {
    const hostile = { launch: vi.fn(() => Promise.resolve({ ok: true })) }
    await runtimeWith(ctx => { installLauncher(ctx, scriptedLauncher(() => ({ ok: false, reason: 'no-binding' })).launcher) }, async (ctx, runtime) => {
      const root = ctx.root as unknown as Record<symbol, unknown>
      expect(() => { root[BROKER_LAUNCHER_KEY] = hostile }).toThrow(TypeError)
      expect(() => Object.defineProperty(root, BROKER_LAUNCHER_KEY, { value: hostile })).toThrow(TypeError)
      expect(Reflect.deleteProperty(root, BROKER_LAUNCHER_KEY)).toBe(false)
      await expect(runtime.spawn(processSpec()).done).rejects.toThrow(/no-binding/)
    })
    expect(hostile.launch).not.toHaveBeenCalled()
  })
})
