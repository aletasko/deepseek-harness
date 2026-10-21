import { once } from 'node:events'
import { createServer, type Socket } from 'node:net'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import LocalSubprocessRuntime from '@deepseek-ai/dsh-subprocess-local'
import type { SubprocessSpawnSpec } from '@deepseek-ai/dsh-subprocess'

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

async function withRuntime<T>(socketPath: string, run: (ctx: Context, fiber: Awaited<ReturnType<Context['plugin']>>) => Promise<T>, sessionId: string | null = 'session-7'): Promise<T> {
  const ctx = new Context()
  ctx.provide('subprocessBrokerConfig', { socketPath })
  const execution = sessionId === null ? undefined : { active: true }
  ctx.provide('tools', {
    currentActiveExecution: () => execution,
    attestActiveExecution: () => ({ direct: true, sessionId }),
  })
  ctx.provide('subprocessBrokerAssignmentResolver', {
    resolve: (id: string) => id === 'session-7' ? 'trusted-assignment-7' : id === 'session-8' ? 'trusted-assignment-8' : undefined,
  })
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

/** Broker that accepts one start and finishes the process or terminal immediately. */
function acceptingBroker(label: string): ReturnType<typeof fakeBroker> {
  return fakeBroker((message, peer) => {
    if (message.type !== 'start') return
    peer.send({ type: 'accepted', handle: label, pid: 4242 })
    peer.send({ type: 'outcome', handle: label, exitCode: 0, signal: null })
    peer.send({ type: 'empty', handle: label })
  })
}

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
  ctx.provide('subprocessBrokerAssignmentResolver', { resolve: (id: string) => id === 'session-7' ? 'trusted-assignment-7' : undefined })
  const fiber = await ctx.plugin(LocalSubprocessRuntime)
  const runtime = ctx.subprocess as LocalSubprocessRuntime
  const localSpawn = vi.fn()
  runtime.internals.spawn = localSpawn as never
  runtime.terminalInspector = { snapshot: vi.fn() } as never
  return { ctx, runtime, localSpawn, stop: async () => { await fiber.dispose() } }
}

const TERMINAL_SPEC = { argv: ['/bin/sh'], cwd: '/assigned/tree', rows: 24, cols: 80, terminalType: 'xterm', graceMs: 100 }

describe('local subprocess broker configured after construction', () => {
  it('routes spawn and spawnTerminal through a config provided after the constructor', async () => {
    const broker = await acceptingBroker('late-config')
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    try {
      ctx.reflect.provide('subprocessBrokerConfig', { socketPath: broker.socketPath })
      await expect(runtime.spawn(processSpec()).done).resolves.toEqual({ exitCode: 0, signal: null })
      await expect(broker.peer.waitFor('start')).resolves.toMatchObject({ mode: 'process', assignmentId: 'trusted-assignment-7' })
      const terminal = await runtime.spawnTerminal(TERMINAL_SPEC)
      expect(terminal.pid).toBe(4242)
      expect(broker.peer.messages.filter(message => message.type === 'start').map(message => message.mode)).toContain('process')
      expect(localSpawn).not.toHaveBeenCalled()
    } finally {
      await stop()
      await broker.close()
    }
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
    const first = await acceptingBroker('first')
    const second = await acceptingBroker('second')
    const { ctx, runtime, localSpawn, stop } = await constructedWithoutBroker()
    try {
      const disposeFirst = ctx.reflect.provide('subprocessBrokerConfig', { socketPath: first.socketPath })
      await expect(runtime.spawn(processSpec()).done).resolves.toEqual({ exitCode: 0, signal: null })
      await disposeFirst()
      expect(() => runtime.spawn(processSpec())).toThrow(/broker configuration is not active/)
      await expect(runtime.spawnTerminal(TERMINAL_SPEC)).rejects.toThrow(/broker configuration is not active/)
      ctx.reflect.provide('subprocessBrokerConfig', { socketPath: second.socketPath })
      await expect(runtime.spawn(processSpec()).done).resolves.toEqual({ exitCode: 0, signal: null })
      expect(first.peer.messages.filter(message => message.type === 'start')).toHaveLength(1)
      expect(second.peer.messages.filter(message => message.type === 'start')).toHaveLength(1)
      expect(localSpawn).not.toHaveBeenCalled()
    } finally {
      await stop()
      await first.close()
      await second.close()
    }
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

describe('local subprocess broker route', () => {
  it('registers Core before start and refuses a forged registration receipt', async () => {
    const broker = await fakeBroker(() => {}, 'invalid')
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const runtime = ctx.subprocess as LocalSubprocessRuntime
        const directSpawn = vi.fn()
        runtime.internals.spawn = directSpawn as never
        const handle = runtime.spawn(processSpec())
        await expect(handle.done).rejects.toThrow(/invalid Core registration event/)
        expect(await broker.peer.waitFor('register-core')).toMatchObject({ type: 'register-core', version: 1 })
        expect(broker.peer.messages.some(message => message.type === 'start')).toBe(false)
        expect(directSpawn).not.toHaveBeenCalled()
      })
    } finally { await broker.close() }
  })

  it('uses a resolver registered after startup and denies again after disposal', async () => {
    const broker = await fakeBroker((message, peer) => {
      if (message.type === 'start') {
        peer.send({ type: 'accepted', handle: 'late-1' })
        peer.send({ type: 'outcome', handle: 'late-1', exitCode: 0, signal: null })
        peer.send({ type: 'empty', handle: 'late-1' })
      }
    })
    const ctx = new Context()
    ctx.provide('subprocessBrokerConfig', { socketPath: broker.socketPath })
    ctx.provide('tools', {
      currentActiveExecution: () => ({ active: true }),
      attestActiveExecution: () => ({ direct: true, sessionId: 'session-7' }),
    })
    const fiber = await ctx.plugin(LocalSubprocessRuntime)
    try {
      const runtime = ctx.subprocess as LocalSubprocessRuntime
      const directSpawn = vi.fn()
      runtime.internals.spawn = directSpawn as never
      expect(() => runtime.spawn(processSpec())).toThrow(/resolver is unavailable/)
      const disposeResolver = ctx.reflect.provide('subprocessBrokerAssignmentResolver', {
        resolve: (id: string) => id === 'session-7' ? 'late-assignment' : undefined,
      })
      const handle = runtime.spawn(processSpec())
      await expect(handle.done).resolves.toEqual({ exitCode: 0, signal: null })
      expect(await broker.peer.waitFor('start')).toMatchObject({ assignmentId: 'late-assignment' })
      await disposeResolver()
      expect(() => runtime.spawn(processSpec())).toThrow(/resolver is unavailable/)
      expect(directSpawn).not.toHaveBeenCalled()
    } finally {
      await fiber.dispose()
      await broker.close()
    }
  })

  it('streams process stdio and holds ownership through broker quiescence', async () => {
    const broker = await fakeBroker((message, peer) => {
      if (message.type === 'start') peer.send({ type: 'accepted', handle: 'process-1' })
      if (message.type === 'stdin-eof') {
        peer.send({ type: 'stdout', handle: 'process-1', data: Buffer.from('broker-out').toString('base64') })
        peer.send({ type: 'stderr', handle: 'process-1', data: Buffer.from('broker-err').toString('base64') })
        peer.send({ type: 'outcome', handle: 'process-1', exitCode: 7, signal: null })
        // A descendant in the managed range can still emit output after the
        // direct process has exited. Stream EOF must follow `empty`.
        setTimeout(() => peer.send({ type: 'stdout', handle: 'process-1', data: Buffer.from('descendant-out').toString('base64') }), 5)
      }
    })
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const runtime = ctx.subprocess as LocalSubprocessRuntime
        const localSpawn = vi.fn()
        runtime.internals.spawn = localSpawn as never
        const handle = ctx.subprocess.spawn(processSpec())
        const stdout: Buffer[] = []; const stderr: Buffer[] = []
        handle.stdout?.on('data', (chunk: Buffer) => stdout.push(chunk))
        handle.stderr?.on('data', (chunk: Buffer) => stderr.push(chunk))
        handle.stdin?.write('input-from-core')
        handle.stdin?.end()
        await expect(handle.done).resolves.toEqual({ exitCode: 7, signal: null })
        await expect(handle.waitForExit(AbortSignal.timeout(25))).resolves.toBe(false)
        await new Promise(resolve => setTimeout(resolve, 10))
        broker.peer.send({ type: 'empty', handle: 'process-1' })
        await expect(handle.waitForExit()).resolves.toBe(true)
        const start = await broker.peer.waitFor('start')
        expect(broker.peer.messages[0]).toMatchObject({ type: 'register-core', version: 1 })
        expect(broker.peer.messages[0]).not.toHaveProperty('assignmentId')
        expect(start).toMatchObject({
          version: 1, mode: 'process', assignmentId: 'trusted-assignment-7',
          argv: ['/bin/echo', 'hello'], cwd: '/assigned/tree',
          stdio: { stdin: { mode: 'pipe' }, stdout: { mode: 'pipe' }, stderr: { mode: 'pipe' } },
        })
        expect(start).not.toHaveProperty('uid')
        expect(start).not.toHaveProperty('allowedRoot')
        expect(start.env).toMatchObject({ DSH_EXAMPLE: 'label' })
        expect(broker.peer.messages.find(message => message.type === 'stdin')).toMatchObject({
          type: 'stdin', handle: 'process-1', data: Buffer.from('input-from-core').toString('base64'),
        })
        expect(Buffer.concat(stdout).toString()).toBe('broker-outdescendant-out')
        expect(Buffer.concat(stderr).toString()).toBe('broker-err')
        expect(localSpawn).not.toHaveBeenCalled()
      })
    } finally { await broker.close() }
  })

  it('uses the broker for PTY allocation, I/O, inspection, foreground signal, and cancellation', async () => {
    const broker = await fakeBroker((message, peer) => {
      if (message.type === 'start') peer.send({ type: 'accepted', handle: 'pty-1', pid: 777 })
      if (['stdin', 'resize', 'inspect-foreground', 'inspect-activity', 'signal-foreground'].includes(message.type)) {
        const result = message.type === 'inspect-foreground'
          ? { processGroupId: 778, inputWaiting: true }
          : message.type === 'inspect-activity' ? { state: 'busy', revision: 3 }
            : message.type === 'signal-foreground' ? { processGroupId: 778 } : {}
        peer.send({ type: 'response', id: message.id, handle: message.handle, ok: true, ...result })
      }
      if (message.type === 'cancel') {
        peer.send({ type: 'outcome', handle: 'pty-1', exitCode: null, signal: 'SIGTERM' })
        peer.send({ type: 'empty', handle: 'pty-1' })
      }
    })
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const runtime = ctx.subprocess as LocalSubprocessRuntime
        const localSpawn = vi.fn(); runtime.internals.spawn = localSpawn as never
        const ptySpawn = vi.fn(); runtime.terminalInspector = { snapshot: ptySpawn } as never
        const handle = await ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh', '-i'], cwd: '/assigned/tree', rows: 24, cols: 80,
          terminalType: 'xterm', graceMs: 200,
        })
        expect(handle.pid).toBe(777)
        await handle.write('echo broker\n')
        await handle.resize(100, 40)
        await expect(handle.inspectForeground()).resolves.toEqual({ processGroupId: 778, inputWaiting: true })
        await expect(handle.inspectActivity()).resolves.toEqual({ state: 'busy', revision: 3 })
        await expect(handle.signalForeground('SIGINT')).resolves.toBe(778)
        await handle.terminate()
        await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
        const start = await broker.peer.waitFor('start')
        expect(start).toMatchObject({
          mode: 'terminal', assignmentId: 'trusted-assignment-7', argv: ['/bin/sh', '-i'],
          cwd: '/assigned/tree', rows: 24, cols: 80,
          stdio: { stdin: { mode: 'pty' }, stdout: { mode: 'pty' }, stderr: { mode: 'pty' } },
        })
        expect(broker.peer.messages.map(message => message.type)).toEqual(expect.arrayContaining([
          'stdin', 'resize', 'inspect-foreground', 'inspect-activity', 'signal-foreground', 'cancel',
        ]))
        expect(localSpawn).not.toHaveBeenCalled()
        expect(ptySpawn).not.toHaveBeenCalled()
      })
    } finally { await broker.close() }
  })

  it('forwards process cancellation and waits for broker-confirmed empty range', async () => {
    const broker = await fakeBroker((message, peer) => {
      if (message.type === 'start') peer.send({ type: 'accepted', handle: 'process-cancel-1' })
      if (message.type === 'cancel') {
        peer.send({ type: 'outcome', handle: 'process-cancel-1', exitCode: null, signal: 'SIGTERM' })
        peer.send({ type: 'empty', handle: 'process-cancel-1' })
      }
    })
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const handle = ctx.subprocess.spawn(processSpec())
        handle.terminate()
        await expect(handle.done).resolves.toEqual({ exitCode: null, signal: 'SIGTERM' })
        await expect(handle.waitForExit()).resolves.toBe(true)
        expect(broker.peer.messages.some(message => message.type === 'cancel' && message.signal === 'SIGTERM')).toBe(true)
      })
    } finally { await broker.close() }
  })

  it('cancels a terminal range after direct outcome until broker confirms empty', async () => {
    const broker = await fakeBroker((message, peer) => {
      if (message.type === 'start') {
        peer.send({ type: 'accepted', handle: 'pty-descendant-1', pid: 778 })
        peer.send({ type: 'outcome', handle: 'pty-descendant-1', exitCode: 0, signal: null })
      }
      if (message.type === 'cancel') peer.send({ type: 'empty', handle: 'pty-descendant-1' })
    })
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const handle = await ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh'], cwd: '/assigned/tree', rows: 24, cols: 80,
          terminalType: 'xterm', graceMs: 100,
        })
        await expect(handle.done).resolves.toEqual({ exitCode: 0, signal: null })
        const quiescence = handle.terminate()
        await expect(broker.peer.waitFor('cancel')).resolves.toMatchObject({ handle: 'pty-descendant-1' })
        await quiescence
      })
    } finally { await broker.close() }
  })

  it('times out a PTY RPC and fails the connection closed', async () => {
    const broker = await fakeBroker((message, peer) => {
      if (message.type === 'start') peer.send({ type: 'accepted', handle: 'pty-stalled-1', pid: 779 })
      // Deliberately leave the socket open and never answer the RPC.
    })
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const handle = await ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh'], cwd: '/assigned/tree', rows: 24, cols: 80,
          terminalType: 'xterm', graceMs: 100,
        })
        const operation = handle.inspectForeground()
        await expect(operation).rejects.toThrow(/request timed out/)
        await expect(handle.done).rejects.toThrow()
      })
    } finally {
      await broker.close()
    }
  }, 15_000)

  it('resolves distinct broker assignments from the active Core session for process and PTY', async () => {
    const processBroker = await fakeBroker((message, peer) => {
      if (message.type === 'start') {
        peer.send({ type: 'accepted', handle: 'session-8-process' })
        peer.send({ type: 'outcome', handle: 'session-8-process', exitCode: 0, signal: null })
        peer.send({ type: 'empty', handle: 'session-8-process' })
      }
    })
    const terminalBroker = await fakeBroker((message, peer) => {
      if (message.type === 'start') {
        peer.send({ type: 'accepted', handle: 'session-7-terminal', pid: 781 })
        peer.send({ type: 'outcome', handle: 'session-7-terminal', exitCode: 0, signal: null })
        peer.send({ type: 'empty', handle: 'session-7-terminal' })
      }
    })
    try {
      await withRuntime(processBroker.socketPath, async ctx => {
        const handle = ctx.subprocess.spawn(processSpec({ cwd: '/work/session-8' }))
        await expect(handle.done).resolves.toEqual({ exitCode: 0, signal: null })
        await expect(handle.waitForExit()).resolves.toBe(true)
        expect(await processBroker.peer.waitFor('start')).toMatchObject({ assignmentId: 'trusted-assignment-8', cwd: '/work/session-8' })
      }, 'session-8')
      await withRuntime(terminalBroker.socketPath, async ctx => {
        const handle = await ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh'], cwd: '/work/session-7', rows: 24, cols: 80,
          terminalType: 'xterm', graceMs: 100,
        })
        await expect(handle.done).resolves.toEqual({ exitCode: 0, signal: null })
        await handle.terminate()
        expect(await terminalBroker.peer.waitFor('start')).toMatchObject({ assignmentId: 'trusted-assignment-7', cwd: '/work/session-7' })
      }, 'session-7')
    } finally {
      await processBroker.close()
      await terminalBroker.close()
    }
  })

  it('denies missing active identity and rejects a cwd outside the selected assignment without fallback', async () => {
    const broker = await fakeBroker((message, peer) => {
      if (message.type === 'start') {
        if (message.assignmentId !== 'trusted-assignment-7' || message.cwd !== '/work/session-7') {
          peer.send({ type: 'error', message: 'cwd is outside the broker assignment' })
          return
        }
        peer.send({ type: 'accepted', handle: 'scoped-process' })
        peer.send({ type: 'outcome', handle: 'scoped-process', exitCode: 0, signal: null })
        peer.send({ type: 'empty', handle: 'scoped-process' })
      }
    })
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const runtime = ctx.subprocess as LocalSubprocessRuntime
        const localSpawn = vi.fn(); runtime.internals.spawn = localSpawn as never
        expect(() => ctx.subprocess.spawn(processSpec())).toThrow(/requires an active ToolRuntime execution/)
        await expect(ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh'], cwd: '/work/session-7', rows: 24, cols: 80,
          terminalType: 'xterm', graceMs: 100,
        })).rejects.toThrow(/requires an active ToolRuntime execution/)
        expect(localSpawn).not.toHaveBeenCalled()
        expect(broker.peer.messages).toHaveLength(0)
      }, null)
      await withRuntime(broker.socketPath, async ctx => {
        const runtime = ctx.subprocess as LocalSubprocessRuntime
        const localSpawn = vi.fn(); runtime.internals.spawn = localSpawn as never
        const handle = ctx.subprocess.spawn(processSpec({ cwd: '/work/session-8' }))
        await expect(handle.done).rejects.toThrow(/cwd is outside the broker assignment/)
        expect(await broker.peer.waitFor('start')).toMatchObject({
          assignmentId: 'trusted-assignment-7', cwd: '/work/session-8',
        })
        expect(() => ctx.subprocess.spawn(processSpec({ env: { DSH_SESSION_ID: 'session-8' } })))
          .toThrow(/DSH_SESSION_ID cannot be set for broker launches/)
        await expect(ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh'], cwd: '/work/session-8', rows: 24, cols: 80,
          terminalType: 'xterm', graceMs: 100,
        })).rejects.toThrow(/cwd is outside the broker assignment/)
        await expect(ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh'], cwd: '/work/session-7', env: { DSH_SESSION_ID: 'session-8' },
          rows: 24, cols: 80, terminalType: 'xterm', graceMs: 100,
        })).rejects.toThrow(/DSH_SESSION_ID cannot be set for broker launches/)
        expect(localSpawn).not.toHaveBeenCalled()
      }, 'session-7')
    } finally { await broker.close() }
  })

  it('fails closed when the broker socket is unavailable and never invokes local spawn', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'dsh-broker-missing-'))
    const missingSocket = join(directory, 'missing.sock')
    try {
      await withRuntime(missingSocket, async ctx => {
        const runtime = ctx.subprocess as LocalSubprocessRuntime
        const localSpawn = vi.fn(); runtime.internals.spawn = localSpawn as never
        const handle = ctx.subprocess.spawn(processSpec())
        await expect(handle.done).rejects.toThrow()
        expect(localSpawn).not.toHaveBeenCalled()
        await expect(ctx.subprocess.spawnTerminal({
          argv: ['/bin/sh'], cwd: '/assigned/tree', rows: 24, cols: 80,
          terminalType: 'xterm', graceMs: 100,
        })).rejects.toThrow()
        expect(localSpawn).not.toHaveBeenCalled()
      })
    } finally { rmSync(directory, { recursive: true, force: true }) }
  })

  it('fails closed on malformed broker protocol without local fallback', async () => {
    const broker = await fakeBroker((_message, peer) => { peer.socket.write('{not-json}\n') })
    try {
      await withRuntime(broker.socketPath, async ctx => {
        const runtime = ctx.subprocess as LocalSubprocessRuntime
        const localSpawn = vi.fn(); runtime.internals.spawn = localSpawn as never
        const handle = ctx.subprocess.spawn(processSpec())
        await expect(handle.done).rejects.toThrow(/invalid protocol frame/)
        expect(localSpawn).not.toHaveBeenCalled()
      })
    } finally { await broker.close() }
  })
})
