/**
 * The two narrow capabilities the broker-required launcher hands out (see `src/broker-capabilities.ts`): the agent
 * loop's lifecycle hook and ROOM's closed seat port. All of it is deny-first: the broker is a fake, and the
 * shipped client still refuses every bind, so nothing here can produce an assignment.
 */
import { mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import { boot } from '@deepseek-ai/dsh-app-boot'
import AgentLoop, { SESSION_LIFECYCLE_HOOK } from '@deepseek-ai/dsh-agent-loop'
import AgentRegistry, { type Agent, type AgentHandle } from '@deepseek-ai/dsh-agent'
import LlmRuntime from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime, { defineTool } from '@deepseek-ai/dsh-tools'
import { ToolCallId } from '@deepseek-ai/dsh-llm'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { assignmentUnavailableClient, createBindingHost, type BindingHost } from '../src/broker-binding.ts'
import {
  createRoomSeatPort, installCapabilityPorts, installLifecycleHook, LIFECYCLE_HOOK_KEY, ROOM_PLUGIN_SPECIFIER,
} from '../src/broker-capabilities.ts'
import type { BrokerConfig } from '../src/broker-required.ts'
import { MockAdapter, textResponse } from '../../../packages/core/agent-loop/tests/mock-adapter.ts'
import { FakeBroker } from './fake-broker.ts'

const BROKER: BrokerConfig = Object.freeze({ socketPath: '/run/dsh/broker.sock' })
const model = { provider: 'mock', model: 'mock' }
const signal = new AbortController().signal

const live: Context[] = []
const dirs: string[] = []
afterEach(async () => {
  await Promise.all(live.splice(0).map(ctx => ctx.fiber.dispose()))
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

function tempDir(): string {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'dsh-broker-capabilities-')))
  dirs.push(dir)
  return dir
}

interface WorkspaceFake { path: string; sessionIds: string[] }

interface Env {
  ctx: Context
  broker: FakeBroker
  host: BindingHost
  workspaces: WorkspaceFake[]
  now: { value: number }
  handles: Map<string, AgentHandle>
}

async function environment(options: { client?: FakeBroker | typeof assignmentUnavailableClient; timeoutMs?: number } = {}): Promise<Env> {
  const ctx = new Context()
  live.push(ctx)
  await ctx.plugin(LlmRuntime)
  await ctx.plugin(SessionStore)
  await ctx.plugin(SessionProjectionRegistry)
  await ctx.plugin(SystemPrompt, { personaPrefix: '' })
  await ctx.plugin(ToolRuntime)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(AgentLoop, { agents: [] })
  ctx.llm.registerAdapter(['mock'], new MockAdapter([textResponse('ok'), textResponse('ok'), textResponse('ok')]))
  ctx.provide('loader', { entries: () => [] } as never)
  ctx.provide('subprocessBrokerConfig', BROKER)
  const workspaces: WorkspaceFake[] = []
  ctx.provide('workspaceRegistry', { list: () => workspaces } as never)
  const broker = new FakeBroker()
  const host = createBindingHost({
    ctx, broker: BROKER, client: options.client ?? broker,
    ...options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs },
  })
  host.gate.arm()
  return { ctx, broker, host, workspaces, now: { value: Date.now() }, handles: new Map() }
}

/** A session ROOM has just created in `cwd`, which the workspace registry places in a workspace rooted there. */
async function roomSession(env: Env, id: string, cwd: string, membership = true): Promise<Agent> {
  const handle = await env.ctx.agents.create({ sessionId: SessionId(id), agentOptions: model, meta: { cwd } })
  env.handles.set(id, handle)
  if (membership) env.workspaces.push({ path: cwd, sessionIds: [id] })
  return handle.agent
}

const seatRequest = (sessionId: string) => ({
  sessionId, instanceId: 'orca-instance-1', room: { stanzaId: '77', agente: 'agente-1', installazione: 'inst-1' },
})

describe('the lifecycle hook on the root context', () => {
  it('is defined once, unnamed, read-only and not replaceable', async () => {
    const env = await environment()
    installLifecycleHook(env.ctx, env.host)
    const root = env.ctx.root as unknown as Record<symbol, unknown>
    const descriptor = Object.getOwnPropertyDescriptor(root, LIFECYCLE_HOOK_KEY)!
    expect(descriptor).toMatchObject({ writable: false, configurable: false, enumerable: false })
    expect(LIFECYCLE_HOOK_KEY).toBe(SESSION_LIFECYCLE_HOOK)
    expect(() => { installLifecycleHook(env.ctx, env.host) }).toThrow(/already installed/)
    expect(() => { root[LIFECYCLE_HOOK_KEY] = { revoke: async () => {}, childPublished: async () => {} } }).toThrow(TypeError)
    expect(Reflect.defineProperty(root, LIFECYCLE_HOOK_KEY, { value: {} })).toBe(false)
    // A plugin cannot take the place of a Context name: there is none.
    expect(Object.keys(env.ctx.reflect.props).filter(name => /hook|lifecycle|binding|issuer/iu.test(name))).toEqual([])
    expect(Object.keys(descriptor.value as object).sort()).toEqual(['childPublished', 'revoke'])
    expect(Object.isFrozen(descriptor.value)).toBe(true)
  })

  it('is read from a child context too, so every agent loop sees the same one', async () => {
    const env = await environment()
    installLifecycleHook(env.ctx, env.host)
    const child = env.ctx.extend()
    const read = (ctx: Context): unknown => (ctx as unknown as Record<symbol, unknown>)[SESSION_LIFECYCLE_HOOK]
    expect(read(child)).toBe(read(env.ctx.root))
    expect(read(child)).toBeDefined()
  })

  describe('with the real agent loop', () => {
    async function withLoop(): Promise<Env> {
      const env = await environment()
      installLifecycleHook(env.ctx, env.host)
      return env
    }

    /** Run `body` as an active direct tool execution of `agent`, the way the ToolRuntime does for a model call. */
    async function inToolOf<T>(env: Env, agent: Agent, name: string, body: () => Promise<T>): Promise<T> {
      let result: T | undefined
      env.ctx.tools.register(defineTool({
        name, description: 'probe', parameters: {},
        output: { schema: { type: 'string' }, render: () => [] },
        async execute() { result = await body(); return 'ok' },
      }))
      await env.ctx.tools.execute({ signal, callId: ToolCallId(name), name, arguments: {}, agent })
      return result as T
    }

    it('revokes through the real teardown and waits for the broker ack before the agent leaves the registries (the order against a running tool is pinned in the agent-loop spec)', async () => {
      const env = await withLoop()
      const agent = await roomSession(env, 'root', tempDir())
      expect(await env.host.issuer.issue({ sessionId: 'root', kind: 'attach' })).toMatchObject({ ok: true })
      const order: string[] = []
      const ack = Promise.withResolvers<{ ok: true }>()
      env.broker.revokeAnswer = () => {
        order.push(`revoke:inStore=${env.ctx.sessions.get(SessionId('root')) !== undefined}:inRegistry=${env.ctx.agents.get(SessionId('root')) === agent}`)
        return ack.promise
      }
      env.ctx.on('agent/disposed', () => { order.push('agent/disposed') })
      const disposing = env.handles.get('root')!.dispose()
      await vi.waitFor(() => { expect(order).toEqual(['revoke:inStore=true:inRegistry=true']) })
      await new Promise(resolve => setTimeout(resolve, 20))
      expect(order).toHaveLength(1)
      ack.resolve({ ok: true })
      await disposing
      await vi.waitFor(() => { expect(order).toEqual(['revoke:inStore=true:inRegistry=true', 'agent/disposed']) })
      expect(env.host.issuer.lookup('root')).toEqual({ ok: false, reason: 'no-binding' })
    })

    it('binds a child created by its parent’s own tool call, under the parent binding, and revokes it with the parent', async () => {
      const env = await withLoop()
      const parent = await roomSession(env, 'parent', tempDir())
      const granted = await env.host.issuer.issue({ sessionId: 'parent', kind: 'attach' })
      expect(granted).toMatchObject({ ok: true })
      await inToolOf(env, parent, 'spawn', async () => {
        await parent.ctx.agents.create({
          sessionId: SessionId('child'), agentOptions: model, parentAgent: parent, meta: { parentSession: SessionId('parent') },
        })
      })
      await vi.waitFor(() => { expect(env.broker.binds.map(request => request.sessionId)).toEqual(['parent', 'child']) })
      expect(env.broker.binds[1]).toMatchObject({
        kind: 'child', parentBindingId: (granted as { bindingId: string }).bindingId, workspaceId: env.broker.binds[0]!.workspaceId,
      })
      await env.handles.get('parent')!.dispose()
      expect(env.broker.revokes.map(request => request.sessionId)).toContain('child')
      expect(env.broker.revokes.map(request => request.sessionId).indexOf('child'))
        .toBeLessThan(env.broker.revokes.map(request => request.sessionId).indexOf('parent'))
    })

    it('does not graft a session onto another session’s binding, whoever asks and however it names the parent', async () => {
      const env = await withLoop()
      const victim = await roomSession(env, 'victim', tempDir())
      const other = await roomSession(env, 'other', tempDir())
      await env.host.issuer.issue({ sessionId: 'victim', kind: 'attach' })
      await env.host.issuer.issue({ sessionId: 'other', kind: 'attach' })
      env.broker.binds.length = 0

      // 1. A plugin outside any tool call names the victim as parent, in both the registry and the header.
      await victim.ctx.agents.create({
        sessionId: SessionId('graft-1'), agentOptions: model, parentAgent: victim, meta: { parentSession: SessionId('victim') },
      })
      // 2. A tool call of ANOTHER session names the victim as parent.
      await inToolOf(env, other, 'graft-tool', async () => {
        await victim.ctx.agents.create({
          sessionId: SessionId('graft-2'), agentOptions: model, parentAgent: victim, meta: { parentSession: SessionId('victim') },
        })
      })
      // 3. The victim's own tool call, but the new session's header does not name the victim.
      await inToolOf(env, victim, 'graft-header', async () => {
        await victim.ctx.agents.create({
          sessionId: SessionId('graft-3'), agentOptions: model, parentAgent: victim, meta: { parentSession: SessionId('other') },
        })
      })
      await new Promise(resolve => setTimeout(resolve, 50))
      expect(env.broker.binds).toEqual([])
      expect(env.host.issuer.lookup('graft-1')).toMatchObject({ ok: false })
      expect(env.host.issuer.lookup('graft-2')).toMatchObject({ ok: false })
      expect(env.host.issuer.lookup('graft-3')).toMatchObject({ ok: false })
    })

    it('binds nothing for a root agent or a plain create, and stays deny-only with the shipped client', async () => {
      const env = await environment({ client: assignmentUnavailableClient })
      installLifecycleHook(env.ctx, env.host)
      const root = await roomSession(env, 'root', tempDir())
      expect(await env.host.issuer.issue({ sessionId: 'root', kind: 'attach' })).toEqual({ ok: false, reason: 'assignment-unavailable' })
      await inToolOf(env, root, 'spawn', async () => {
        await root.ctx.agents.create({ sessionId: SessionId('child'), agentOptions: model, parentAgent: root, meta: { parentSession: SessionId('root') } })
      })
      await new Promise(resolve => setTimeout(resolve, 30))
      expect(env.host.issuer.lookup('child')).toEqual({ ok: false, reason: 'no-binding' })
      const answer = await inToolOf(env, root, 'resolve', () => env.host.resolver.resolve(1))
      expect(answer).toEqual({ ok: false, reason: 'no-binding' })
    })
  })
})

describe('the ROOM seat port', () => {
  it('is a closed object: seat and revoke only, no issuer, no kind to choose, no org, no parent', async () => {
    const env = await environment()
    const port = createRoomSeatPort(env.ctx, env.host)
    expect(Object.keys(port).sort()).toEqual(['revoke', 'seat'])
    expect(Object.isFrozen(port)).toBe(true)
    const cwd = tempDir()
    await roomSession(env, 'room-1', cwd)
    const outcome = await port.seat({
      ...seatRequest('room-1'),
      // Everything below is outside the port’s contract and must not reach the broker.
      kind: 'attach', orgId: 'org-9', parentSessionId: 'x', workspaceId: 'w', cwd: '/etc', assignmentId: 'forged',
    } as never)
    expect(outcome).toMatchObject({ ok: true })
    const [request] = env.broker.binds
    expect(Object.keys(request!).sort()).toEqual(['coreEpoch', 'cwd', 'kind', 'orca', 'room', 'sessionId', 'workspaceId'])
    // The forged `cwd: '/etc'` is dropped; the request carries Core's own realpath of the session header cwd (C-2).
    expect(request!.cwd).toBe(cwd)
    expect(request).toMatchObject({
      kind: 'room-seat', sessionId: 'room-1', orca: { instanceId: 'orca-instance-1' },
      room: { origine: 'dsh', stanzaId: '77', agente: 'agente-1', installazione: 'inst-1' },
    })
    expect(Object.keys(port as unknown as Record<string, unknown>)).not.toContain('issuer')
  })

  it('refuses a session that is not live, not recent, has no cwd, or is not in a workspace rooted at its cwd', async () => {
    const env = await environment()
    const port = createRoomSeatPort(env.ctx, env.host, { freshMs: 60_000, now: () => env.now.value })
    expect(await port.seat(seatRequest('ghost'))).toEqual({ ok: false, reason: 'unknown-session' })

    const cwd = tempDir()
    await roomSession(env, 'no-workspace', cwd, false)
    expect(await port.seat(seatRequest('no-workspace'))).toEqual({ ok: false, reason: 'invalid-workspace' })

    const elsewhere = tempDir()
    await env.ctx.agents.create({ sessionId: SessionId('wrong-root'), agentOptions: model, meta: { cwd } })
    env.workspaces.push({ path: elsewhere, sessionIds: ['wrong-root'] })
    expect(await port.seat(seatRequest('wrong-root'))).toEqual({ ok: false, reason: 'invalid-workspace' })

    await env.ctx.agents.create({ sessionId: SessionId('not-a-member'), agentOptions: model, meta: { cwd } })
    env.workspaces.push({ path: cwd, sessionIds: ['someone-else'] })
    expect(await port.seat(seatRequest('not-a-member'))).toEqual({ ok: false, reason: 'invalid-workspace' })

    await env.ctx.agents.create({ sessionId: SessionId('no-cwd'), agentOptions: model })
    expect(await port.seat(seatRequest('no-cwd'))).toEqual({ ok: false, reason: 'invalid-workspace' })

    await roomSession(env, 'old', tempDir())
    env.now.value += 61_000
    expect(await port.seat(seatRequest('old'))).toEqual({ ok: false, reason: 'stale-session' })
    expect(env.broker.binds).toEqual([])
  })

  it('refuses malformed requests and a second seat of the same session, and passes broker refusals through', async () => {
    const env = await environment()
    const port = createRoomSeatPort(env.ctx, env.host)
    await roomSession(env, 'room-1', tempDir())
    for (const broken of [
      { ...seatRequest('room-1'), instanceId: '' },
      { ...seatRequest('room-1'), room: { stanzaId: '', agente: 'a', installazione: 'i' } },
      { ...seatRequest('room-1'), room: undefined },
      { ...seatRequest('room-1'), sessionId: 42 },
    ]) expect(await port.seat(broken as never)).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await port.seat(seatRequest('room-1'))).toMatchObject({ ok: true })
    expect(await port.seat(seatRequest('room-1'))).toEqual({ ok: false, reason: 'already-bound' })
    const refused = await environment()
    refused.broker.bindAnswer = async () => ({ ok: false, reason: 'orca-unverified' })
    await roomSession(refused, 'room-2', tempDir())
    expect(await createRoomSeatPort(refused.ctx, refused.host).seat(seatRequest('room-2'))).toEqual({ ok: false, reason: 'orca-unverified' })
  })

  it('revokes only what it seated, and with the shipped client nothing is ever granted', async () => {
    const env = await environment()
    const port = createRoomSeatPort(env.ctx, env.host)
    await roomSession(env, 'seated', tempDir())
    await roomSession(env, 'foreign', tempDir())
    await port.seat(seatRequest('seated'))
    await env.host.issuer.issue({ sessionId: 'foreign', kind: 'attach' })
    expect(await port.revoke('foreign')).toEqual({ ok: true })
    expect(env.broker.revokes).toEqual([])
    expect(await port.revoke('seated')).toEqual({ ok: true })
    expect(env.broker.revokes.map(request => request.sessionId)).toEqual(['seated'])
    expect(await port.revoke('seated')).toEqual({ ok: true })
    expect(env.broker.revokes).toHaveLength(1)

    const denied = await environment({ client: assignmentUnavailableClient })
    await roomSession(denied, 'room-1', tempDir())
    expect(await createRoomSeatPort(denied.ctx, denied.host).seat(seatRequest('room-1'))).toEqual({ ok: false, reason: 'assignment-unavailable' })
  })

  it('does not issue when the gate is not active', async () => {
    const env = await environment()
    const port = createRoomSeatPort(env.ctx, env.host)
    await roomSession(env, 'room-1', tempDir())
    env.ctx.reflect.provide('pluginManager', {})
    expect(await port.seat(seatRequest('room-1'))).toEqual({ ok: false, reason: 'gate-inactive' })
    expect(env.broker.binds).toEqual([])
  })
})

/** A real package on disk the Loader imports by its bare name; it reports what its `apply` received. */
function writeRoomPackage(root: string, name: string): void {
  const dir = join(root, 'node_modules', ...name.split('/'))
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, 'package.json'), JSON.stringify({ name, version: '1.0.0', type: 'module', exports: './index.mjs' }))
  writeFileSync(join(dir, 'index.mjs'), `
export const name = ${JSON.stringify(name)}
export const inject = []
export function apply(ctx, config, port) {
  globalThis.__capabilityProbe.calls.push({ name, config, port, arity: arguments.length })
}
`)
}

const probe = { calls: [] as { name: string; config: unknown; port: unknown; arity: number }[] }

describe('delivery of the port through the Loader', () => {
  /** Boot a real tree whose root config is `rows`, with the launcher's capability install in `prepare`. */
  async function bootWith(files: Record<string, string>, rows: unknown[], ports: Record<string, object>, bareBase?: boolean) {
    ;(globalThis as { __capabilityProbe?: unknown }).__capabilityProbe = probe
    probe.calls.length = 0
    const root = tempDir()
    for (const [file, text] of Object.entries(files)) writeFileSync(join(root, file), text)
    writeFileSync(join(root, 'cordis.yml'), '[]\n')
    for (const name of [ROOM_PLUGIN_SPECIFIER, 'other-room-plugin']) writeRoomPackage(root, name)
    const ctx = await boot('capability', join(root, 'cordis.yml'), [{ insert: rows as never }], (hostCtx) => {
      installCapabilityPorts(hostCtx, ports, bareBase === true ? { parentURL: `file://${root}/` } : {}).pin()
    }, bareBase === true ? `file://${root}/` : undefined)
    live.push(ctx)
    return ctx
  }

  const port = Object.freeze({ seat: async () => ({ ok: false }), revoke: async () => ({ ok: true }) })

  it('hands the port as the third argument of apply to the module imported under the exact ROOM specifier', async () => {
    await bootWith({}, [{ id: 'room', name: ROOM_PLUGIN_SPECIFIER, config: { base: 'x' } }], { [ROOM_PLUGIN_SPECIFIER]: port })
    expect(probe.calls).toHaveLength(1)
    expect(probe.calls[0]).toMatchObject({ name: ROOM_PLUGIN_SPECIFIER, config: { base: 'x' }, arity: 3 })
    expect(probe.calls[0]!.port).toBe(port)
  })

  it('applies to a row in a nested include file and in a group, not only to the root rows', async () => {
    await bootWith(
      { 'nested.yml': `- id: room-nested\n  name: '${ROOM_PLUGIN_SPECIFIER}'\n` },
      [{ id: 'inc', name: 'cordis:include', config: { path: './nested.yml' } }],
      { [ROOM_PLUGIN_SPECIFIER]: port },
    )
    expect(probe.calls.map(call => call.port)).toEqual([port])

    await bootWith({}, [{ id: 'grp', name: 'cordis:group', group: true, config: [{ id: 'room-in-group', name: ROOM_PLUGIN_SPECIFIER }] }],
      { [ROOM_PLUGIN_SPECIFIER]: port })
    expect(probe.calls.map(call => call.port)).toEqual([port])
  })

  it('also applies when the root include resolves bare names through an installed-host base', async () => {
    await bootWith({}, [{ id: 'room', name: ROOM_PLUGIN_SPECIFIER }], { [ROOM_PLUGIN_SPECIFIER]: port }, true)
    expect(probe.calls.map(call => call.port)).toEqual([port])
  })

  it('gives nothing to any other package, and the config cannot ask for it', async () => {
    await bootWith({}, [
      { id: 'other', name: 'other-room-plugin', config: { port: 'please', capabilities: ['room-seat'] } },
    ], { [ROOM_PLUGIN_SPECIFIER]: port })
    expect(probe.calls).toHaveLength(1)
    expect(probe.calls[0]).toMatchObject({ name: 'other-room-plugin', arity: 2, port: undefined })
  })

  it('gives nothing when the module is not imported under the exact specifier (relative path, other casing)', async () => {
    const root = tempDir()
    writeRoomPackage(root, ROOM_PLUGIN_SPECIFIER)
    const absolute = join(root, 'node_modules', ...ROOM_PLUGIN_SPECIFIER.split('/'), 'index.mjs')
    await bootWith({}, [{ id: 'by-path', name: absolute }], { [ROOM_PLUGIN_SPECIFIER]: port })
    expect(probe.calls.map(call => ({ arity: call.arity, port: call.port }))).toEqual([{ arity: 2, port: undefined }])
  })

  it('leaves a module without apply, and a root with no ports at all, untouched', async () => {
    await bootWith({}, [{ id: 'room', name: ROOM_PLUGIN_SPECIFIER }], {})
    expect(probe.calls.map(call => ({ arity: call.arity, port: call.port }))).toEqual([{ arity: 2, port: undefined }])
  })

  it('wraps the include class once and keeps the wrapper stable per module', async () => {
    const ctx = new Context()
    live.push(ctx)
    const builtins = Object.create(null) as Record<string, unknown>
    class Base { import(name: string): unknown { return Promise.resolve({ apply: () => undefined, name }) } }
    ;(ctx as unknown as { loader: unknown }).loader = { builtins }
    const ports = installCapabilityPorts(ctx, { [ROOM_PLUGIN_SPECIFIER]: port }, { resolveModule: name => `file:///pinned/${name}.mjs` })
    ports.pin()
    // Boot assigns its root include once, after `prepare`; it is wrapped, and that is the last write accepted.
    builtins['include'] = Base
    const Ported = builtins['include'] as new () => Base
    expect(Ported).not.toBe(Base)
    expect(builtins['include']).toBe(Ported)
    const instance = new Ported()
    const first = await instance.import(ROOM_PLUGIN_SPECIFIER) as { apply: unknown }
    expect(first).not.toBeNull()
    expect(await instance.import('cordis:group')).toMatchObject({ name: 'cordis:group' })
  })
})

describe('the include override is closed and ROOM is pinned (S3 review C-1)', () => {
  const realPort = Object.freeze({ seat: async () => ({ ok: false }), revoke: async () => ({ ok: true }) })
  class Base { import(name: string): unknown { return Promise.resolve({ apply: () => undefined, name }) } }
  const pinnedAt = (url: string) => ({ resolveModule: () => url })

  /** A bare Loader-shaped context as the launcher sees it right after `prepare`: no include assigned yet. */
  function bare(): { ctx: Context; loader: { builtins: Record<string, unknown> }; builtins: Record<string, unknown> } {
    const ctx = new Context()
    live.push(ctx)
    const builtins = Object.create(null) as Record<string, unknown>
    const loader = { builtins }
    ;(ctx as unknown as { loader: unknown }).loader = loader
    return { ctx, loader, builtins }
  }

  it('accepts boot\'s one write and then refuses every other: assignment, redefinition, deletion, swapping the dictionary', () => {
    const { ctx, loader, builtins } = bare()
    installCapabilityPorts(ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, pinnedAt('file:///pinned/room.mjs')).pin()
    builtins['include'] = Base
    const Ported = builtins['include']
    class Evil extends Base {}
    expect(() => { builtins['include'] = Evil }).toThrow(/sealed/)
    expect(() => { builtins['include'] = undefined }).toThrow(/sealed/)
    expect(() => Object.defineProperty(builtins, 'include', { value: Evil })).toThrow(TypeError)
    expect(Reflect.deleteProperty(builtins, 'include')).toBe(false)
    expect(() => { loader.builtins = { include: Evil } }).toThrow(TypeError)
    expect(Reflect.defineProperty(loader, 'builtins', { value: { include: Evil } })).toBe(false)
    expect(builtins['include']).toBe(Ported)
    expect(loader.builtins).toBe(builtins)
  })

  it('is closed by seal() even if boot never assigned an include', () => {
    const { ctx, builtins } = bare()
    const ports = installCapabilityPorts(ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, pinnedAt('file:///pinned/room.mjs'))
    ports.seal()
    expect(() => { builtins['include'] = Base }).toThrow(/sealed/)
    ports.seal()
  })

  it('cannot be written at all when an include is already there at install time', () => {
    const { ctx, builtins } = bare()
    builtins['include'] = Base
    installCapabilityPorts(ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, pinnedAt('file:///pinned/room.mjs'))
    expect(() => { builtins['include'] = Base }).toThrow(/sealed/)
  })

  it('does not hand the port to the reviewer\'s hostile include: it cannot be swapped in any more', async () => {
    // The sonde of the S3 review (p1.mts): subclass the include class, swap it through the setter, ask for ROOM.
    const { ctx, builtins } = bare()
    installCapabilityPorts(ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, pinnedAt('file:///pinned/room.mjs')).pin()
    builtins['include'] = Base
    let stolen: unknown
    class Evil extends Base {
      override import(_name: string): unknown { return Promise.resolve({ apply(_c: unknown, _cfg: unknown, p: unknown) { stolen = p } }) }
    }
    expect(() => { builtins['include'] = Evil }).toThrow(/sealed/)
    // What a plugin can still do: build the installed include and ask it. It gets ROOM's wrapper, never the port.
    const Installed = builtins['include'] as new () => Base
    const loaded = await new Installed().import(ROOM_PLUGIN_SPECIFIER) as { apply(ctx: unknown, config: unknown): void }
    loaded.apply({}, {})
    expect(stolen).toBeUndefined()
  })

  /** A base include whose modules record the third argument their `apply` gets. */
  function recordingBase(received: unknown[]) {
    return class Recording {
      import(name: string): unknown {
        return Promise.resolve({ name, apply: (_ctx: unknown, _config: unknown, ...rest: unknown[]) => { received.push(rest[0]) } })
      }
    }
  }
  const load = async (Installed: unknown): Promise<void> => {
    const mod = await new (Installed as new () => Base)().import(ROOM_PLUGIN_SPECIFIER) as { apply(ctx: unknown, config: unknown): void }
    mod.apply({}, {})
  }

  it('refuses an import that resolves to a different file than the one pinned at boot', async () => {
    const { ctx, builtins } = bare()
    const received: unknown[] = []
    let resolvesTo: string | undefined = 'file:///pinned/room.mjs'
    installCapabilityPorts(ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, { resolveModule: () => resolvesTo }).pin()
    builtins['include'] = recordingBase(received)
    await load(builtins['include'])
    expect(received).toEqual([realPort])
    resolvesTo = 'file:///elsewhere/evil.mjs'
    await expect(load(builtins['include'])).rejects.toThrow(/not the file pinned at boot/)
    resolvesTo = undefined
    await expect(load(builtins['include'])).rejects.toThrow(/resolves to nothing/)
    expect(received).toEqual([realPort])
  })

  it('gives no port before the pin is taken, and none to a specifier that did not resolve when pinning', async () => {
    const received: unknown[] = []
    const early = bare()
    installCapabilityPorts(early.ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, pinnedAt('file:///pinned/room.mjs'))
    early.builtins['include'] = recordingBase(received)
    await load(early.builtins['include'])
    expect(received).toEqual([undefined])

    const unresolved = bare()
    installCapabilityPorts(unresolved.ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, { resolveModule: () => undefined }).pin()
    unresolved.builtins['include'] = recordingBase(received)
    await load(unresolved.builtins['include'])
    expect(received).toEqual([undefined, undefined])
  })

  it('keeps the first pin: a second pin() after the resolution moved does not re-pin', async () => {
    const { ctx, builtins } = bare()
    const received: unknown[] = []
    let resolvesTo = 'file:///pinned/room.mjs'
    const ports = installCapabilityPorts(ctx, { [ROOM_PLUGIN_SPECIFIER]: realPort }, { resolveModule: () => resolvesTo })
    ports.pin()
    resolvesTo = 'file:///elsewhere/evil.mjs'
    ports.pin()
    builtins['include'] = recordingBase(received)
    await expect(load(builtins['include'])).rejects.toThrow(/not the file pinned at boot/)
    expect(received).toEqual([])
  })

  it('pins in a real boot with the Loader\'s own resolution, and a copy under the same name from another directory is refused', async () => {
    const root = tempDir()
    writeFileSync(join(root, 'cordis.yml'), '[]\n')
    writeRoomPackage(root, ROOM_PLUGIN_SPECIFIER)
    // A different file answering to the same bare name, reachable only from `nested/`.
    mkdirSync(join(root, 'nested'))
    writeRoomPackage(join(root, 'nested'), ROOM_PLUGIN_SPECIFIER)
    writeFileSync(join(root, 'nested', 'rows.yml'), `- id: room-copy\n  name: '${ROOM_PLUGIN_SPECIFIER}'\n`)
    ;(globalThis as { __capabilityProbe?: unknown }).__capabilityProbe = probe
    probe.calls.length = 0
    const rows = [{ id: 'inc', name: 'cordis:include', config: { path: './nested/rows.yml' } }]
    const port = Object.freeze({ seat: async () => ({ ok: false }), revoke: async () => ({ ok: true }) })
    const ctx = await boot('capability', join(root, 'cordis.yml'), [{ insert: rows }], (hostCtx) => {
      installCapabilityPorts(hostCtx, { [ROOM_PLUGIN_SPECIFIER]: port }).pin()
    })
    live.push(ctx)
    // The Loader reports a failed import and leaves the row without a fiber; the substitute never ran.
    const entries = (ctx.get('loader') as unknown as { entries(): { options: { id?: string }; fiber?: unknown }[] }).entries()
    expect(entries.find(entry => entry.options.id === 'room-copy')?.fiber).toBeUndefined()
    expect(probe.calls).toEqual([])
  })
})
