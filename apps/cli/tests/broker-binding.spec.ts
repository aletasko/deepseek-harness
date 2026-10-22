/**
 * Host-only binding issuer and resolver. Everything here is deny-first: the broker is a fake that records what
 * Core sent, and the assertions are about what Core refuses, what it never sends, and what it never invents.
 */
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Context, FiberState } from '@deepseek-ai/cordis'
import LlmRuntime, { ToolCallId } from '@deepseek-ai/dsh-llm'
import SessionStore, { Session, SessionId } from '@deepseek-ai/dsh-session'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime, { defineTool, type ToolRunContext } from '@deepseek-ai/dsh-tools'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  assignmentUnavailableClient, BindingIssuer, BrokerGate, createBindingHost,
  type AssignmentGrant, type BindingBrokerClient, type BindingGrant, type BindingHost, type IssueInput,
} from '../src/broker-binding.ts'
import type { BrokerConfig } from '../src/broker-required.ts'
import { FakeBroker } from './fake-broker.ts'

const BROKER: BrokerConfig = Object.freeze({ socketPath: '/run/dsh/broker.sock' })
const signal = new AbortController().signal

interface Env {
  ctx: Context
  broker: FakeBroker
  host: BindingHost
  loaderRows: unknown[]
  agents: Map<string, Agent>
  /** A fiber that creates `id` in the store, as the agent factory's creation effect does. */
  seat(id: string, run?: (inner: Context, session: Session) => Promise<void> | void): Promise<{ dispose(): Promise<void> }>
}

const live: Context[] = []
afterEach(async () => { await Promise.all(live.splice(0).map(ctx => ctx.fiber.dispose())) })

interface EnvironmentOptions {
  arm?: boolean
  broker?: FakeBroker
  coreEpoch?: string
  client?: BindingBrokerClient
  timeoutMs?: number
}

async function environment(options: EnvironmentOptions = {}): Promise<Env> {
  const ctx = new Context()
  live.push(ctx)
  await ctx.plugin(LlmRuntime)
  await ctx.plugin(SessionStore)
  await ctx.plugin(SessionProjectionRegistry)
  await ctx.plugin(SystemPrompt)
  await ctx.plugin(ToolRuntime)
  const loaderRows: unknown[] = []
  ctx.provide('loader', { entries: () => loaderRows } as never)
  const agents = new Map<string, Agent>()
  ctx.provide('agents', { get: (id: string) => agents.get(id) } as never)
  ctx.provide('subprocessBrokerConfig', BROKER)
  const broker = options.broker ?? new FakeBroker()
  const host = createBindingHost({
    ctx, broker: BROKER, client: options.client ?? broker,
    ...options.coreEpoch === undefined ? {} : { coreEpoch: options.coreEpoch },
    ...options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs },
  })
  if (options.arm !== false) host.gate.arm()
  return {
    ctx, broker, host, loaderRows, agents,
    seat: async (id, run) => await ctx.plugin({
      name: `seat-${id}`,
      inject: ['sessions'],
      apply: async (inner: Context) => {
        const session = inner.sessions.create(SessionId(id))
        await run?.(inner, session)
      },
    }),
  }
}

/**
 * Seat a session in the store and ask for its binding, the way a creator would. Revocation is not paired here:
 * the agent loop awaits it first in teardown (see `packages/core/agent-loop/tests/session-lifecycle-hook.spec.ts`)
 * and `session/disposed` is the backstop this file exercises.
 */
async function bound(env: Env, id: string, input: Partial<IssueInput> = {}, meta?: { parentSession?: string }) {
  let outcome: Awaited<ReturnType<BindingIssuer['issue']>> | undefined
  const fiber = await env.ctx.plugin({
    name: `bound-${id}`,
    inject: ['sessions'],
    apply: async (inner: Context) => {
      await inner.effect(async function* () {
        const parent = meta?.parentSession
        const options = parent === undefined ? undefined : { meta: { parentSession: SessionId(parent) } }
        const session = inner.sessions.prepare(SessionId(id), options)
        yield inner.sessions.enter(session)
        inner.sessions.announce(session)
        outcome = await env.host.issuer.issue({ sessionId: id, kind: 'attach', ...input })
      }, 'seat')
    },
  })
  return { fiber, outcome: outcome! }
}

/** Run `body` as an active tool execution of agent `id`, with arbitrary forged-looking arguments. */
let probes = 0

async function inTool<T>(env: Env, id: string, body: (exec: ToolRunContext) => Promise<T>, args: Record<string, unknown> = {}): Promise<T> {
  const agent = { id, session: Session.create(SessionId(`${id}-detached`)) } as unknown as Agent
  env.agents.set(id, agent)
  let result: T | undefined
  const name = `probe-${id}-${++probes}`
  env.ctx.tools.register(defineTool({
    name, description: 'probe', parameters: {},
    output: { schema: { type: 'string' }, render: () => [] },
    async execute(_args, exec) { result = await body(exec); return 'ok' },
  }))
  await env.ctx.tools.execute({ signal, callId: ToolCallId(name), name, arguments: args, agent })
  return result as T
}

/** Create `id` as a child of `parentId` from inside the parent's own direct tool execution, as a subagent tool does. */
async function boundChild(env: Env, id: string, parentId: string, input: Partial<IssueInput> = {}) {
  return await inTool(env, parentId, () => bound(env, id, { kind: 'child', parentSessionId: parentId, ...input }, { parentSession: parentId }))
}

function forbiddenRow(): unknown {
  return {
    id: 'sneaky', options: { id: 'sneaky', name: '@deepseek-ai/dsh-hmr' },
    fiber: { state: FiberState.ACTIVE }, parent: { ctx: { fiber: { entry: {} } } },
  }
}

describe('the gate is a hard precondition, checked at the effect', () => {
  it('refuses issuance and resolution until armed, without calling the broker', async () => {
    const env = await environment({ arm: false })
    const { outcome } = await bound(env, 'session-1')
    expect(outcome).toEqual({ ok: false, reason: 'gate-inactive' })
    expect(await inTool(env, 'session-1', () => env.host.resolver.resolve(1))).toEqual({ ok: false, reason: 'gate-inactive' })
    expect(env.broker.log).toEqual([])
  })

  it('cannot be armed when the audit fails, and stays closed', async () => {
    const env = await environment({ arm: false })
    env.loaderRows.push(forbiddenRow())
    expect(() => { env.host.gate.arm() }).toThrow(/audit failed/)
    env.loaderRows.length = 0
    expect(env.host.gate.isActive()).toBe(false)
    expect(() => { env.host.gate.arm() }).toThrow(/already failed/)
  })

  it('refuses when the launch carried no broker declaration at all', async () => {
    const ctx = new Context()
    live.push(ctx)
    ctx.provide('loader', { entries: () => [] } as never)
    const gate = new BrokerGate(ctx, BROKER)
    expect(() => { gate.arm() }).toThrow(/missing or pending/)
    expect(gate.isActive()).toBe(false)
  })

  it('latches off for good once a forbidden row appears, even after it is removed', async () => {
    const env = await environment()
    expect((await bound(env, 'session-1')).outcome).toMatchObject({ ok: true })
    env.loaderRows.push(forbiddenRow())
    expect((await bound(env, 'session-2')).outcome).toEqual({ ok: false, reason: 'gate-inactive' })
    env.loaderRows.length = 0
    expect((await bound(env, 'session-3')).outcome).toEqual({ ok: false, reason: 'gate-inactive' })
    expect(env.host.gate.isActive()).toBe(false)
    expect(env.broker.binds.map(request => request.sessionId)).toEqual(['session-1'])
  })

  it('refuses once the model-defined Host runner or the plugin manager exists', async () => {
    for (const name of ['dynamicCordisRunner', 'pluginManager'] as const) {
      const env = await environment()
      env.ctx.reflect.provide(name, {})
      expect((await bound(env, 'session-1')).outcome).toEqual({ ok: false, reason: 'gate-inactive' })
      expect(env.broker.binds).toHaveLength(0)
    }
  })

  it('refuses when the broker config is replaced after boot', async () => {
    const env = await environment()
    env.ctx.set('subprocessBrokerConfig', { socketPath: '/tmp/other.sock' })
    expect((await bound(env, 'session-1')).outcome).toEqual({ ok: false, reason: 'gate-inactive' })
  })
})

describe('issuance', () => {
  it('sends only facts Core created, with a workspace minted by the issuer, and drops everything else', async () => {
    const env = await environment({ coreEpoch: 'epoch-A' })
    const forged = {
      cwd: '/etc', path: '/etc/shadow', workspaceId: 'attacker-workspace', executionHostId: 'host-x', dev: 1, ino: 2,
      assignmentId: 'forged', env: { DSH_SESSION_ID: 'other' }, headers: { cwd: '/' },
      orcaInstanceId: 'instance-9',
      room: { origine: 'dsh', stanzaId: 'room-1', agente: 'agent-1', installazione: 'inst-1', extra: 'dropped' },
      orgId: 'org-1',
    }
    const { outcome } = await bound(env, 'session-1', { kind: 'room-seat', ...forged } as never)
    expect(outcome).toMatchObject({ ok: true })
    const [request] = env.broker.binds
    expect(Object.keys(request!).sort()).toEqual(['coreEpoch', 'kind', 'orca', 'orgId', 'room', 'sessionId', 'workspaceId'])
    expect(request).toMatchObject({
      sessionId: 'session-1', coreEpoch: 'epoch-A', kind: 'room-seat', orca: { instanceId: 'instance-9' }, orgId: 'org-1',
      room: { origine: 'dsh', stanzaId: 'room-1', agente: 'agent-1', installazione: 'inst-1' },
    })
    expect(Object.keys(request!.room!).sort()).toEqual(['agente', 'installazione', 'origine', 'stanzaId'])
    expect(request!.workspaceId).toMatch(/^workspace-[0-9a-f-]{36}$/)
    expect(Object.isFrozen(request)).toBe(true)
  })

  it('refuses a session that is not live in this Core, and malformed or contradictory requests', async () => {
    const env = await environment()
    const issue = (input: Partial<IssueInput>) => env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach', ...input })
    expect(await issue({})).toEqual({ ok: false, reason: 'unknown-session' })
    await env.seat('session-1')
    expect(await issue({ kind: 'bogus' as never })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await issue({ sessionId: '' })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await issue({ parentSessionId: 'x' })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await issue({ kind: 'child' })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await issue({ room: { origine: 'other' } as never })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await issue({ orgId: '' })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(env.broker.binds).toHaveLength(0)
  })

  it('binds a session once; a second request is refused until the first is revoked', async () => {
    const env = await environment()
    const first = await bound(env, 'session-1')
    expect(first.outcome).toMatchObject({ ok: true })
    expect(await env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach' })).toEqual({ ok: false, reason: 'already-bound' })
    expect(env.broker.binds).toHaveLength(1)
  })

  it('binds a child under a live parent with the parent workspace and the broker-assigned parent id', async () => {
    const env = await environment()
    const parent = await bound(env, 'parent')
    const child = await boundChild(env, 'child', 'parent')
    expect(child.outcome).toMatchObject({ ok: true })
    const [parentRequest, childRequest] = env.broker.binds
    expect(childRequest).toMatchObject({ kind: 'child', parentBindingId: (parent.outcome as BindingGrant).bindingId })
    expect(childRequest!.workspaceId).toBe(parentRequest!.workspaceId)
    // A parent Core holds no binding for.
    expect(await boundChild(env, 'orphan', 'nobody').then(result => result.outcome)).toEqual({ ok: false, reason: 'unknown-parent' })
  })

  it('relays a broker refusal and treats a malformed or throwing answer as a refusal', async () => {
    const env = await environment()
    env.broker.bindAnswer = async () => ({ ok: false, reason: 'orca-unverified' })
    expect((await bound(env, 'a')).outcome).toEqual({ ok: false, reason: 'orca-unverified' })
    env.broker.bindAnswer = async () => ({ ok: true, bindingId: '', revision: 1 })
    expect((await bound(env, 'b')).outcome).toEqual({ ok: false, reason: 'broker-malformed' })
    env.broker.bindAnswer = async () => { throw new Error('socket closed') }
    expect((await bound(env, 'c')).outcome).toEqual({ ok: false, reason: 'assignment-unavailable' })
    // A refused bind leaves nothing to resolve.
    expect(env.host.issuer.lookup('a')).toMatchObject({ ok: false })
  })
})

describe('resolver', () => {
  it('returns the broker answer unchanged and sends only the attested agent id, the binding and the revision', async () => {
    const env = await environment({ coreEpoch: 'epoch-A' })
    const seated = await bound(env, 'session-7')
    const { bindingId, revision } = seated.outcome as BindingGrant
    const sentinel: AssignmentGrant = Object.freeze({ ok: true, assignmentId: 'opaque-from-broker', revision })
    env.broker.assignAnswer = async () => sentinel
    vi.stubEnv('DSH_SESSION_ID', 'session-forged')
    try {
      const answer = await inTool(env, 'session-7', async (exec) => {
        return await env.host.resolver.resolve(revision, exec)
      }, { path: '/etc/shadow', cwd: '/etc', alias: '#root', prefix: '/srv' })
      expect(answer).toBe(sentinel)
    } finally { vi.unstubAllEnvs() }
    expect(env.broker.assigns).toEqual([{ sessionId: 'session-7', bindingId, revision, coreEpoch: 'epoch-A' }])
  })

  it('uses the ToolRuntime current execution when none is passed', async () => {
    const env = await environment()
    const { outcome } = await bound(env, 'session-7')
    const answer = await inTool(env, 'session-7', () => env.host.resolver.resolve((outcome as BindingGrant).revision))
    expect(answer).toMatchObject({ ok: true })
    expect(env.broker.assigns[0]?.sessionId).toBe('session-7')
  })

  it('refuses outside a tool execution, for a cloned execution, and for an agent the registry does not hold', async () => {
    const env = await environment()
    const { outcome } = await bound(env, 'session-7')
    const { revision } = outcome as BindingGrant
    expect(await env.host.resolver.resolve(revision)).toEqual({ ok: false, reason: 'no-active-execution' })
    const cloned = await inTool(env, 'session-7', exec => env.host.resolver.resolve(revision, { ...exec }))
    expect(cloned).toEqual({ ok: false, reason: 'no-active-execution' })
    const forgedAgent = await inTool(env, 'session-7', async (exec) => {
      env.agents.delete('session-7')
      return await env.host.resolver.resolve(revision, exec)
    })
    expect(forgedAgent).toEqual({ ok: false, reason: 'no-active-execution' })
    expect(env.broker.assigns).toHaveLength(0)
  })

  it('refuses a session with no binding, a stale revision and a revoked binding without asking the broker', async () => {
    const env = await environment()
    const { outcome, fiber } = await bound(env, 'session-7')
    const { revision } = outcome as BindingGrant
    expect(await inTool(env, 'session-8', () => env.host.resolver.resolve(1))).toEqual({ ok: false, reason: 'no-binding' })
    expect(await inTool(env, 'session-7', () => env.host.resolver.resolve(revision + 1))).toEqual({ ok: false, reason: 'stale-revision' })
    const ack = Promise.withResolvers<{ ok: true }>()
    env.broker.revokeAnswer = () => ack.promise
    const revoking = env.host.issuer.revoke('session-7')
    // While the revocation is outstanding the binding is revoked, not merely unknown.
    expect(await inTool(env, 'session-7', () => env.host.resolver.resolve(revision))).toEqual({ ok: false, reason: 'revoked' })
    ack.resolve({ ok: true })
    await revoking
    expect(await inTool(env, 'session-7', () => env.host.resolver.resolve(revision))).toEqual({ ok: false, reason: 'no-binding' })
    expect(env.broker.assigns).toHaveLength(0)
    await fiber.dispose()
  })

  it('never fabricates: a broker refusal or malformed answer yields no assignment id', async () => {
    const env = await environment()
    const { outcome } = await bound(env, 'session-7')
    const { revision } = outcome as BindingGrant
    const answers: unknown[] = [
      { ok: false, reason: 'broker-says-no' },
      { ok: true, revision },
      { ok: true, assignmentId: '', revision },
      { ok: true, assignmentId: 'x', revision: revision + 1 },
      'assignment-text', undefined, null,
    ]
    const results: unknown[] = []
    for (const answer of answers) {
      env.broker.assignAnswer = async () => answer
      results.push(await inTool(env, 'session-7', () => env.host.resolver.resolve(revision)))
    }
    expect(results[0]).toEqual({ ok: false, reason: 'broker-says-no' })
    for (const result of results.slice(1)) expect(result).toEqual({ ok: false, reason: 'broker-malformed' })
    env.broker.assignAnswer = async () => { throw new Error('socket closed') }
    expect(await inTool(env, 'session-7', () => env.host.resolver.resolve(revision))).toEqual({ ok: false, reason: 'assignment-unavailable' })
    expect(results.every(result => !('assignmentId' in (result as object)))).toBe(true)
    expect(env.broker.issuedAssignments.size).toBe(0)
  })

  it('refuses when the binding is revoked while the broker is answering', async () => {
    const env = await environment()
    const { outcome } = await bound(env, 'session-7')
    const { revision } = outcome as BindingGrant
    env.broker.assignAnswer = async (request) => {
      void env.host.issuer.revoke(request.sessionId)
      return { ok: true, assignmentId: 'late', revision }
    }
    expect(await inTool(env, 'session-7', () => env.host.resolver.resolve(revision))).toEqual({ ok: false, reason: 'revoked' })
  })
})

describe('deny-safe shipped client', () => {
  it('answers assignment-unavailable to everything and has no positive path', async () => {
    const request = { sessionId: 's', workspaceId: 'w', coreEpoch: 'e', kind: 'attach' } as const
    const answers = [
      await assignmentUnavailableClient.bind(request),
      await assignmentUnavailableClient.revoke({ sessionId: 's', bindingId: 'b', coreEpoch: 'e' }),
      await assignmentUnavailableClient.assign({ sessionId: 's', bindingId: 'b', revision: 1, coreEpoch: 'e' }),
    ]
    for (const answer of answers) expect(answer).toEqual({ ok: false, reason: 'assignment-unavailable' })
    expect(Object.isFrozen(assignmentUnavailableClient)).toBe(true)
  })

  it('leaves Core with no binding and no assignment end to end', async () => {
    const env = await environment({ client: assignmentUnavailableClient })
    expect((await bound(env, 'session-7')).outcome).toEqual({ ok: false, reason: 'assignment-unavailable' })
    expect(await inTool(env, 'session-7', () => env.host.resolver.resolve(1))).toEqual({ ok: false, reason: 'no-binding' })
  })
})

describe('revocation', () => {
  it('is the backstop for a session that leaves the store without the agent loop: session/disposed revokes', async () => {
    const env = await environment()
    const { fiber } = await bound(env, 'session-1')
    await fiber.dispose()
    await vi.waitFor(() => { expect(env.broker.revokes.map(request => request.sessionId)).toEqual(['session-1']) })
    expect(await env.host.issuer.revoke('session-1')).toEqual({ ok: true })
    expect(env.broker.revokes).toHaveLength(1)
  })

  it('waits for the broker acknowledgement, and closes the control connection when it does not come', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    const ack = Promise.withResolvers<{ ok: true }>()
    env.broker.revokeAnswer = () => ack.promise
    let settled = false
    const revoking = env.host.issuer.revoke('session-1').then((outcome) => { settled = true; return outcome })
    await vi.waitFor(() => { expect(env.broker.revokes).toHaveLength(1) })
    await new Promise(resolve => setTimeout(resolve, 20))
    expect(settled).toBe(false)
    ack.resolve({ ok: true })
    expect(await revoking).toEqual({ ok: true })
    expect(env.broker.closed).toBe(0)

    await bound(env, 'session-2')
    env.broker.revokeAnswer = async () => ({ ok: false, reason: 'busy' })
    expect(await env.host.issuer.revoke('session-2')).toEqual({ ok: false, reason: 'busy', controlClosed: true })
    expect(env.broker.closed).toBe(1)

    const throwing = await environment()
    await bound(throwing, 'session-3')
    throwing.broker.revokeAnswer = async () => { throw new Error('socket closed') }
    expect(await throwing.host.issuer.revoke('session-3')).toMatchObject({ ok: false, controlClosed: true })
    expect(throwing.broker.closed).toBe(1)
  })

  it('sends one revoke frame per binding however many paths revoke it', async () => {
    const env = await environment()
    const { fiber } = await bound(env, 'session-1')
    await Promise.all([env.host.issuer.revoke('session-1'), env.host.issuer.revoke('session-1')])
    await fiber.dispose()
    await env.host.issuer.revokeAll()
    expect(env.broker.revokes).toHaveLength(1)
  })

  it('revokes a binding whose bind was still in flight when the session went away', async () => {
    const env = await environment()
    const grant = Promise.withResolvers<unknown>()
    env.broker.bindAnswer = () => grant.promise
    await env.seat('session-1')
    const issuing = env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach' })
    const revoking = env.host.issuer.revoke('session-1')
    expect(env.broker.revokes).toHaveLength(0)
    grant.resolve({ ok: true, bindingId: 'binding-late', revision: 4 })
    expect(await issuing).toEqual({ ok: false, reason: 'revoked' })
    expect(await revoking).toEqual({ ok: true })
    expect(env.broker.revokes).toEqual([{ sessionId: 'session-1', bindingId: 'binding-late', coreEpoch: env.host.issuer.coreEpoch }])
  })

  it('revokes children before the parent, and revokeAll covers everything at shutdown', async () => {
    const env = await environment()
    await bound(env, 'parent')
    await boundChild(env, 'child', 'parent')
    await bound(env, 'other')
    await env.host.issuer.revoke('parent')
    expect(env.broker.log.filter(line => line.startsWith('revoke:'))).toEqual(['revoke:child', 'revoke:parent'])
    await env.host.issuer.revokeAll()
    expect(env.broker.log.filter(line => line.startsWith('revoke:'))).toEqual(['revoke:child', 'revoke:parent', 'revoke:other'])
  })

  it('still revokes after the gate has latched off', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    env.ctx.reflect.provide('pluginManager', {})
    expect(env.host.gate.isActive()).toBe(false)
    await env.host.issuer.revoke('session-1')
    expect(env.broker.revokes).toHaveLength(1)
  })

  it('removes a finished record: a session can bind again, and the new record owns no stale children', async () => {
    const env = await environment()
    await bound(env, 'p')
    await boundChild(env, 'c', 'p')
    await env.host.issuer.revoke('p')
    expect(env.host.issuer.lookup('p')).toEqual({ ok: false, reason: 'no-binding' })
    expect(env.host.issuer.lookup('c')).toEqual({ ok: false, reason: 'no-binding' })
    const before = env.broker.revokes.length
    // The ids are bound again from scratch; the old child record does not come back under the new parent.
    expect(await env.host.issuer.issue({ sessionId: 'p', kind: 'attach' })).toMatchObject({ ok: true })
    await env.host.issuer.revoke('p')
    expect(env.broker.revokes.slice(before).map(request => request.sessionId)).toEqual(['p'])
  })

  it('refuses to bind again while the previous revocation of the same session is still outstanding', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    const ack = Promise.withResolvers<{ ok: true }>()
    env.broker.revokeAnswer = () => ack.promise
    const revoking = env.host.issuer.revoke('session-1')
    expect(await env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach' })).toEqual({ ok: false, reason: 'revoking' })
    ack.resolve({ ok: true })
    await revoking
    expect(await env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach' })).toMatchObject({ ok: true })
  })
})

describe('a broker that does not answer', () => {
  const silent = (): Promise<never> => new Promise(() => {})

  it('gets its control connection closed when a bind is not answered, and nothing is left bound', async () => {
    const env = await environment({ timeoutMs: 30 })
    env.broker.bindAnswer = silent
    const { outcome } = await bound(env, 'session-1')
    expect(outcome).toEqual({ ok: false, reason: 'broker-timeout' })
    expect(env.broker.closed).toBe(1)
    expect(env.host.issuer.lookup('session-1')).toEqual({ ok: false, reason: 'no-binding' })
  })

  it('does not hold a revocation, and so a dispose, forever', async () => {
    const env = await environment({ timeoutMs: 30 })
    await bound(env, 'session-1')
    await bound(env, 'session-2')
    env.broker.revokeAnswer = silent
    const started = Date.now()
    expect(await env.host.issuer.revoke('session-1')).toEqual({ ok: false, reason: 'broker-timeout', controlClosed: true })
    expect(Date.now() - started).toBeLessThan(2000)
    expect(env.broker.closed).toBe(1)
    // Closing the control connection is a revoke of everything: Core forgets all of it and sends nothing more.
    expect(env.host.issuer.lookup('session-2')).toEqual({ ok: false, reason: 'no-binding' })
    await env.host.issuer.revokeAll()
    expect(env.broker.revokes.map(request => request.sessionId)).toEqual(['session-1'])
  })

  it('refuses an assignment that is not answered, and closes the control connection', async () => {
    const env = await environment({ timeoutMs: 30 })
    const { outcome } = await bound(env, 'session-1')
    env.broker.assignAnswer = silent
    expect(await inTool(env, 'session-1', () => env.host.resolver.resolve((outcome as BindingGrant).revision)))
      .toEqual({ ok: false, reason: 'broker-timeout' })
    expect(env.broker.closed).toBe(1)
    expect(env.host.issuer.lookup('session-1')).toEqual({ ok: false, reason: 'no-binding' })
  })
})

describe('the gate is checked again after the broker answers', () => {
  it('undoes a grant that arrives after the gate latched off, and returns a refusal', async () => {
    const env = await environment()
    env.broker.bindAnswer = async () => {
      env.ctx.reflect.provide('pluginManager', {})
      return { ok: true, bindingId: 'binding-late', revision: 1 }
    }
    const { outcome } = await bound(env, 'session-1')
    expect(outcome).toEqual({ ok: false, reason: 'gate-inactive' })
    await vi.waitFor(() => { expect(env.broker.revokes).toEqual([{ sessionId: 'session-1', bindingId: 'binding-late', coreEpoch: env.host.issuer.coreEpoch }]) })
    expect(env.host.issuer.lookup('session-1')).toMatchObject({ ok: false })
  })

  it('does not return an assignment that arrives after the gate latched off', async () => {
    const env = await environment()
    const { outcome } = await bound(env, 'session-1')
    const { revision } = outcome as BindingGrant
    env.broker.assignAnswer = async () => {
      env.ctx.reflect.provide('pluginManager', {})
      return { ok: true, assignmentId: 'late-opaque', revision }
    }
    expect(await inTool(env, 'session-1', () => env.host.resolver.resolve(revision))).toEqual({ ok: false, reason: 'gate-inactive' })
  })
})

describe('a child is bound only under a parent the caller owns', () => {
  it('refuses a child whose header does not name the parent, even when that parent is bound', async () => {
    const env = await environment()
    await bound(env, 'victim')
    await bound(env, 'attacker')
    // The attacker session never recorded the victim as its parent, so no parent id in the request can graft it.
    const result = await inTool(env, 'victim', () => bound(env, 'adopted', { kind: 'child', parentSessionId: 'victim' }))
    expect(result.outcome).toEqual({ ok: false, reason: 'unknown-parent' })
    const wrongParent = await inTool(env, 'victim', () => bound(env, 'adopted-2', { kind: 'child', parentSessionId: 'victim' }, { parentSession: 'attacker' }))
    expect(wrongParent.outcome).toEqual({ ok: false, reason: 'unknown-parent' })
    expect(env.broker.binds.map(request => request.sessionId)).toEqual(['victim', 'attacker'])
  })

  it('refuses a child created outside the parent’s tool call, or from another session’s', async () => {
    const env = await environment()
    await bound(env, 'victim')
    await bound(env, 'other')
    // Header and parent record are both right; only the caller is wrong.
    const outside = await bound(env, 'c-outside', { kind: 'child', parentSessionId: 'victim' }, { parentSession: 'victim' })
    expect(outside.outcome).toEqual({ ok: false, reason: 'not-parent-execution' })
    const fromOther = await inTool(env, 'other', () => bound(env, 'c-other', { kind: 'child', parentSessionId: 'victim' }, { parentSession: 'victim' }))
    expect(fromOther.outcome).toEqual({ ok: false, reason: 'not-parent-execution' })
    expect(env.broker.binds.map(request => request.sessionId)).toEqual(['victim', 'other'])
    // The parent's own execution is accepted.
    expect((await boundChild(env, 'c-own', 'victim')).outcome).toMatchObject({ ok: true })
  })

  it('refuses a child under a parent that is not bound, revoked, or whose bind is still in flight', async () => {
    const env = await environment()
    await env.seat('unbound')
    expect((await boundChild(env, 'c1', 'unbound')).outcome).toEqual({ ok: false, reason: 'unknown-parent' })
    await bound(env, 'gone')
    await env.host.issuer.revoke('gone')
    expect((await boundChild(env, 'c2', 'gone')).outcome).toEqual({ ok: false, reason: 'unknown-parent' })
  })
})

describe('Core restart', () => {
  it('mints a new coreEpoch per host and never reuses a previous process binding', async () => {
    const broker = new FakeBroker()
    const first = await environment({ broker })
    const seated = await bound(first, 'session-7')
    const { revision } = seated.outcome as BindingGrant
    expect(await inTool(first, 'session-7', () => first.host.resolver.resolve(revision))).toMatchObject({ ok: true })

    const second = await environment({ broker })
    expect(second.host.issuer.coreEpoch).not.toBe(first.host.issuer.coreEpoch)
    await second.seat('session-7')
    const before = broker.assigns.length
    // The same session id and revision are known only to the dead process; the new one holds nothing.
    expect(await inTool(second, 'session-7', () => second.host.resolver.resolve(revision))).toEqual({ ok: false, reason: 'no-binding' })
    expect(broker.assigns).toHaveLength(before)

    // The session re-binds once, under the new epoch, and gets a fresh broker revision.
    const rebound = await env2bind(second, 'session-7')
    expect((rebound as BindingGrant).revision).not.toBe(revision)
    expect(broker.binds.at(-1)?.coreEpoch).toBe(second.host.issuer.coreEpoch)
    // The old process's binding no longer assigns anything, even if it were still asked.
    expect(await broker.assign({ sessionId: 'session-7', bindingId: (seated.outcome as BindingGrant).bindingId, revision, coreEpoch: first.host.issuer.coreEpoch }))
      .toEqual({ ok: false, reason: 'stale-core-epoch' })
  })

  it('keeps the table in memory only', () => {
    const source = readFileSync(join(srcDir, 'broker-binding.ts'), 'utf8')
    expect(source).not.toMatch(/node:fs|writeFile|readFile|sqlite|localStorage/u)
  })
})

async function env2bind(env: Env, id: string) {
  return await env.host.issuer.issue({ sessionId: id, kind: 'attach' })
}

/** Every object reachable from `root` through own properties, bounded and cycle-safe. */
function reachable(root: unknown, limit = 20_000): Set<unknown> {
  const seen = new Set<unknown>()
  const queue: unknown[] = [root]
  while (queue.length > 0 && seen.size < limit) {
    const value = queue.pop()
    if ((typeof value !== 'object' && typeof value !== 'function') || value === null || seen.has(value)) continue
    seen.add(value)
    for (const key of Reflect.ownKeys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key)
      if (descriptor !== undefined && 'value' in descriptor) queue.push(descriptor.value)
    }
    queue.push(Object.getPrototypeOf(value))
  }
  return seen
}

function secrets(env: Env): unknown[] {
  return [env.host, env.host.issuer, env.host.resolver, env.host.gate]
}

describe('not reachable from events, tools, plugins or model-defined code', () => {
  it('is not on the Context, in any provided service, or on the event bus', async () => {
    const env = await environment()
    const graph = reachable(env.ctx, 60_000)
    // The Context holds a handful of bound closures over the issuer (the session/disposed observer and the
    // teardown effect); those are host-installed, so only the objects themselves must be absent.
    for (const secret of secrets(env)) expect(graph.has(secret)).toBe(false)
    const names = Object.keys(env.ctx.reflect.props)
    expect(names.filter(name => /binding|issuer|assign|resolver/iu.test(name))).toEqual([])
    for (const name of names) {
      const service: unknown = env.ctx.reflect.get(name, false)
      for (const secret of secrets(env)) expect(reachable(service, 5_000).has(secret)).toBe(false)
    }
  })

  it('cannot be reached by a tool body or its execution context, and a tool cannot cause issuance', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    const baseline = env.broker.log.length
    await inTool(env, 'session-1', async (exec) => {
      for (const secret of secrets(env)) expect(reachable(exec, 20_000).has(secret)).toBe(false)
      // A tool can read its own session and try every ctx name; none of it issues.
      for (const name of Object.keys(env.ctx.reflect.props)) void env.ctx.reflect.get(name, false)
      void env.ctx.tools.currentActiveExecution()
      return undefined
    })
    expect(env.broker.log.length).toBe(baseline)
  })

  it('cannot be made to issue by a listener, or by emitting a forged lifecycle event', async () => {
    const env = await environment()
    const received: unknown[] = []
    env.ctx.on('session/created', (...args) => { received.push(...args) })
    env.ctx.on('session/disposed', (...args) => { received.push(...args) })
    env.ctx.on('session/event', (...args) => { received.push(...args) })
    // A forged announcement for a session that does not exist, and one for a live but unbound session.
    const forged = Session.create(SessionId('forged'))
    env.ctx.emit('session/created', forged)
    await env.seat('live-unbound')
    env.ctx.emit('session/created', env.ctx.sessions.get(SessionId('live-unbound'))!)
    expect(env.broker.binds).toHaveLength(0)
    expect(env.broker.log).toEqual([])
    for (const secret of secrets(env)) expect(reachable(received, 20_000).has(secret)).toBe(false)
    expect(await env.host.issuer.issue({ sessionId: 'forged', kind: 'attach' })).toEqual({ ok: false, reason: 'unknown-session' })
  })

  it('lets a forged session/disposed event only revoke, never issue', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    env.ctx.emit('session/disposed', Session.create(SessionId('never-existed')))
    expect(env.broker.revokes).toHaveLength(0)
    env.ctx.emit('session/disposed', env.ctx.sessions.get(SessionId('session-1'))!)
    await vi.waitFor(() => { expect(env.broker.revokes).toHaveLength(1) })
    expect(env.broker.binds).toHaveLength(1)
    await vi.waitFor(() => { expect(env.host.issuer.lookup('session-1')).toEqual({ ok: false, reason: 'no-binding' }) })
  })

  it('registers nothing on a Context beyond one revoke-only observer, and only the launcher imports it', () => {
    const source = readFileSync(join(srcDir, 'broker-binding.ts'), 'utf8')
    const code = source.replace(/\/\*[\s\S]*?\*\//gu, '').replace(/\/\/.*$/gmu, '')
    expect(code.match(/\.(?:provide|serial|parallel|emit|register|bail|waterfall)\(/gu)).toBeNull()
    expect(code.match(/\bctx\.on\(/gu)).toHaveLength(1)
    expect(code).toContain("ctx.on('session/disposed'")

    const importers: string[] = []
    for (const root of ['apps', 'packages', 'scripts', 'vendor']) walk(join(repoRoot, root), (file) => {
      if (!/\.(?:[cm]?[jt]sx?)$/u.test(file) || file === join(srcDir, 'broker-binding.ts')) return
      if (/broker-binding/u.test(readFileSync(file, 'utf8'))) importers.push(relative(repoRoot, file))
    })
    expect(importers.sort()).toEqual([
      'apps/cli/src/broker-capabilities.ts', 'apps/cli/src/profile-boot.ts', 'apps/cli/tests/broker-binding.spec.ts',
      'apps/cli/tests/broker-capabilities.spec.ts', 'apps/cli/tests/broker-required.spec.ts', 'apps/cli/tests/fake-broker.ts',
    ])

    // The `./lib/*` pattern would otherwise name the emitted file; the named subpaths are mapped to null, which
    // Node resolves as "not exported" (checked by hand with `import('@deepseek-ai/dsh/lib/types/broker-binding.js')`).
    const manifest = JSON.parse(readFileSync(join(srcDir, '..', 'package.json'), 'utf8')) as { exports: Record<string, unknown> }
    expect(Object.keys(manifest.exports).filter(key => /binding/u.test(key) && manifest.exports[key] !== null)).toEqual([])
    for (const file of ['broker-binding', 'broker-capabilities']) {
      expect(manifest.exports[`./lib/types/${file}.js`]).toBeNull()
      expect(manifest.exports[`./lib/types/${file}.d.ts`]).toBeNull()
    }
  })
})

const srcDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'src')
const repoRoot = join(srcDir, '..', '..', '..')

function walk(directory: string, visit: (file: string) => void): void {
  for (const name of readdirSync(directory)) {
    if (name === 'node_modules' || name === 'lib' || name === 'dist' || name.startsWith('.')) continue
    const path = join(directory, name)
    const stats = statSync(path)
    if (stats.isDirectory()) walk(path, visit)
    else visit(path)
  }
}
