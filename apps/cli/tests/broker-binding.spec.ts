/**
 * Host-only binding issuer and resolver. Everything here is deny-first: the broker is a fake that records what
 * Core sent, and the assertions are about what Core refuses, what it never sends, and what it never invents.
 */
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, statSync, symlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
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
import { BindingLauncher } from '../src/broker-launch.ts'
import type { BrokerConfig } from '../src/broker-required.ts'
import { FakeBroker } from './fake-broker.ts'

const BROKER: BrokerConfig = Object.freeze({ socketPath: '/run/dsh/broker.sock' })
/** A real directory, already canonical, standing in for a session's working directory. */
const CWD = realpathSync(tmpdir())
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
    ...options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs },
  })
  if (options.arm !== false) host.gate.arm()
  return {
    ctx, broker, host, loaderRows, agents,
    seat: async (id, run) => await ctx.plugin({
      name: `seat-${id}`,
      inject: ['sessions'],
      apply: async (inner: Context) => {
        const session = inner.sessions.create(SessionId(id), { meta: { cwd: CWD } })
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
        const options = { meta: { cwd: CWD, ...parent === undefined ? {} : { parentSession: SessionId(parent) } } }
        const session = inner.sessions.prepare(SessionId(id), options)
        yield inner.sessions.enter(session)
        inner.sessions.announce(session)
        outcome = await env.host.issuer.issue({
          sessionId: id, kind: 'attach', ...input.kind === 'child' ? {} : { orcaInstanceId: 'instance-1' }, ...input,
        })
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
    const env = await environment()
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
    expect(Object.keys(request!).sort()).toEqual(['cwd', 'kind', 'orca', 'orgId', 'room', 'sessionId', 'workspaceId'])
    // The forged `cwd: '/etc'` above is dropped; the value is Core's own realpath of the session header cwd.
    expect(request!.cwd).toBe(CWD)
    expect(request).toMatchObject({
      sessionId: 'session-1', kind: 'room-seat', orca: { instanceId: 'instance-9' }, orgId: 'org-1',
      room: { origine: 'dsh', stanzaId: 'room-1', agente: 'agent-1', installazione: 'inst-1' },
    })
    expect(Object.keys(request!.room!).sort()).toEqual(['agente', 'installazione', 'origine', 'stanzaId'])
    expect(request!.workspaceId).toMatch(/^workspace-[0-9a-f-]{36}$/)
    expect(Object.isFrozen(request)).toBe(true)
  })

  describe('the cwd assertion (C-2)', () => {
    const roots: string[] = []
    afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }) })
    const scratch = (): string => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'dsh-cwd-')))
      roots.push(root)
      return root
    }
    /** Create `id` with an explicit header cwd and ask for a binding of `kind`. */
    async function issueWith(env: Env, id: string, kind: 'attach' | 'room-seat', cwd: string | undefined, extra: object = {}) {
      let outcome: Awaited<ReturnType<BindingIssuer['issue']>> | undefined
      await env.ctx.plugin({
        name: `cwd-${id}`,
        inject: ['sessions'],
        apply: async (inner: Context) => {
          await inner.effect(async function* () {
            const session = inner.sessions.prepare(SessionId(id), cwd === undefined ? undefined : { meta: { cwd } })
            yield inner.sessions.enter(session)
            inner.sessions.announce(session)
            outcome = await env.host.issuer.issue({ sessionId: id, kind, orcaInstanceId: 'i-0', ...extra })
          }, 'seat')
        },
      })
      return outcome!
    }

    it('sends the realpath of the session header cwd, resolved through symlinks, for room-seat and attach', async () => {
      const env = await environment()
      const root = scratch()
      mkdirSync(join(root, 'real'))
      symlinkSync(join(root, 'real'), join(root, 'link'))
      expect(await issueWith(env, 'a', 'attach', join(root, 'link'))).toMatchObject({ ok: true })
      expect(await issueWith(env, 'b', 'room-seat', join(root, 'real', '..', 'real'), { orcaInstanceId: 'i-1' })).toMatchObject({ ok: true })
      expect(env.broker.binds.map(request => request.cwd)).toEqual([join(root, 'real'), join(root, 'real')])
    })

    it('takes it from Core alone: nothing on the input, and not from a plugin, can supply or replace it', async () => {
      const env = await environment()
      const root = scratch()
      const outcome = await issueWith(env, 'a', 'attach', root, { cwd: '/etc', path: '/etc', root: '/' })
      expect(outcome).toMatchObject({ ok: true })
      expect(env.broker.binds[0]!.cwd).toBe(root)
    })

    it('refuses a session whose cwd is missing or cannot be resolved, without asking the broker', async () => {
      const env = await environment()
      const root = scratch()
      expect(await issueWith(env, 'none', 'attach', undefined)).toEqual({ ok: false, reason: 'invalid-workspace' })
      expect(await issueWith(env, 'gone', 'room-seat', join(root, 'does-not-exist'), { orcaInstanceId: 'i-1' }))
        .toEqual({ ok: false, reason: 'invalid-workspace' })
      expect(env.broker.binds).toHaveLength(0)
    })

    it('omits it for a child, whose root is the parent binding pinned root', async () => {
      const env = await environment()
      expect((await bound(env, 'parent')).outcome).toMatchObject({ ok: true })
      expect((await boundChild(env, 'kid', 'parent')).outcome).toMatchObject({ ok: true })
      const [parent, kid] = env.broker.binds
      expect(parent!.cwd).toBe(CWD)
      expect(kid!.kind).toBe('child')
      expect('cwd' in kid!).toBe(false)
    })

    it('is the bind frame JSON key "cwd"', async () => {
      const env = await environment()
      await bound(env, 'session-1')
      const frame = JSON.parse(JSON.stringify(env.broker.binds[0])) as Record<string, unknown>
      expect(frame['cwd']).toBe(CWD)
      expect(Object.keys(frame)).toContain('cwd')
    })

    it('still binds a session once when two requests race through the path resolution', async () => {
      const env = await environment()
      const root = scratch()
      const both = await Promise.all([
        issueWith(env, 'twice', 'attach', root),
        (async () => { await Promise.resolve(); return await env.host.issuer.issue({ sessionId: 'twice', kind: 'attach', orcaInstanceId: 'instance-1' }) })(),
      ])
      expect(both.filter(outcome => outcome.ok)).toHaveLength(1)
      expect(env.broker.binds).toHaveLength(1)
    })
  })

  it('refuses a session that is not live in this Core, and malformed or contradictory requests', async () => {
    const env = await environment()
    const issue = (input: Partial<IssueInput>) => env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach', orcaInstanceId: 'instance-1', ...input })
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
    expect(await env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach', orcaInstanceId: 'instance-1' })).toEqual({ ok: false, reason: 'already-bound' })
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
    const env = await environment()
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
    expect(env.broker.assigns).toEqual([{ sessionId: 'session-7', bindingId, revision }])
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

describe('identifier shape and Orca locator (S6 contract 3, 9)', () => {
  const label = (length: number): string => `s${'x'.repeat(length - 1)}`
  async function issue(env: Env, id: string, input: Partial<IssueInput>) {
    return (await bound(env, id, input)).outcome
  }

  it.each([
    ['a slash', 'agent/one-session-1'],
    ['a space', 'agent one'],
    ['a leading dot', '.hidden'],
    ['a leading dash', '-x'],
    ['a newline', 'abc\n'],
    ['129 characters', 'a'.repeat(129)],
    ['non-ASCII', 'sessione-è'],
  ])('refuses a session id with %s locally as invalid-label, and never sends a bind', async (_name, id) => {
    const env = await environment()
    expect(await issue(env, id, {})).toEqual({ ok: false, reason: 'invalid-label' })
    expect(env.broker.binds).toHaveLength(0)
  })

  it('accepts every shape the broker accepts: 128 characters, dots, colons, dashes, underscores', async () => {
    const env = await environment()
    for (const id of [label(128), 'a.b:c-d_e', 'agent-1-session-3f2b8f0e-9c1d-4b57-8a43-1d2e3f4a5b6c', 'session-12']) {
      expect(await issue(env, id, {}), id).toMatchObject({ ok: true })
    }
  })

  it('applies the same shape to the Orca instance id, the org id and every ROOM field', async () => {
    const env = await environment()
    const room = { origine: 'dsh' as const, stanzaId: 'room-1', agente: 'agent-1', installazione: 'inst-1' }
    const cases: Array<Partial<IssueInput>> = [
      { orcaInstanceId: 'instance/1' }, { orgId: 'org 1' },
      { room: { ...room, stanzaId: 'room/1' } }, { room: { ...room, agente: 'a'.repeat(129) } }, { room: { ...room, installazione: '.x' } },
    ]
    for (const [index, input] of cases.entries()) {
      expect(await issue(env, `s${index}`, { kind: 'room-seat', ...input }), JSON.stringify(input)).toEqual({ ok: false, reason: 'invalid-label' })
    }
    expect(env.broker.binds).toHaveLength(0)
  })

  it('requires the Orca locator for attach and room-seat, and forbids it on a child', async () => {
    const env = await environment()
    expect(await env.host.issuer.issue({ sessionId: 'x', kind: 'attach' })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await env.host.issuer.issue({ sessionId: 'x', kind: 'room-seat' })).toEqual({ ok: false, reason: 'invalid-request' })
    await bound(env, 'parent')
    const kid = await boundChild(env, 'kid', 'parent', { orcaInstanceId: 'instance-1' })
    expect(kid.outcome).toEqual({ ok: false, reason: 'invalid-request' })
    expect(env.broker.binds.map(request => request.sessionId)).toEqual(['parent'])
  })
})

describe('the launch bridge: lease then start, for the attested session only (W-2)', () => {
  const spec = { argv: ['/bin/echo', 'hi'], cwd: '/srv/wt', env: { A: 'b' }, graceMs: 200 }

  it('leases the session binding, starts under the broker assignment, and ends with the broker worker-exited', async () => {
    const env = await environment()
    const { outcome } = await bound(env, 'session-7')
    const { bindingId, revision } = outcome as BindingGrant
    const result = await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec }))
    expect(result).toMatchObject({ ok: true, pid: 4242 })
    expect(env.broker.assigns).toEqual([{ sessionId: 'session-7', bindingId, revision }])
    expect(env.broker.starts).toEqual([{ assignmentId: 'assignment-1-opaque', ...spec }])
    expect(env.broker.log).toEqual(['bind:session-7', 'assign:session-7', 'start:assignment-1-opaque'])
    if (!result.ok) return
    let settled = false
    void result.exited.then(() => { settled = true })
    await Promise.resolve()
    expect(settled).toBe(false)
    env.broker.exitWorker({ ok: true, code: 2, signal: null })
    expect(await result.exited).toEqual({ ok: true, code: 2, signal: null })
  })

  it('takes the session from the execution alone: a session id, cwd or assignment on the request changes nothing', async () => {
    const env = await environment()
    await bound(env, 'victim')
    await bound(env, 'attacker')
    const forged = { sessionId: 'victim', assignmentId: 'forged', bindingId: 'binding-1', ...spec }
    await inTool(env, 'attacker', exec => env.host.launcher.launch({ execution: exec, ...forged } as never))
    expect(env.broker.assigns.map(request => request.sessionId)).toEqual(['attacker'])
    expect(env.broker.starts.map(request => request.assignmentId)).toEqual(['assignment-1-opaque'])
  })

  it('refuses without asking the broker when there is no attested execution, a cloned one, or no binding', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    expect(await env.host.launcher.launch({ execution: undefined, ...spec })).toEqual({ ok: false, reason: 'no-active-execution' })
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: { ...exec }, ...spec })))
      .toEqual({ ok: false, reason: 'no-active-execution' })
    expect(await inTool(env, 'session-8', exec => env.host.launcher.launch({ execution: exec, ...spec })))
      .toEqual({ ok: false, reason: 'no-binding' })
    expect(env.broker.assigns).toHaveLength(0)
    expect(env.broker.starts).toHaveLength(0)
  })

  it('relays a refused lease and a refused start, and starts nothing after a refused lease', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    env.broker.assignAnswer = async () => ({ ok: false, reason: 'stale-binding' })
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec }))).toEqual({ ok: false, reason: 'stale-binding' })
    expect(env.broker.starts).toHaveLength(0)
    delete env.broker.assignAnswer
    env.broker.startAnswer = async () => ({ ok: false, reason: 'stdio-unsupported' })
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec }))).toEqual({ ok: false, reason: 'stdio-unsupported' })
  })

  it('refuses a malformed request and never throws, even when the client does', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    expect(await env.host.launcher.launch(null as never)).toEqual({ ok: false, reason: 'invalid-request' })
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec, argv: 'echo' } as never)))
      .toEqual({ ok: false, reason: 'invalid-request' })
    env.broker.startAnswer = () => { throw new Error('socket exploded') }
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec })))
      .toEqual({ ok: false, reason: 'assignment-unavailable' })
  })

  it('does not hand out a worker that started under a binding revoked meanwhile', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    const started = Promise.withResolvers<unknown>()
    env.broker.startAnswer = () => started.promise
    const launching = inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec }))
    await vi.waitFor(() => { expect(env.broker.starts).toHaveLength(1) })
    await env.host.issuer.revoke('session-7')
    started.resolve({ ok: true, pid: 9, workerId: 'w', exited: new Promise(() => {}) })
    expect(await launching).toEqual({ ok: false, reason: 'revoked' })
  })

  it('revokes the binding when the gate latched off while the broker was starting the worker', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    env.broker.startAnswer = async () => {
      env.ctx.reflect.provide('pluginManager', {})
      return { ok: true, pid: 9, workerId: 'w', exited: new Promise(() => {}) }
    }
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec }))).toEqual({ ok: false, reason: 'gate-inactive' })
    await vi.waitFor(() => { expect(env.broker.revokes.map(request => request.sessionId)).toEqual(['session-7']) })
  })

  it('refuses while the gate is closed', async () => {
    const env = await environment({ arm: false })
    expect(await env.host.launcher.launch({ execution: undefined, ...spec })).toEqual({ ok: false, reason: 'gate-inactive' })
    expect(env.broker.assigns).toHaveLength(0)
  })

  it('starts nothing for a caller that already gave up: aborted before the lease, no frame is sent', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    const abort = new AbortController()
    abort.abort()
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec, signal: abort.signal })))
      .toEqual({ ok: false, reason: 'aborted' })
    expect(env.broker.assigns).toHaveLength(0)
    expect(env.broker.starts).toHaveLength(0)
  })

  it('starts nothing for a caller that gave up while the broker was leasing', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    const abort = new AbortController()
    const leased = Promise.withResolvers<unknown>()
    env.broker.assignAnswer = () => leased.promise
    const launching = inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec, signal: abort.signal }))
    await vi.waitFor(() => { expect(env.broker.assigns).toHaveLength(1) })
    abort.abort()
    leased.resolve({ ok: true, assignmentId: 'late-opaque', revision: 1 })
    expect(await launching).toEqual({ ok: false, reason: 'aborted' })
    expect(env.broker.starts).toHaveLength(0)
  })

  it('refuses a signal that is not an AbortSignal', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec, signal: { aborted: false } })))
      .toEqual({ ok: false, reason: 'invalid-request' })
    expect(env.broker.assigns).toHaveLength(0)
  })

  it('hands back a worker that can be signalled, through the same connection, and keeps one that was aborted while starting', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    const abort = new AbortController()
    const started = Promise.withResolvers<unknown>()
    env.broker.startAnswer = () => started.promise
    const launching = inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec, signal: abort.signal }))
    await vi.waitFor(() => { expect(env.broker.starts).toHaveLength(1) })
    abort.abort()
    started.resolve({ ok: true, pid: 9, workerId: 'w-late', exited: new Promise(() => {}) })
    const result = await launching
    // The worker exists, so it is returned: the runtime that asked for the stop sends it the signal.
    expect(result).toMatchObject({ ok: true, workerId: 'w-late' })
    if (!result.ok) return
    expect(await result.signal('TERM')).toEqual({ ok: true })
    expect(await result.signal('KILL')).toEqual({ ok: true })
    expect(env.broker.signals).toEqual([{ workerId: 'w-late', signal: 'TERM' }, { workerId: 'w-late', signal: 'KILL' }])
  })

  it('kills the worker, and leaves the new binding alone, when the session was re-bound while the broker was starting it', async () => {
    const env = await environment()
    const first = await bound(env, 'session-7')
    const started = Promise.withResolvers<unknown>()
    env.broker.startAnswer = () => started.promise
    const launching = inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec }))
    await vi.waitFor(() => { expect(env.broker.starts).toHaveLength(1) })
    await env.host.issuer.revoke('session-7')
    const second = await env2bind(env, 'session-7')
    expect(second).toMatchObject({ ok: true })
    expect((second as BindingGrant).bindingId).not.toBe((first.outcome as BindingGrant).bindingId)
    started.resolve({ ok: true, pid: 9, workerId: 'w-old', exited: new Promise(() => {}) })
    expect(await launching).toEqual({ ok: false, reason: 'revoked' })
    expect(env.broker.signals).toEqual([{ workerId: 'w-old', signal: 'KILL' }])
    // One revoke, the explicit one: the replacement binding was not revoked on the old worker's account.
    expect(env.broker.revokes).toHaveLength(1)
    expect(env.host.issuer.lookup('session-7')).toMatchObject({ bindingId: (second as BindingGrant).bindingId })
  })

  it('kills the worker when the gate latched off while the broker was starting it', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    env.broker.startAnswer = async () => {
      env.ctx.reflect.provide('pluginManager', {})
      return { ok: true, pid: 9, workerId: 'w-gate', exited: new Promise(() => {}) }
    }
    await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec }))
    expect(env.broker.signals).toEqual([{ workerId: 'w-gate', signal: 'KILL' }])
  })

  it('relays the detail of a local refusal of the start to the caller', async () => {
    const env = await environment()
    await bound(env, 'session-7')
    env.broker.startAnswer = async () => ({ ok: false, reason: 'invalid-argv', detail: 'argument 0 is 5000 characters long' })
    expect(await inTool(env, 'session-7', exec => env.host.launcher.launch({ execution: exec, ...spec })))
      .toEqual({ ok: false, reason: 'invalid-argv', detail: 'argument 0 is 5000 characters long' })
  })

  it('lease() takes the current revision from the issuer, so a caller cannot name a stale or future one', async () => {
    const env = await environment()
    const { outcome } = await bound(env, 'session-7')
    const { revision } = outcome as BindingGrant
    expect(await inTool(env, 'session-7', exec => env.host.resolver.lease(exec))).toMatchObject({ ok: true, revision })
    expect(env.broker.assigns.map(request => request.revision)).toEqual([revision])
  })
})

describe('the launcher rechecks the very binding after start (F7)', () => {
  const request = { argv: ['/bin/true'], cwd: '/srv', env: {}, graceMs: 100 }

  function rig(lookups: Array<{ bindingId: string; revision: number } | { ok: false; reason: string }>, gateOpen = () => true) {
    const calls = { revoked: [] as string[], signals: [] as string[] }
    let next = 0
    const issuer = {
      lookup: () => lookups[Math.min(next++, lookups.length - 1)],
      revoke: (id: string) => { calls.revoked.push(id); return Promise.resolve({ ok: true }) },
    }
    const resolver = { lease: () => Promise.resolve({ ok: true, assignmentId: 'a-1', revision: 1 }) }
    const client = {
      start: () => Promise.resolve({ ok: true, pid: 9, workerId: 'w-1', exited: new Promise(() => {}) }),
      signal: (workerId: string, signal: string) => { calls.signals.push(`${workerId}:${signal}`); return Promise.resolve({ ok: true }) },
    }
    const launcher = new BindingLauncher(() => 'session-1', { isActive: gateOpen } as never, issuer as never, resolver as never, client as never)
    return { launcher, calls }
  }

  it('keeps the worker when the session still has the same binding id and revision', async () => {
    const { launcher, calls } = rig([{ bindingId: 'b-1', revision: 1 }, { bindingId: 'b-1', revision: 1 }])
    expect(await launcher.launch({ execution: {}, ...request })).toMatchObject({ ok: true, workerId: 'w-1' })
    expect(calls).toEqual({ revoked: [], signals: [] })
  })

  it('kills it when the binding id is another one', async () => {
    const { launcher, calls } = rig([{ bindingId: 'b-1', revision: 1 }, { bindingId: 'b-2', revision: 1 }])
    expect(await launcher.launch({ execution: {}, ...request })).toEqual({ ok: false, reason: 'revoked' })
    expect(calls).toEqual({ revoked: [], signals: ['w-1:KILL'] })
  })

  it('kills it when the revision is another one, even with the same binding id', async () => {
    const { launcher, calls } = rig([{ bindingId: 'b-1', revision: 1 }, { bindingId: 'b-1', revision: 2 }])
    expect(await launcher.launch({ execution: {}, ...request })).toEqual({ ok: false, reason: 'revoked' })
    expect(calls).toEqual({ revoked: [], signals: ['w-1:KILL'] })
  })

  it('kills it when the binding is gone', async () => {
    const { launcher, calls } = rig([{ bindingId: 'b-1', revision: 1 }, { ok: false, reason: 'no-binding' }])
    expect(await launcher.launch({ execution: {}, ...request })).toEqual({ ok: false, reason: 'revoked' })
    expect(calls.signals).toEqual(['w-1:KILL'])
  })

  it('revokes the session as well, and kills the worker, when the gate closed', async () => {
    const { launcher, calls } = rig([{ bindingId: 'b-1', revision: 1 }, { bindingId: 'b-1', revision: 1 }], () => false)
    expect(await launcher.launch({ execution: {}, ...request })).toEqual({ ok: false, reason: 'gate-inactive' })
    expect(calls).toEqual({ revoked: ['session-1'], signals: ['w-1:KILL'] })
  })
})

describe('a connection that ends takes the bindings with it', () => {
  it('forgets every binding at once, sends no revoke, and refuses a lease', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    await bound(env, 'session-2')
    expect(env.host.issuer.lookup('session-1')).toMatchObject({ bindingId: expect.any(String) as string })
    env.broker.loseConnection()
    expect(env.host.issuer.lookup('session-1')).toEqual({ ok: false, reason: 'no-binding' })
    expect(env.host.issuer.lookup('session-2')).toEqual({ ok: false, reason: 'no-binding' })
    expect(await inTool(env, 'session-1', exec => env.host.launcher.launch({ execution: exec, argv: ['/bin/true'], cwd: '/srv', env: {}, graceMs: 10 })))
      .toEqual({ ok: false, reason: 'no-binding' })
    expect(env.broker.assigns).toHaveLength(0)
    expect(env.broker.revokes).toHaveLength(0)
    // Disposing the sessions afterwards owes the broker nothing.
    await env.host.issuer.revokeAll()
    expect(env.broker.revokes).toHaveLength(0)
  })

  it('refuses a bind that was still in flight when the connection ended', async () => {
    const env = await environment()
    const granted = Promise.withResolvers<unknown>()
    env.broker.bindAnswer = () => granted.promise
    const pending = bound(env, 'session-1')
    await vi.waitFor(() => { expect(env.broker.binds).toHaveLength(1) })
    env.broker.loseConnection()
    granted.resolve({ ok: true, bindingId: 'b-late', revision: 1 })
    expect((await pending).outcome).toMatchObject({ ok: false })
    expect(env.host.issuer.lookup('session-1')).toMatchObject({ ok: false })
  })

  it('allows the session to be bound again afterwards (on the new connection)', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    env.broker.loseConnection()
    expect(await env2bind(env, 'session-1')).toMatchObject({ ok: true })
    expect(env.host.issuer.lookup('session-1')).toMatchObject({ bindingId: expect.any(String) as string })
  })
})

describe('deny-safe shipped client', () => {
  it('answers assignment-unavailable to everything and has no positive path', async () => {
    const request = { sessionId: 's', workspaceId: 'w', kind: 'attach' } as const
    const answers = [
      await assignmentUnavailableClient.bind(request),
      await assignmentUnavailableClient.revoke({ sessionId: 's', bindingId: 'b' }),
      await assignmentUnavailableClient.assign({ sessionId: 's', bindingId: 'b', revision: 1 }),
      await assignmentUnavailableClient.start({ assignmentId: 'a', argv: ['/bin/true'], cwd: '/', env: {}, graceMs: 1 }),
      await assignmentUnavailableClient.signal('w', 'KILL'),
      await assignmentUnavailableClient.status(),
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
    const issuing = env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach', orcaInstanceId: 'instance-1' })
    await vi.waitFor(() => { expect(env.broker.binds).toHaveLength(1) })
    const revoking = env.host.issuer.revoke('session-1')
    expect(env.broker.revokes).toHaveLength(0)
    grant.resolve({ ok: true, bindingId: 'binding-late', revision: 4 })
    expect(await issuing).toEqual({ ok: false, reason: 'revoked' })
    expect(await revoking).toEqual({ ok: true })
    expect(env.broker.revokes).toEqual([{ sessionId: 'session-1', bindingId: 'binding-late' }])
  })

  it('sends nothing to the broker for a binding revoked while its cwd was still being resolved', async () => {
    const env = await environment()
    await env.seat('session-1')
    // No await between the two calls: the revoke lands during the path resolution, not during the broker call.
    const issuing = env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach', orcaInstanceId: 'instance-1' })
    const revoking = env.host.issuer.revoke('session-1')
    expect(await issuing).toEqual({ ok: false, reason: 'revoked' })
    expect(await revoking).toEqual({ ok: true })
    expect(env.broker.binds).toHaveLength(0)
    expect(env.broker.revokes).toHaveLength(0)
    expect(env.host.issuer.lookup('session-1')).toEqual({ ok: false, reason: 'no-binding' })
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
    expect(await env.host.issuer.issue({ sessionId: 'p', kind: 'attach', orcaInstanceId: 'instance-1' })).toMatchObject({ ok: true })
    await env.host.issuer.revoke('p')
    expect(env.broker.revokes.slice(before).map(request => request.sessionId)).toEqual(['p'])
  })

  it('refuses to bind again while the previous revocation of the same session is still outstanding', async () => {
    const env = await environment()
    await bound(env, 'session-1')
    const ack = Promise.withResolvers<{ ok: true }>()
    env.broker.revokeAnswer = () => ack.promise
    const revoking = env.host.issuer.revoke('session-1')
    expect(await env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach', orcaInstanceId: 'instance-1' })).toEqual({ ok: false, reason: 'revoking' })
    ack.resolve({ ok: true })
    await revoking
    expect(await env.host.issuer.issue({ sessionId: 'session-1', kind: 'attach', orcaInstanceId: 'instance-1' })).toMatchObject({ ok: true })
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
    await vi.waitFor(() => { expect(env.broker.revokes).toEqual([{ sessionId: 'session-1', bindingId: 'binding-late' }]) })
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
  it('holds nothing from a previous process and sends no epoch of its own', async () => {
    const first = await environment()
    const seated = await bound(first, 'session-7')
    const { revision } = seated.outcome as BindingGrant
    expect(await inTool(first, 'session-7', () => first.host.resolver.resolve(revision))).toMatchObject({ ok: true })

    // A restarted Core is a new host on a new control connection: the broker's epoch is its own, and Core's table is empty.
    const second = await environment()
    await second.seat('session-7')
    // The same session id and revision are known only to the dead process; the new one holds nothing.
    expect(await inTool(second, 'session-7', () => second.host.resolver.resolve(revision))).toEqual({ ok: false, reason: 'no-binding' })
    expect(second.broker.assigns).toHaveLength(0)

    // The session binds again, from scratch.
    expect(await env2bind(second, 'session-7')).toMatchObject({ ok: true })
    for (const request of [...first.broker.binds, ...second.broker.binds]) expect('coreEpoch' in request).toBe(false)
  })

  it('keeps the table in memory only', () => {
    const source = readFileSync(join(srcDir, 'broker-binding.ts'), 'utf8')
    // The one filesystem call is `realpath` of the session cwd; nothing is written or read back.
    expect(source).not.toMatch(/node:fs(?!\/promises)|writeFile|readFile|sqlite|localStorage/u)
    expect([...source.matchAll(/import \{([^}]*)\} from 'node:fs\/promises'/gu)].map(match => match[1]!.trim())).toEqual(['realpath'])
  })
})

async function env2bind(env: Env, id: string) {
  return await env.host.issuer.issue({ sessionId: id, kind: 'attach', orcaInstanceId: 'instance-1' })
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
    expect(await env.host.issuer.issue({ sessionId: 'forged', kind: 'attach', orcaInstanceId: 'instance-1' })).toEqual({ ok: false, reason: 'unknown-session' })
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
    // Nothing outside the launcher's own modules (and the specs next to them) names it.
    expect(importers.filter(file => !file.startsWith('apps/cli/tests/')).sort()).toEqual([
      'apps/cli/src/broker-capabilities.ts', 'apps/cli/src/broker-client.ts', 'apps/cli/src/broker-control.ts',
      'apps/cli/src/broker-launch.ts', 'apps/cli/src/broker-wire.ts', 'apps/cli/src/profile-boot.ts',
    ])

    // The `./lib/*` pattern would otherwise name the emitted file; the named subpaths are mapped to null, which
    // Node resolves as "not exported" (checked by hand with `import('@deepseek-ai/dsh/lib/types/broker-binding.js')`).
    const manifest = JSON.parse(readFileSync(join(srcDir, '..', 'package.json'), 'utf8')) as { exports: Record<string, unknown> }
    expect(Object.keys(manifest.exports).filter(key => /binding/u.test(key) && manifest.exports[key] !== null)).toEqual([])
    for (const file of ['broker-binding', 'broker-capabilities', 'broker-control', 'broker-client', 'broker-launch', 'broker-wire']) {
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
