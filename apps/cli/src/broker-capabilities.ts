/**
 * How the broker-required launcher hands its two narrow capabilities to the rest of the process, without any
 * name on a Context, any event, `ctx.serial`, configuration, or API/Remote surface:
 *
 * 1. A lifecycle hook for the agent loop, defined on the ROOT context under a symbol key, once, before the tree
 *    mounts. The loop awaits `revoke` first thing in an agent's teardown and announces a child published under a
 *    live parent. It cannot issue anything else, and the issuer verifies every child itself.
 * 2. A closed port passed to ROOM as the THIRD argument of its `apply(ctx, config, port)`. The launcher wraps the
 *    ROOM module as the Loader imports it, by exact package specifier, so no row, config value or event can ask
 *    for the port. A row under another name, a relative path or an agent-preset subtree gets none.
 *
 * Both are deny-only in effect: the shipped broker client refuses every bind, and the resolver relays refusals.
 * In-process isolation between trusted Host plugins is still not a goal (see `broker-binding.ts`); the boot-deny
 * gate is what keeps model-defined code out.
 * @module @deepseek-ai/dsh/broker-capabilities
 */

import { realpath } from 'node:fs/promises'
import { isAbsolute } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-session'
import type { BindingHost, BindingRefusal, IssueOutcome, RevokeOutcome } from './broker-binding.ts'

/** Same registry symbol the agent loop reads (`SESSION_LIFECYCLE_HOOK` in `@deepseek-ai/dsh-agent-loop`). */
export const LIFECYCLE_HOOK_KEY: unique symbol = Symbol.for('dsh.agent-loop.session-lifecycle-hook')

/** The package specifier whose module receives the room-seat port. Exact match only. */
export const ROOM_PLUGIN_SPECIFIER = '@myh/dsh-client-ui-rooms'

/** How recent a session must be for ROOM to seat it. A chat the room created just now, not an old session. */
export const ROOM_SEAT_FRESH_MS = 120_000

/**
 * Define the agent loop's hook on the root context. Non-writable and non-configurable, so nothing can replace it
 * afterwards, and a second call throws. Call before the tree mounts.
 */
export function installLifecycleHook(ctx: Context, host: BindingHost): void {
  const root = ctx.root as unknown as object
  if (Object.hasOwn(root, LIFECYCLE_HOOK_KEY)) throw new Error('dsh: session lifecycle hook is already installed')
  const hook = Object.freeze({
    revoke: async (sessionId: string): Promise<unknown> => await host.issuer.revoke(sessionId),
    childPublished: async (child: { sessionId: string; parentSessionId: string }): Promise<unknown> => {
      const { sessionId, parentSessionId } = child
      if (typeof sessionId !== 'string' || typeof parentSessionId !== 'string') return refused('invalid-request')
      return await host.issuer.issue({ sessionId, kind: 'child', parentSessionId })
    },
  })
  Object.defineProperty(root, LIFECYCLE_HOOK_KEY, { value: hook, writable: false, configurable: false, enumerable: false })
}

function refused(reason: string): BindingRefusal {
  return Object.freeze({ ok: false, reason })
}

/** What ROOM may ask for. The kind is fixed to `room-seat` and there is no way to name a parent or an org. */
export interface RoomSeatRequest {
  readonly sessionId: string
  /** Orca's stable instance id for the worktree, a locator only; the broker asks Orca itself. */
  readonly instanceId: string
  readonly room: { readonly stanzaId: string; readonly agente: string; readonly installazione: string }
}

/** The closed capability ROOM receives. Nothing else about the issuer is reachable through it. */
export interface RoomSeatPort {
  /** Ask for a `room-seat` binding of a session ROOM just created. Never throws. */
  seat(request: RoomSeatRequest): Promise<IssueOutcome>
  /** Revoke a binding this port seated. Refuses any other session. Never throws. */
  revoke(sessionId: string): Promise<RevokeOutcome>
}

interface SessionView {
  readonly header?: { readonly createdAt?: number; readonly cwd?: string }
}

interface WorkspaceView {
  readonly path?: string
  readonly sessionIds?: readonly string[]
}

/**
 * The room-seat port. Before it asks the issuer it checks, from Core's own registries, that the session is live,
 * is recent, and sits in a workspace whose root is exactly the session's `cwd`; and it never forwards an org,
 * a parent or a kind, so ROOM's `ctx.serial`-derived facts cannot become claims here. Whether the instance id
 * is the right worktree is not decidable in Core: the broker verifies it against Orca itself.
 */
export function createRoomSeatPort(
  ctx: Context,
  host: BindingHost,
  options: { freshMs?: number; now?: () => number } = {},
): RoomSeatPort {
  const freshMs = options.freshMs ?? ROOM_SEAT_FRESH_MS
  const now = options.now ?? Date.now
  const seated = new Set<string>()
  return Object.freeze({
    async seat(request: RoomSeatRequest): Promise<IssueOutcome> {
      try {
        const { sessionId, instanceId } = request
        // ROOM is trusted code, but the port is the boundary: take nothing on faith about the shape.
        const room: unknown = request.room
        const facts = (typeof room === 'object' && room !== null ? room : {}) as Partial<RoomSeatRequest['room']>
        if (!text(sessionId) || !text(instanceId) || !text(facts.stanzaId) || !text(facts.agente) || !text(facts.installazione)) {
          return refused('invalid-request')
        }
        const session = (ctx.reflect.get('sessions') as { get(id: string): SessionView | undefined } | undefined)?.get(sessionId)
        if (session === undefined) return refused('unknown-session')
        const { createdAt, cwd } = session.header ?? {}
        if (typeof createdAt !== 'number' || now() - createdAt > freshMs || createdAt - now() > freshMs) return refused('stale-session')
        if (typeof cwd !== 'string' || !isAbsolute(cwd)) return refused('invalid-workspace')
        if (!await workspaceRootIs(ctx, sessionId, cwd)) return refused('invalid-workspace')
        const outcome = await host.issuer.issue({
          sessionId, kind: 'room-seat', orcaInstanceId: instanceId,
          room: { origine: 'dsh', stanzaId: facts.stanzaId, agente: facts.agente, installazione: facts.installazione },
        })
        if (outcome.ok) seated.add(sessionId)
        return outcome
      } catch {
        return refused('assignment-unavailable')
      }
    },
    async revoke(sessionId: string): Promise<RevokeOutcome> {
      if (!seated.delete(sessionId)) return { ok: true }
      return await host.issuer.revoke(sessionId)
    },
  })
}

function text(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 300
}

async function workspaceRootIs(ctx: Context, sessionId: string, cwd: string): Promise<boolean> {
  const registry = ctx.reflect.get('workspaceRegistry') as { list?(): readonly WorkspaceView[] } | undefined
  if (typeof registry?.list !== 'function') return false
  let root: string
  try { root = await realpath(cwd) } catch { return false }
  return registry.list().some(workspace => workspace.path === root && workspace.sessionIds?.includes(sessionId) === true)
}

type IncludeClass = new (...args: never[]) => { import(name: string, getOuterStack?: () => string[]): unknown }
const WRAPPED = new WeakSet<object>()

/**
 * Hand a port to the module the Loader imports under an exact specifier. The root Include (and so every nested
 * Include and group row) is the Loader's `builtins.include`; its class is replaced, once, so each import passes
 * through here. The module's `apply(ctx, config)` is called as `apply(ctx, config, port)`. A module that has no
 * `apply` method, or any other specifier, is returned untouched.
 * @param ctx The root context with the Loader mounted.
 * @param ports Specifier to the closed port for that module.
 */
export function installCapabilityPorts(ctx: Context, ports: Readonly<Record<string, object>>): void {
  const builtins = (ctx as unknown as { loader: { builtins: Record<string, unknown> } }).loader.builtins
  const wrappers = new WeakMap<object, object>()
  const portFor = (name: string): object | undefined => Object.hasOwn(ports, name) ? ports[name] : undefined

  const wrap = (Base: IncludeClass): IncludeClass => {
    if (WRAPPED.has(Base)) return Base
    const Ported = class extends Base {
      override import(name: string, getOuterStack?: () => string[]): unknown {
        const loaded = super.import(name, getOuterStack)
        const port = portFor(name)
        if (port === undefined) return loaded
        return Promise.resolve(loaded).then(namespace => withPort(namespace, port, wrappers))
      }
    }
    WRAPPED.add(Ported)
    return Ported
  }

  let current = builtins['include'] as IncludeClass | undefined
  if (current !== undefined) current = wrap(current)
  Object.defineProperty(builtins, 'include', {
    configurable: false,
    enumerable: true,
    get: () => current,
    set: (value: IncludeClass | undefined) => { current = value === undefined ? value : wrap(value) },
  })
}

/** The Loader unwraps `default ?? namespace`; do the same and return a plugin object whose `apply` carries the port. */
function withPort(namespace: unknown, port: object, cache: WeakMap<object, object>): unknown {
  if (namespace === null || typeof namespace !== 'object') return namespace
  const cached = cache.get(namespace)
  if (cached !== undefined) return cached
  const exports: unknown = (namespace as { default?: unknown }).default ?? namespace
  if (exports === null || typeof exports !== 'object' || typeof (exports as { apply?: unknown }).apply !== 'function') return namespace
  const plugin = exports as { apply(ctx: unknown, config: unknown, ...rest: unknown[]): unknown }
  const wrapped = { ...plugin, apply: (ctx: unknown, config: unknown) => plugin.apply(ctx, config, port) }
  cache.set(namespace, wrapped)
  return wrapped
}
