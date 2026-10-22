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
 * A third slot, the assignment resolver the local subprocess runtime reads, is claimed the same way (a locked
 * symbol property on the root, `installAssignmentResolver`); it used to be a Context name any plugin could provide.
 *
 * All are deny-only in effect: the shipped broker client refuses every bind, and the resolver answers nobody.
 * In-process isolation between trusted Host plugins is still not a goal (see `broker-binding.ts`): the closed
 * `builtins.include` override and the pinned ROOM module are speed bumps, and the broker verifying instance, path
 * and `(dev, ino)` itself is the boundary. The boot-deny gate is what keeps model-defined code out.
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

/** Same registry symbol `@deepseek-ai/dsh-subprocess-local` reads (`BROKER_ASSIGNMENT_RESOLVER_KEY`). */
export const ASSIGNMENT_RESOLVER_KEY: unique symbol = Symbol.for('dsh.subprocess-local.broker-assignment-resolver')

/**
 * Claim the slot from which the local subprocess runtime reads the assignment resolver, on the root context, once,
 * before the tree mounts. The resolver there is the launcher's alone: no Context name for a plugin to provide, and
 * a property no later code can replace. It is installed even though nothing can be granted yet, because a slot
 * left empty is a slot the first plugin to define the symbol would own; the runtime also refuses a slot that is
 * not locked. The shipped resolver answers nobody, so a launch in broker mode is denied until a later slice
 * replaces it here. A second call throws.
 */
export function installAssignmentResolver(ctx: Context): void {
  const root = ctx.root as unknown as object
  if (Object.hasOwn(root, ASSIGNMENT_RESOLVER_KEY)) throw new Error('dsh: broker assignment resolver is already installed')
  const resolver = Object.freeze({ resolve: (_sessionId: string): string | undefined => undefined })
  Object.defineProperty(root, ASSIGNMENT_RESOLVER_KEY, { value: resolver, writable: false, configurable: false, enumerable: false })
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

/** Resolve a module specifier to the file URL the Loader would import it from, or `undefined` when it cannot. */
export type ModuleResolver = (specifier: string, parentURL: string) => string | undefined

interface InternalResolver {
  version: string
  resolveSync: (...args: never[]) => { url: string }
}

/** The Loader's own resolution, through the same Node internal module loader it imports with. */
function internalResolver(ctx: Context): ModuleResolver {
  return (specifier, parentURL) => {
    const internal = (ctx as unknown as { loader: { internal?: InternalResolver } }).loader.internal
    if (internal === undefined) return undefined
    try {
      const resolve = internal.resolveSync as (...args: unknown[]) => { url: string }
      const resolved = internal.version === 'v2'
        ? resolve.call(internal, parentURL, { specifier, attributes: {} })
        : resolve.call(internal, specifier, parentURL, {})
      return typeof resolved.url === 'string' ? resolved.url : undefined
    } catch {
      return undefined
    }
  }
}

export interface CapabilityPortOptions {
  /** Defaults to the Loader's own internal resolution. A resolver that finds nothing leaves the module without a port. */
  readonly resolveModule?: ModuleResolver
  /** The base bare specifiers resolve from; defaults to `ctx.baseUrl`, which is what the Loader imports from. */
  readonly parentURL?: string
}

/** What the launcher keeps after {@link installCapabilityPorts}. */
export interface CapabilityPorts {
  /**
   * Record the file URL each ported specifier resolves to now. Call it once, after the package resolution the
   * Loader imports through is in place and before any configuration row runs. Until then nothing gets a port; a
   * second call keeps the first pin. A specifier that does not resolve is left unpinned and so never gets one.
   */
  pin(): void
  /** Close the `builtins.include` override for good. Idempotent; the first write by boot closes it as well. */
  seal(): void
}

/**
 * Hand a port to the module the Loader imports under an exact specifier. The root Include (and so every nested
 * Include and group row) is the Loader's `builtins.include`; its class is replaced, so each import passes through
 * here. The module's `apply(ctx, config)` is called as `apply(ctx, config, port)`. A module that has no `apply`
 * method, or any other specifier, is returned untouched.
 *
 * Two things keep a plugin from simply taking the port, and neither is a boundary. (1) The override is closed:
 * `loader.builtins` is replaced by a non-writable property, and `builtins.include` accepts exactly one write, the
 * root Include that boot assigns right after `prepare` and before any row has run; after that, or after
 * {@link CapabilityPorts.seal}, assigning it throws and it cannot be redefined or deleted. (2) The module's
 * identity is pinned: {@link CapabilityPorts.pin} records the file URL each ported specifier resolves to, before the
 * tree mounts, and an import that later resolves anywhere else is rejected (the row fails to mount) instead of
 * being handed the port. Until the pin is taken, or when the URL cannot be resolved, that specifier gets no port.
 *
 * What this does not stop: Host code in this process can still read the wrapped class from `builtins.include`
 * and subclass it, import the launcher's source, or replace a file under the pinned URL before the pin is read or
 * after it is checked. It slows an accident or a lazy plugin; the broker verifying instance, path and `(dev, ino)`
 * itself is the boundary.
 * @param ctx The root context with the Loader mounted.
 * @param ports Specifier to the closed port for that module.
 * @param options Resolution overrides, for tests and for a base other than `ctx.baseUrl`.
 */
export function installCapabilityPorts(
  ctx: Context,
  ports: Readonly<Record<string, object>>,
  options: CapabilityPortOptions = {},
): CapabilityPorts {
  const loader = (ctx as unknown as { loader: { builtins: Record<string, unknown> } }).loader
  const builtins = loader.builtins
  const wrappers = new WeakMap<object, object>()
  const portFor = (name: string): object | undefined => Object.hasOwn(ports, name) ? ports[name] : undefined
  const resolveModule = options.resolveModule ?? internalResolver(ctx)
  const baseOf = (included?: { ctx?: { baseUrl?: string } }): string => options.parentURL ?? included?.ctx?.baseUrl ?? ctx.baseUrl ?? ''

  const pinned = new Map<string, string>()
  let pinDone = false

  const wrap = (Base: IncludeClass): IncludeClass => {
    if (WRAPPED.has(Base)) return Base
    const Ported = class extends Base {
      override import(name: string, getOuterStack?: () => string[]): unknown {
        const port = portFor(name)
        const expected = pinned.get(name)
        // No port for another specifier, and none for one whose identity could not be pinned.
        if (port === undefined || expected === undefined) return super.import(name, getOuterStack)
        const actual = resolveModule(name, baseOf(this as { ctx?: { baseUrl?: string } }))
        if (actual !== expected) {
          return Promise.reject(new Error(`dsh: ${name} now resolves to ${actual ?? 'nothing'}, not the file pinned at boot (${expected}); refusing to import it`))
        }
        const loaded = super.import(name, getOuterStack)
        return Promise.resolve(loaded).then(namespace => withPort(namespace, port, wrappers))
      }
    }
    WRAPPED.add(Ported)
    return Ported
  }

  // `loader.builtins` itself must not be swappable for a dictionary this accessor is not on.
  Object.defineProperty(loader, 'builtins', { value: builtins, writable: false, configurable: false, enumerable: true })

  let current = builtins['include'] as IncludeClass | undefined
  if (current !== undefined) current = wrap(current)
  // An include that is already there leaves nothing for boot to assign.
  let sealed = current !== undefined
  Object.defineProperty(builtins, 'include', {
    configurable: false,
    enumerable: true,
    get: () => current,
    set: (value: IncludeClass | undefined) => {
      if (sealed) throw new TypeError('dsh: builtins.include is sealed in broker mode and cannot be reassigned')
      // The single write boot makes (`mountRootInclude`), before any configuration row has run.
      sealed = true
      current = value === undefined ? value : wrap(value)
    },
  })
  return Object.freeze({
    pin: () => {
      if (pinDone) return
      pinDone = true
      for (const name of Object.keys(ports)) {
        const url = resolveModule(name, baseOf())
        if (url !== undefined) pinned.set(name, url)
      }
    },
    seal: () => { sealed = true },
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
