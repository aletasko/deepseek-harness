/**
 * The narrow, launcher-written hook through which a host that gates Core (the broker-required launcher) learns of
 * two lifecycle edges of an agent: its teardown is about to begin, and a child agent has just been published
 * under a live parent.
 *
 * It is stored on the ROOT context under a symbol key, not provided as a named service, so no plugin can shadow it
 * with `ctx.provide` or replace it: the launcher defines it once, non-writable and non-configurable, before the
 * tree mounts. The hook can only revoke, or ask the launcher's issuer to bind a child that the issuer then
 * verifies itself; it is not an issuer and cannot create any other kind of binding. Anything running in this
 * process can still read the property, which is why the issuer, not this reader, decides what a call may do.
 * @module @deepseek-ai/dsh-agent-loop/lifecycle-hook
 */
import type { Context } from '@deepseek-ai/cordis'

/** Registry-wide symbol, so the launcher and this package agree on the key even if bundled as separate copies. */
export const SESSION_LIFECYCLE_HOOK: unique symbol = Symbol.for('dsh.agent-loop.session-lifecycle-hook')

/** A child agent that has just been published, with the live parent agent its creator named. */
export interface PublishedChild {
  readonly sessionId: string
  readonly parentSessionId: string
}

export interface SessionLifecycleHook {
  /**
   * Awaited as the very first step of the teardown of an agent that entered the session registry, before the loop
   * is cancelled, so that nothing keeps running or stays resolvable under a binding the host has not yet revoked.
   * Resolves when acknowledged. Not called for an instance that never entered (a create that lost a same-id race
   * or failed in setup): it owns no binding, and the id may belong to a live session of another instance.
   */
  revoke(sessionId: string): Promise<unknown>
  /** Called synchronously after a child enters the registries; the result is not awaited by publication. */
  childPublished(child: PublishedChild): Promise<unknown>
}

/** @returns the hook the launcher defined on the root context, or `undefined` when none (the normal case). */
export function readSessionLifecycleHook(ctx: Context): SessionLifecycleHook | undefined {
  const hook = (ctx.root as unknown as Record<symbol, unknown>)[SESSION_LIFECYCLE_HOOK]
  if (hook === null || typeof hook !== 'object') return undefined
  const { revoke, childPublished } = hook as Partial<SessionLifecycleHook>
  return typeof revoke === 'function' && typeof childPublished === 'function' ? hook as SessionLifecycleHook : undefined
}
