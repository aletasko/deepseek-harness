/**
 * Host-only binding issuer and assignment resolver for a `dsh` started under the broker-required gate.
 *
 * Core states only the facts it created itself: which session exists, the workspace id minted here, its own
 * process epoch, the binding kind and parent, and the `realpath` of the session's header `cwd` as Core read it when
 * the binding was requested. It does not assert what only Orca or the broker can prove (worktree path of an
 * instance, `dev`/`ino`, execution host), and it never mints an `assignmentId`; the broker owns the registry, the
 * monotonic revision and every assignment. This module sends requests and relays the broker's answer.
 *
 * Trust rests on the boot-deny gate in `broker-required.ts`: an in-process issuer is only meaningful while no
 * model-defined Host code can run in this process. {@link BrokerGate} therefore re-checks the gate at every
 * issuance and resolution, and latches off for good the first time it fails. Revocation is deny-only and never
 * needs the gate.
 *
 * Reachability. Nothing here is provided on a Context, registered as a tool, or exposed on the event bus. The only
 * direct holder is the launcher (`profile-boot.ts`); the lifecycle hook it defines on the root, which any code in this
 * process can read, closes over `revoke` and over `issue` for kind `child` only, and the issuer checks every child
 * itself. The `./lib/*` pattern of the package `exports` can still name the
 * emitted file, so the named subpath is blocked there; that is a speed bump, not a boundary. The one event this
 * module listens to, `session/disposed`, can only revoke. In-process isolation between trusted Host plugins is not
 * a goal: a plugin that imports the launcher's source is outside this guarantee, and that is why the gate must
 * refuse model-defined code first. How the agent loop and ROOM reach this module is in `broker-capabilities.ts`.
 *
 * Every broker call has a deadline (default 5 s). A broker that does not answer in time gets its control
 * connection closed, which the broker treats as revoking everything; Core then forgets every binding it held.
 * @module @deepseek-ai/dsh/broker-binding
 */

import { randomUUID } from 'node:crypto'
import { realpath } from 'node:fs/promises'
import { isAbsolute } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-session'
import { auditBrokerTree, type BrokerConfig } from './broker-required.ts'

/** Why Core produced no binding or assignment. Broker-supplied reasons are relayed as-is. */
export type BindingRefusalReason =
  | 'gate-inactive'
  | 'invalid-request'
  | 'unknown-session'
  | 'invalid-workspace'
  | 'already-bound'
  | 'unknown-parent'
  | 'not-parent-execution'
  | 'revoking'
  | 'broker-timeout'
  | 'no-active-execution'
  | 'no-binding'
  | 'revoked'
  | 'stale-revision'
  | 'broker-malformed'
  | 'assignment-unavailable'
  | (string & {})

export interface BindingRefusal {
  readonly ok: false
  readonly reason: BindingRefusalReason
}

export type BindingKind = 'room-seat' | 'attach' | 'child'

/**
 * ROOM facts Core carries on behalf of the ROOM plugin. The key names are the design contract's own (BE03m1
 * design section 2) so the plugin and the broker read the same record. Core cannot verify them.
 */
export interface RoomFacts {
  readonly origine: 'dsh'
  readonly stanzaId: string
  readonly agente: string
  readonly installazione: string
}

/** The binding request Core sends. Every field is something Core created or observed itself. */
export interface BindingRequest {
  readonly sessionId: string
  /** Minted by the issuer (inherited from the parent for `child`); never read from a request, header or cwd. */
  readonly workspaceId: string
  /** This Core process's own epoch. A restart yields a new one. */
  readonly coreEpoch: string
  readonly kind: BindingKind
  /** Broker-assigned id of the parent binding, `child` only. */
  readonly parentBindingId?: string
  /**
   * Core's own `realpath` of the session header `cwd`, taken here at issue time. It is an assertion about what Core
   * read from the session it holds, never a value from the request, a plugin or ROOM, and it is the JSON key
   * `"cwd"` of the bind frame. Present for `room-seat` and `attach`; absent for `child`, whose root is the parent
   * binding's pinned root. The broker compares it with the path Orca attests for `orca.instanceId` and with that
   * path's `(dev, ino)`, and grants only on a match; Core asserts nothing else about the path, in particular not
   * that it is a worktree, that `instanceId` owns it, or that it still is what it was when read. This is what ties
   * an instance id that arrived through an unauthenticated channel to the session it is being bound to.
   */
  readonly cwd?: string
  /** Locator only; the broker asks Orca itself for path, `dev`, `ino` and execution host. */
  readonly orca?: { readonly instanceId: string }
  readonly room?: RoomFacts
  readonly orgId?: string
}

export interface BindingGrant {
  readonly ok: true
  /** Assigned by the broker. */
  readonly bindingId: string
  /** The broker's monotonic revision for this binding. */
  readonly revision: number
}

export interface RevokeRequest {
  readonly sessionId: string
  readonly bindingId: string
  readonly coreEpoch: string
}

export interface AssignRequest {
  /** Taken from `exec.agent.id` as attested by the ToolRuntime; nothing else identifies the caller. */
  readonly sessionId: string
  readonly bindingId: string
  readonly revision: number
  readonly coreEpoch: string
}

/** The broker's opaque answer to {@link AssignRequest}; Core relays it and never builds one. */
export interface AssignmentGrant {
  readonly ok: true
  readonly assignmentId: string
  readonly revision: number
}

/** What Core needs from the root broker. Real implementations talk to the control socket; tests simulate it. */
export interface BindingBrokerClient {
  bind(request: BindingRequest): Promise<BindingGrant | BindingRefusal>
  /** Resolves only once the broker has acknowledged the revocation. */
  revoke(request: RevokeRequest): Promise<{ readonly ok: true } | BindingRefusal>
  assign(request: AssignRequest): Promise<AssignmentGrant | BindingRefusal>
  /** Drop the control connection, which the broker treats as revoking everything. Used when a revoke is not acked. */
  closeControl(): void
}

/**
 * The broker client this slice ships: it mirrors the daemon, which answers `assignment-unavailable` to everything.
 * No positive grant path exists until the broker slices land.
 */
export const assignmentUnavailableClient: BindingBrokerClient = Object.freeze({
  bind: () => Promise.resolve(refusal('assignment-unavailable')),
  revoke: () => Promise.resolve(refusal('assignment-unavailable')),
  assign: () => Promise.resolve(refusal('assignment-unavailable')),
  closeControl: () => {},
})

function refusal(reason: BindingRefusalReason): BindingRefusal {
  return Object.freeze({ ok: false, reason })
}

/** Services whose presence means model-defined Host code may run in this process. */
const FORBIDDEN_SERVICES = ['dynamicCordisRunner', 'pluginManager'] as const

/**
 * The live gate. It is never assumed: {@link BrokerGate.arm} runs the post-mount audit itself, and
 * {@link BrokerGate.isActive} re-runs it on every call. The first failure latches the gate off for the life of
 * the process, so a forbidden row that appears and disappears cannot reopen it.
 */
export class BrokerGate {
  private armed = false
  private tripped = false

  constructor(private readonly ctx: Context, private readonly expected: BrokerConfig) {}

  /**
   * Arm the gate after the tree has mounted.
   * @throws when the audit fails; the gate stays closed.
   */
  arm(): void {
    if (this.tripped) throw new Error('dsh: broker binding gate already failed and cannot be armed')
    try {
      this.verify()
    } catch (error) {
      this.tripped = true
      throw error
    }
    this.armed = true
  }

  /** Whether issuance and resolution are allowed right now. Never throws. */
  isActive(): boolean {
    if (!this.armed || this.tripped) return false
    try {
      this.verify()
      return true
    } catch {
      this.tripped = true
      return false
    }
  }

  private verify(): void {
    auditBrokerTree(this.ctx, this.expected)
    for (const name of FORBIDDEN_SERVICES) {
      if (this.ctx.reflect.get(name, false) !== undefined) throw new Error(`${name} is present`)
    }
  }
}

/** What a caller may ask for. Only these fields are read; anything else on the object is dropped. */
export interface IssueInput {
  readonly sessionId: string
  readonly kind: BindingKind
  /** Required for `child`, forbidden otherwise. */
  readonly parentSessionId?: string
  readonly orcaInstanceId?: string
  readonly room?: RoomFacts
  readonly orgId?: string
}

export type IssueOutcome = BindingGrant | BindingRefusal

export type RevokeOutcome =
  | { readonly ok: true }
  | { readonly ok: false; readonly reason: BindingRefusalReason; readonly controlClosed: boolean }

interface Entry {
  readonly sessionId: string
  readonly workspaceId: string
  readonly parentSessionId?: string
  /** Set once the broker has granted; undefined while the bind is in flight or after a refusal. */
  grant?: BindingGrant
  revoked: boolean
  /** Settles when the in-flight bind has. */
  readonly settled: Promise<void>
  /** Started by the first revocation; later ones share it, so one binding sends one revoke frame. */
  revocation?: Promise<RevokeOutcome>
}

/** How long Core waits for the broker before it closes the control connection. */
export const DEFAULT_BROKER_TIMEOUT_MS = 5000

class BrokerSilence extends Error {}

/** Settle with `run()`, or reject with {@link BrokerSilence} once `ms` pass without an answer. */
async function withDeadline<T>(ms: number, run: () => Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      run(),
      new Promise<never>((_resolve, reject) => { timer = setTimeout(() => { reject(new BrokerSilence('broker did not answer')) }, ms) }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

interface SessionView {
  readonly header?: { readonly parentSession?: string; readonly cwd?: string }
}

/** The session header's `cwd` resolved through every symlink, or `undefined` when it is missing or unusable. */
async function assertedCwd(session: SessionView): Promise<string | undefined> {
  const declared = session.header?.cwd
  if (typeof declared !== 'string' || declared.length === 0 || declared.includes('\0') || !isAbsolute(declared)) return undefined
  try {
    const resolved = await realpath(declared)
    return isAbsolute(resolved) ? resolved : undefined
  } catch {
    return undefined
  }
}

const KINDS: ReadonlySet<string> = new Set<BindingKind>(['room-seat', 'attach', 'child'])

function nonEmpty(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}

function validRoom(room: unknown): room is RoomFacts {
  if (room === null || typeof room !== 'object') return false
  const { origine, stanzaId, agente, installazione } = room as Partial<RoomFacts>
  return origine === 'dsh' && nonEmpty(stanzaId) && nonEmpty(agente) && nonEmpty(installazione)
}

/**
 * Issues binding requests and revokes them. Bindings live in memory only: a restarted Core has a new
 * `coreEpoch` and an empty table, so nothing issued by a previous process is ever reused.
 */
export class BindingIssuer {
  private readonly records = new Map<string, Entry>()

  constructor(
    private readonly ctx: Context,
    private readonly gate: BrokerGate,
    private readonly client: BindingBrokerClient,
    readonly coreEpoch: string,
    readonly timeoutMs: number = DEFAULT_BROKER_TIMEOUT_MS,
  ) {}

  /**
   * Ask the broker to bind a live session. Revocation needs no pairing by the caller: the agent loop awaits
   * {@link BindingIssuer.revoke} first thing in an agent's teardown, and `session/disposed` is the backstop.
   *
   * `cwd` is never an input: for `room-seat` and `attach` the issuer resolves the live session's own header `cwd`
   * and sends that (see {@link BindingRequest.cwd}); a session without a usable `cwd` is refused
   * `invalid-workspace`. A field of that name on `input` is dropped like any other.
   *
   * A `child` is bound only when all of these hold: the session is live and its header names `parentSessionId` as
   * its `parentSession`; the parent holds a live granted binding here; and the call runs inside the parent's own
   * direct tool execution, as attested by the ToolRuntime. A session that merely names someone else as its parent
   * is therefore never grafted onto that session's binding.
   * @param input Session facts; see {@link IssueInput}.
   * @returns The broker's grant, or a refusal. Never throws.
   */
  async issue(input: IssueInput): Promise<IssueOutcome> {
    if (!this.gate.isActive()) return refusal('gate-inactive')
    const { sessionId, kind } = input
    if (!nonEmpty(sessionId) || !KINDS.has(kind)) return refusal('invalid-request')
    if (input.orcaInstanceId !== undefined && !nonEmpty(input.orcaInstanceId)) return refusal('invalid-request')
    if (input.orgId !== undefined && !nonEmpty(input.orgId)) return refusal('invalid-request')
    if (input.room !== undefined && !validRoom(input.room)) return refusal('invalid-request')
    if ((kind === 'child') !== (input.parentSessionId !== undefined)) return refusal('invalid-request')
    // Only a session that is live in this Core's store can be bound. A forged event cannot create one.
    const sessions = this.ctx.reflect.get('sessions') as { get(id: string): SessionView | undefined } | undefined
    const session = sessions?.get(sessionId)
    if (session === undefined) return refusal('unknown-session')
    // A record stays until its revocation has finished, so a session cannot be re-bound while the old one dies.
    const existing = this.records.get(sessionId)
    if (existing !== undefined) return refusal(existing.revoked ? 'revoking' : 'already-bound')

    let parentBindingId: string | undefined
    let workspaceId = `workspace-${randomUUID()}`
    if (input.parentSessionId !== undefined) {
      if (session.header?.parentSession !== input.parentSessionId) return refusal('unknown-parent')
      if (attestedSessionId(this.ctx) !== input.parentSessionId) return refusal('not-parent-execution')
      const parent = this.records.get(input.parentSessionId)
      if (parent === undefined || parent.revoked || parent.grant === undefined) return refusal('unknown-parent')
      parentBindingId = parent.grant.bindingId
      workspaceId = parent.workspaceId
    }

    const done = Promise.withResolvers<void>()
    const record: Entry = {
      sessionId, workspaceId, revoked: false, settled: done.promise,
      ...input.parentSessionId === undefined ? {} : { parentSessionId: input.parentSessionId },
    }
    this.records.set(sessionId, record)
    const forget = (): void => {
      record.revoked = true
      // A refused bind leaves nothing to resolve or to revoke.
      if (this.records.get(sessionId) === record) this.records.delete(sessionId)
    }
    try {
      // The record exists before the first await, so a revoke that arrives while the path is resolved, or while
      // the broker thinks, finds it. A child has no path to resolve and so no await before its record: its
      // tool-execution attestation above is read in the caller's own synchronous turn.
      let cwd: string | undefined
      if (kind !== 'child') {
        cwd = await assertedCwd(session)
        if (cwd === undefined) {
          forget()
          return refusal('invalid-workspace')
        }
        // Gone or replaced while the path was resolved, or revoked meanwhile: nothing is sent to the broker.
        if (sessions?.get(sessionId) !== session) {
          forget()
          return refusal('unknown-session')
        }
        if (record.revoked) {
          forget()
          return refusal('revoked')
        }
      }
      const request: BindingRequest = Object.freeze({
        sessionId, workspaceId, coreEpoch: this.coreEpoch, kind,
        ...parentBindingId === undefined ? {} : { parentBindingId },
        ...cwd === undefined ? {} : { cwd },
        ...input.orcaInstanceId === undefined ? {} : { orca: Object.freeze({ instanceId: input.orcaInstanceId }) },
        ...input.room === undefined ? {} : {
          room: Object.freeze({
            origine: 'dsh' as const, stanzaId: input.room.stanzaId, agente: input.room.agente,
            installazione: input.room.installazione,
          }),
        },
        ...input.orgId === undefined ? {} : { orgId: input.orgId },
      })

      const answer = await withDeadline(this.timeoutMs, () => this.client.bind(request))
      if (!isGrant(answer)) {
        forget()
        return isRefusal(answer) ? answer : refusal('broker-malformed')
      }
      // Kept even when a revoke raced the bind, so that revoke can name the binding it must kill.
      record.grant = answer
      if (record.revoked) return refusal('revoked')
      // The gate may have latched off while the broker was answering: the grant is then undone, not returned.
      if (!this.gate.isActive()) {
        void this.revokeRecord(record)
        return refusal('gate-inactive')
      }
      return answer
    } catch (error) {
      forget()
      if (error instanceof BrokerSilence) {
        this.closeControl()
        return refusal('broker-timeout')
      }
      return refusal('assignment-unavailable')
    } finally {
      done.resolve()
    }
  }

  /**
   * Revoke the session's binding and every child bound under it. Needs no gate: it only tightens. The revoke
   * frame for a granted binding is handed to the broker synchronously, before this method first yields. A
   * finished revocation removes the record, so the table does not grow with the process's lifetime.
   * @param sessionId The session whose binding to revoke.
   * @returns Whether the broker acknowledged. When it did not, the control connection has been closed.
   */
  revoke(sessionId: string): Promise<RevokeOutcome> {
    const record = this.records.get(sessionId)
    if (record === undefined) return Promise.resolve({ ok: true })
    return this.revokeRecord(record)
  }

  /** Revoke everything this issuer holds. */
  async revokeAll(): Promise<void> {
    await Promise.all([...this.records.values()].map(record => this.revokeRecord(record)))
  }

  /**
   * Resolution view for the resolver: the live binding of a session, or why there is none.
   * @internal
   */
  lookup(sessionId: string): { readonly bindingId: string; readonly revision: number } | BindingRefusal {
    const record = this.records.get(sessionId)
    if (record === undefined) return refusal('no-binding')
    if (record.revoked) return refusal('revoked')
    if (record.grant === undefined) return refusal('no-binding')
    return { bindingId: record.grant.bindingId, revision: record.grant.revision }
  }

  private revokeRecord(record: Entry): Promise<RevokeOutcome> {
    record.revoked = true
    record.revocation ??= this.startRevocation(record).then((outcome) => {
      if (this.records.get(record.sessionId) === record) this.records.delete(record.sessionId)
      return outcome
    })
    return record.revocation
  }

  private startRevocation(record: Entry): Promise<RevokeOutcome> {
    // Children first, each frame sent before this one.
    const children = [...this.records.values()].filter(other => other.parentSessionId === record.sessionId)
    const pending = children.map(child => this.revokeRecord(child))
    const own = record.grant !== undefined
      ? this.sendRevoke(record.sessionId, record.grant)
      // A bind still in flight: a grant that arrives is revoked at once; a refusal needs nothing.
      : record.settled.then(() => record.grant === undefined ? { ok: true as const } : this.sendRevoke(record.sessionId, record.grant))
    return Promise.all([own, ...pending]).then(results => results.find(result => !result.ok) ?? { ok: true as const })
  }

  private async sendRevoke(sessionId: string, grant: BindingGrant): Promise<RevokeOutcome> {
    try {
      const frame: RevokeRequest = { sessionId, bindingId: grant.bindingId, coreEpoch: this.coreEpoch }
      const answer = await withDeadline(this.timeoutMs, () => this.client.revoke(frame))
      if (answer.ok) return { ok: true }
      this.closeControl()
      return { ok: false, reason: answer.reason, controlClosed: true }
    } catch (error) {
      this.closeControl()
      return { ok: false, reason: error instanceof BrokerSilence ? 'broker-timeout' : 'assignment-unavailable', controlClosed: true }
    }
  }

  /**
   * Drop the control connection, which the broker treats as revoking everything, and forget every binding here:
   * none can be resolved again, and no further frame is sent for them. Idempotent.
   */
  closeControl(): void {
    this.client.closeControl()
    for (const record of this.records.values()) {
      record.revoked = true
      record.revocation ??= Promise.resolve({ ok: true as const })
    }
    this.records.clear()
  }
}

function isGrant(value: unknown): value is BindingGrant {
  if (value === null || typeof value !== 'object') return false
  const { ok, bindingId, revision } = value as Partial<BindingGrant>
  return ok === true && nonEmpty(bindingId) && Number.isSafeInteger(revision) && (revision as number) >= 1
}

function isRefusal(value: unknown): value is BindingRefusal {
  if (value === null || typeof value !== 'object') return false
  const { ok, reason } = value as Partial<BindingRefusal>
  return ok === false && nonEmpty(reason)
}

/** The ToolRuntime surface the resolver uses; the same structural shape `subprocess` attests against. */
interface ToolAttestor {
  currentActiveExecution(): unknown
  attestActiveExecution(execution: unknown): { readonly sessionId?: string; readonly direct?: boolean }
}

/**
 * The session id the ToolRuntime attests for a direct, live tool execution (`exec.agent.id`), or `undefined`. Only
 * the execution object is read: no path, cwd, header, alias, prefix or `DSH_SESSION_ID`. Used for the resolver and
 * for proving that a child is being created by its parent's own tool call.
 */
function attestedSessionId(ctx: Context, execution?: unknown): string | undefined {
  const tools = ctx.reflect.get('tools') as ToolAttestor | undefined
  try {
    const active = execution ?? tools?.currentActiveExecution()
    const attestation = active === undefined || tools === undefined ? undefined : tools.attestActiveExecution(active)
    return attestation?.direct === true && nonEmpty(attestation.sessionId) ? attestation.sessionId : undefined
  } catch {
    return undefined
  }
}

/**
 * Maps the active tool execution to a broker assignment. The only input taken from the call is the execution
 * object, and the only thing read from it is the session id the ToolRuntime attests from `exec.agent.id`. No
 * path, cwd, header, alias, prefix or `DSH_SESSION_ID` is consulted.
 */
export class BindingResolver {
  constructor(
    private readonly ctx: Context,
    private readonly gate: BrokerGate,
    private readonly issuer: BindingIssuer,
    private readonly client: BindingBrokerClient,
  ) {}

  /**
   * @param revision The revision the caller believes is current; a different one is refused without asking the broker.
   * @param execution The active tool execution; defaults to the ToolRuntime's current one.
   * @returns The broker's {@link AssignmentGrant} unchanged, or a refusal. Never throws.
   */
  async resolve(revision: number, execution?: unknown): Promise<AssignmentGrant | BindingRefusal> {
    if (!this.gate.isActive()) return refusal('gate-inactive')
    const sessionId = attestedSessionId(this.ctx, execution)
    if (sessionId === undefined) return refusal('no-active-execution')
    const binding = this.issuer.lookup(sessionId)
    if (!('bindingId' in binding)) return binding
    if (binding.revision !== revision) return refusal('stale-revision')
    let answer: unknown
    try {
      const frame: AssignRequest = { sessionId, bindingId: binding.bindingId, revision, coreEpoch: this.issuer.coreEpoch }
      answer = await withDeadline(this.issuer.timeoutMs, () => this.client.assign(frame))
    } catch (error) {
      if (error instanceof BrokerSilence) {
        this.issuer.closeControl()
        return refusal('broker-timeout')
      }
      return refusal('assignment-unavailable')
    }
    // The binding may have been revoked, or the gate latched off, while the broker was thinking.
    if (!('bindingId' in this.issuer.lookup(sessionId))) return refusal('revoked')
    if (!this.gate.isActive()) return refusal('gate-inactive')
    if (isRefusal(answer)) return answer
    if (!isAssignment(answer) || answer.revision !== revision) return refusal('broker-malformed')
    return answer
  }
}

function isAssignment(value: unknown): value is AssignmentGrant {
  if (value === null || typeof value !== 'object') return false
  const { ok, assignmentId, revision } = value as Partial<AssignmentGrant>
  return ok === true && nonEmpty(assignmentId) && Number.isSafeInteger(revision)
}

/** The launcher's handle. Not exported through the package `exports`, never provided on a Context. */
export interface BindingHost {
  readonly gate: BrokerGate
  readonly issuer: BindingIssuer
  readonly resolver: BindingResolver
}

/**
 * Build the host-only machinery for a broker-required launch. Call before the tree mounts, then
 * {@link BrokerGate.arm} after the post-mount audit; until then everything but revocation refuses.
 * @param options The root Context, the trusted broker config provided to it, and the broker client.
 * @returns The handle for the launcher alone.
 */
export function createBindingHost(options: {
  ctx: Context
  broker: BrokerConfig
  client: BindingBrokerClient
  coreEpoch?: string
  timeoutMs?: number
}): BindingHost {
  const { ctx, broker, client } = options
  const gate = new BrokerGate(ctx, broker)
  const issuer = new BindingIssuer(ctx, gate, client, options.coreEpoch ?? randomUUID(), options.timeoutMs)
  const resolver = new BindingResolver(ctx, gate, issuer, client)
  // The only event this module listens to. A forged `session/disposed` can only revoke, never issue.
  ctx.on('session/disposed', (session) => { void issuer.revoke(session.id) })
  ctx.effect(() => () => void issuer.revokeAll(), 'binding-issuer teardown')
  return Object.freeze({ gate, issuer, resolver })
}
