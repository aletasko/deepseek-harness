/**
 * The real control client: ONE connection to the root broker's Unix socket per Core owner (S6 contract 1).
 *
 * The first frame is `register-core`; the integer in `registered` is the broker's epoch for this connection and the
 * only epoch there is. Every request is a frame with a UUIDv4 `requestId` and is answered by exactly one frame
 * carrying the same id. Frames without a `requestId` are the broker's own: `worker-exited` (the end of a worker that
 * `start` launched on this connection) and connection-level errors.
 *
 * Fail closed. Anything outside the contract closes the connection (the broker then revokes everything and kills
 * every worker): an unparseable line, an unknown frame type or field, an answer that matches no request or answers
 * the wrong kind of request, an error without a request id, a silent broker. The client never reconnects, because a
 * new connection is a new epoch with no bindings: a lost connection ends this client for good, every later call is
 * refused, and the issuer is told by the refusals. Positive answers are translated into Core's grant types here;
 * `error` frames become refusals carrying the broker's reason.
 *
 * A connection that ends on its own tells its listeners ({@link BrokerControlClient.onClosed}); the binding issuer
 * forgets every binding at that moment, because the broker revoked them all. {@link ReconnectingBrokerClient} may then
 * open a fresh connection a bounded number of times, re-registering from scratch (a new epoch, no bindings). It does
 * so only after a transport loss, never after a protocol violation, a timeout or a close Core asked for.
 *
 * The socket is not taken on trust: {@link socketDistrust} checks the path before every connect (see there).
 *
 * Which client Core runs is decided by {@link createBrokerClient}: the real one only when a broker socket is
 * configured, the deny client otherwise.
 * @module @deepseek-ai/dsh/broker-client
 */

import { randomUUID } from 'node:crypto'
import { lstatSync, realpathSync, type Stats } from 'node:fs'
import { createConnection, type Socket } from 'node:net'
import { basename, dirname, isAbsolute, join } from 'node:path'
import {
  assignmentUnavailableClient, type AssignmentGrant, type AssignRequest, type BindingBrokerClient,
  type BindingGrant, type BindingRefusal, type BindingRefusalReason, type BindingRequest, type BrokerStatus,
  type RevokeRequest, type StartGrant, type StartRequest, type WorkerExit, type WorkerSignalName,
} from './broker-binding.ts'
import type { BrokerConfig } from './broker-required.ts'
import {
  bindFrame, leaseFrame, parseInbound, refusalOf, registerFrame, revokeFrame, signalFrame, startFrame,
  startRequestProblem, statusFrame, toAssignmentGrant, toBindingGrant, toStatus, WireViolation, type Inbound,
} from './broker-wire.ts'

export interface BrokerClientOptions {
  /** bind, lease, revoke and status. The issuer has its own 5 s deadline on top; this one closes the connection. */
  readonly requestTimeoutMs?: number
  /** `start` runs Orca and the worker launch inside the broker and can take longer than the others. */
  readonly startTimeoutMs?: number
  /** From connecting to `registered`. */
  readonly registerTimeoutMs?: number
  /** Test seam. */
  readonly connect?: (socketPath: string) => Socket
}

export interface ReconnectOptions {
  /** Attempts after one loss. 0 turns reconnecting off. */
  readonly attempts?: number
  /** Delay before the first attempt; each next one doubles it. */
  readonly baseDelayMs?: number
  /** Attempts over the life of the client, so a broker that registers and drops again cannot keep it busy forever. */
  readonly lifetimeAttempts?: number
}

export const DEFAULT_RECONNECT_ATTEMPTS = 3
export const DEFAULT_RECONNECT_DELAY_MS = 200
export const DEFAULT_LIFETIME_RECONNECTS = 12

export const DEFAULT_REQUEST_TIMEOUT_MS = 5000
export const DEFAULT_START_TIMEOUT_MS = 30_000
export const DEFAULT_REGISTER_TIMEOUT_MS = 5000

/** The broker never sends more than 4 KiB per line; this bound keeps a runaway peer from filling memory. */
const MAX_INBOUND_LINE = 64 * 1024
/** `bind`, `lease`, `revoke` and `status` frames are bounded by the broker at 8 KiB. */
const MAX_BINDING_FRAME_BYTES = 8192
/** Exits that arrived before the `started` that names them. A worker's `started` is already in flight. */
const MAX_EARLY_EXITS = 64
const MAX_LEASED = 256

interface Pending {
  readonly expect: Inbound['type']
  readonly accept: (frame: Inbound) => unknown
  readonly resolve: (value: unknown) => void
  readonly timer: ReturnType<typeof setTimeout>
}

interface Tracked {
  readonly bindingId: string | undefined
  readonly exit: PromiseWithResolvers<WorkerExit>
}

function refusal(reason: BindingRefusalReason): BindingRefusal {
  return Object.freeze({ ok: false, reason })
}

export class BrokerControlClient implements BindingBrokerClient {
  private readonly socket: Socket
  private readonly requestTimeoutMs: number
  private readonly startTimeoutMs: number
  private state: 'connecting' | 'ready' | 'closed' = 'connecting'
  private closedReason: BindingRefusalReason = 'control-closed'
  private brokerEpoch: number | undefined
  private buffer = ''
  private readonly pending = new Map<string, Pending>()
  private readonly workers = new Map<string, Tracked>()
  private readonly earlyExits = new Map<string, Extract<Inbound, { type: 'worker-exited' }>>()
  /** assignmentId to the binding it was leased from, so the worker's exit can be checked against it. */
  private readonly leased = new Map<string, string>()
  private readonly registered = Promise.withResolvers<boolean>()
  private readonly closedListeners: Array<(reason: BindingRefusalReason) => void> = []
  private readonly registerId: string
  private registerTimer: ReturnType<typeof setTimeout> | undefined

  constructor(readonly socketPath: string, options: BrokerClientOptions = {}) {
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
    this.startTimeoutMs = options.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS
    this.registerId = randomUUID()
    void this.registered.promise.catch(() => {})
    const socket = (options.connect ?? createConnection)(socketPath)
    this.socket = socket
    socket.setEncoding('utf8')
    socket.on('data', (chunk: string) => { this.read(chunk) })
    socket.on('error', () => { this.close('control-unavailable') })
    socket.once('close', () => { this.close('control-closed') })
    this.registerTimer = setTimeout(() => { this.close('broker-timeout') }, options.registerTimeoutMs ?? DEFAULT_REGISTER_TIMEOUT_MS)
    socket.once('connect', () => { this.write(registerFrame(this.registerId)) })
    // A socket handed in by a test may already be connected.
    if (!socket.connecting && !socket.destroyed) this.write(registerFrame(this.registerId))
  }

  /** The broker's epoch for this connection, once registered. */
  get epoch(): number | undefined { return this.brokerEpoch }

  get closed(): boolean { return this.state === 'closed' }

  /** Settles `true` once the broker registered this connection, `false` if it ended first. */
  whenReady(): Promise<boolean> { return this.registered.promise }

  onClosed(listener: (reason: BindingRefusalReason) => void): void {
    if (this.state === 'closed') queueMicrotask(() => { listener(this.closedReason) })
    else this.closedListeners.push(listener)
  }

  bind(request: BindingRequest): Promise<BindingGrant | BindingRefusal> {
    const id = randomUUID()
    return this.request(bindFrame(request, id), id, 'bound', this.requestTimeoutMs, frame => toBindingGrant(frame as Extract<Inbound, { type: 'bound' }>), MAX_BINDING_FRAME_BYTES)
  }

  revoke(request: RevokeRequest): Promise<{ readonly ok: true } | BindingRefusal> {
    const id = randomUUID()
    return this.request(revokeFrame(request, id), id, 'revoked', this.requestTimeoutMs, (frame) => {
      const revoked = frame as Extract<Inbound, { type: 'revoked' }>
      // An ack for another binding is not an ack for this one.
      return revoked.bindingId === request.bindingId ? Object.freeze({ ok: true as const }) : refusal('broker-malformed')
    }, MAX_BINDING_FRAME_BYTES)
  }

  assign(request: AssignRequest): Promise<AssignmentGrant | BindingRefusal> {
    const id = randomUUID()
    return this.request(leaseFrame(request, id), id, 'leased', this.requestTimeoutMs, (frame) => {
      const grant = toAssignmentGrant(frame as Extract<Inbound, { type: 'leased' }>)
      this.leased.set(grant.assignmentId, request.bindingId)
      if (this.leased.size > MAX_LEASED) this.leased.delete(this.leased.keys().next().value as string)
      return grant
    }, MAX_BINDING_FRAME_BYTES)
  }

  start(request: StartRequest): Promise<StartGrant | BindingRefusal> {
    const problem = startRequestProblem(request)
    if (problem !== undefined) return Promise.resolve(Object.freeze({ ok: false as const, reason: problem.reason, detail: problem.detail }))
    const id = randomUUID()
    const bindingId = this.leased.get(request.assignmentId)
    this.leased.delete(request.assignmentId)
    return this.request(startFrame(request, id), id, 'started', this.startTimeoutMs, (frame) => {
      const started = frame as Extract<Inbound, { type: 'started' }>
      if (this.workers.has(started.workerId)) throw new WireViolation('duplicate worker id')
      const tracked: Tracked = { bindingId, exit: Promise.withResolvers<WorkerExit>() }
      this.workers.set(started.workerId, tracked)
      const early = this.earlyExits.get(started.workerId)
      if (early !== undefined) {
        this.earlyExits.delete(started.workerId)
        this.finishWorker(started.workerId, tracked, early)
      }
      return Object.freeze({ ok: true as const, pid: started.pid, workerId: started.workerId, exited: tracked.exit.promise })
    }, 256 * 1024)
  }

  /**
   * Ask the broker to deliver TERM or KILL to a worker this connection started. The broker answers `signalled` once the
   * signal is delivered (`worker-exited` follows when the worker is gone), or `not-found` for a worker it does not
   * know, which includes one that already exited.
   */
  signal(workerId: string, name: WorkerSignalName): Promise<{ readonly ok: true } | BindingRefusal> {
    // A worker that is not tracked here has exited, or never was ours: nothing to send.
    if (!this.workers.has(workerId)) return Promise.resolve(refusal('not-found'))
    const id = randomUUID()
    return this.request(signalFrame(workerId, name, id), id, 'signalled', this.requestTimeoutMs, (frame) => {
      // An ack for another worker is not an ack for this one.
      if ((frame as Extract<Inbound, { type: 'signalled' }>).workerId !== workerId) throw new WireViolation('signal ack names another worker')
      return Object.freeze({ ok: true as const })
    }, MAX_BINDING_FRAME_BYTES)
  }

  status(): Promise<BrokerStatus | BindingRefusal> {
    const id = randomUUID()
    return this.request(statusFrame(id), id, 'status', this.requestTimeoutMs, frame => toStatus(frame as Extract<Inbound, { type: 'status' }>), MAX_BINDING_FRAME_BYTES)
  }

  /** Drop the connection. The broker treats that as revoking everything. Idempotent. */
  closeControl(): void {
    this.close('control-closed')
  }

  // -- plumbing ----------------------------------------------------------------------------------------------------

  private async request<T>(
    frame: Record<string, unknown>,
    requestId: string,
    expect: Inbound['type'],
    timeoutMs: number,
    accept: (frame: Inbound) => T,
    limit: number,
  ): Promise<T | BindingRefusal> {
    if (this.state === 'closed') return refusal(this.closedReason)
    const line = `${JSON.stringify(frame)}\n`
    if (Buffer.byteLength(line) > limit) return refusal('invalid-request')
    if (this.state === 'connecting') {
      const registered = await this.registered.promise.catch(() => false)
      if (!registered || !this.is('ready')) return refusal(this.closedReason)
    }
    return await new Promise<T | BindingRefusal>((resolve) => {
      if (this.state !== 'ready') { resolve(refusal(this.closedReason)); return }
      const timer = setTimeout(() => {
        // The broker's state is unknown, so the connection is dropped and with it every grant.
        this.pending.delete(requestId)
        resolve(refusal('broker-timeout'))
        this.close('broker-timeout')
      }, timeoutMs)
      this.pending.set(requestId, { expect, accept, resolve: resolve as (value: unknown) => void, timer })
      if (!this.writeLine(line)) {
        this.pending.delete(requestId)
        clearTimeout(timer)
        resolve(refusal(this.closedReason))
      }
    })
  }

  /** A method, not a field read, so a state changed by a callback is not narrowed away by the control flow. */
  private is(state: 'connecting' | 'ready' | 'closed'): boolean { return this.state === state }

  private write(frame: Record<string, unknown>): void {
    this.writeLine(`${JSON.stringify(frame)}\n`)
  }

  private writeLine(line: string): boolean {
    if (this.socket.destroyed) return false
    try {
      this.socket.write(line)
      return true
    } catch {
      this.close('control-unavailable')
      return false
    }
  }

  private read(chunk: string): void {
    if (this.state === 'closed') return
    this.buffer += chunk
    while (!this.is('closed')) {
      const end = this.buffer.indexOf('\n')
      if (end < 0) break
      const line = this.buffer.slice(0, end)
      this.buffer = this.buffer.slice(end + 1)
      let frame: Inbound
      try {
        frame = parseInbound(JSON.parse(line) as unknown)
        this.handle(frame)
      } catch {
        // Unparseable, unknown, or contradicting what Core asked for: a broker Core cannot understand is a broker
        // Core cannot trust, and the only safe answer is to end the connection.
        this.close('broker-malformed')
      }
    }
    if (this.buffer.length > MAX_INBOUND_LINE) this.close('broker-malformed')
  }

  private handle(frame: Inbound): void {
    if (frame.type === 'registered') {
      if (this.state !== 'connecting' || frame.requestId !== this.registerId) throw new WireViolation('unexpected registration')
      this.state = 'ready'
      this.brokerEpoch = frame.revision
      clearTimeout(this.registerTimer)
      this.registered.resolve(true)
      return
    }
    if (frame.type === 'worker-exited') {
      if (this.state !== 'ready') throw new WireViolation('worker exit before registration')
      const tracked = this.workers.get(frame.workerId)
      if (tracked === undefined) {
        if (this.earlyExits.size >= MAX_EARLY_EXITS || this.earlyExits.has(frame.workerId)) throw new WireViolation('unattributable worker exit')
        this.earlyExits.set(frame.workerId, frame)
      } else this.finishWorker(frame.workerId, tracked, frame)
      return
    }
    if (frame.type === 'error' && (frame.requestId === undefined || this.state === 'connecting')) {
      // A connection-level refusal (the peer was denied, Core is not the owner, a second control connection, a frame
      // the broker could not parse) or a refused registration. No request can be attributed to it, so the connection
      // is over; the broker's code says why.
      this.close(frame.code)
      return
    }
    if (this.state !== 'ready') throw new WireViolation('frame before registration')
    const requestId = (frame as { requestId: string }).requestId
    const pending = this.pending.get(requestId)
    if (pending === undefined) throw new WireViolation('answer matches no request')
    this.pending.delete(requestId)
    clearTimeout(pending.timer)
    if (frame.type === 'error') {
      pending.resolve(refusalOf(frame))
      return
    }
    if (frame.type !== pending.expect) {
      pending.resolve(refusal('broker-malformed'))
      throw new WireViolation('answer of the wrong kind')
    }
    try {
      pending.resolve(pending.accept(frame))
    } catch (error) {
      pending.resolve(refusal('broker-malformed'))
      throw error
    }
  }

  private finishWorker(workerId: string, tracked: Tracked, frame: Extract<Inbound, { type: 'worker-exited' }>): void {
    this.workers.delete(workerId)
    if (tracked.bindingId !== undefined && tracked.bindingId !== frame.bindingId) {
      tracked.exit.resolve({ ok: false, reason: 'broker-malformed' })
      throw new WireViolation('worker exit names another binding')
    }
    tracked.exit.resolve({ ok: true, code: frame.code, signal: frame.signal })
  }

  private close(reason: BindingRefusalReason): void {
    if (this.state === 'closed') return
    this.state = 'closed'
    this.closedReason = reason
    clearTimeout(this.registerTimer)
    this.registered.resolve(false)
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer)
      this.pending.delete(id)
      pending.resolve(refusal(reason))
    }
    for (const [id, tracked] of this.workers) {
      this.workers.delete(id)
      tracked.exit.resolve({ ok: false, reason })
    }
    this.earlyExits.clear()
    this.leased.clear()
    this.buffer = ''
    this.socket.destroy()
    for (const listener of this.closedListeners.splice(0)) {
      try { listener(reason) } catch { /* a listener must not keep the others from learning the connection is gone */ }
    }
  }
}

// -- who is on the other end of the path ----------------------------------------------------------------------------

/** Who must own the socket and its directory. The broker runs as root; a test overrides it with its own uid. */
export interface SocketTrust {
  readonly ownerUid?: number
}

function describe(path: string, stats: Stats): string {
  return `${path} (uid ${stats.uid}, gid ${stats.gid}, mode ${(stats.mode & 0o7777).toString(8).padStart(4, '0')})`
}

/**
 * Whether the path may be connected to. Core sends its bindings, leases and launch requests to whatever listens
 * there, and a listener Core does not own could answer `started` for a launch that never happened. The broker's
 * socket is `root:<group of Core>`, mode 0660, in a root-owned directory nobody else can write to, so:
 *
 * - the directory that holds the socket (after resolving every symlink) belongs to the owner and is not writable by
 *   group or others, which is what stops anyone else from putting a socket in its place;
 * - every directory above it belongs to root (or the owner) and is not writable by group or others, except where the
 *   sticky bit keeps other users from renaming it (`/tmp`, or `/run/user`);
 * - the socket itself, if it exists, is a real socket (not a symlink or a file), belongs to the owner, gives others
 *   no access, and its group is one Core is a member of (or Core is root).
 *
 * A path that does not exist yet is not distrusted: the broker may be restarting, and connecting simply fails.
 * @returns The resolved path to connect to (connecting to the resolved path, not to the original, leaves no symlink to
 *   swap between this check and the connect), or why the path is not trusted.
 */
export function socketDistrust(socketPath: string, trust: SocketTrust = {}): { readonly path: string } | { readonly problem: string } {
  const owner = trust.ownerUid ?? 0
  if (!isAbsolute(socketPath) || socketPath.includes('\0')) return { problem: 'the broker socket path is not an absolute path' }
  let directory: string
  try {
    directory = realpathSync(dirname(socketPath))
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { path: socketPath }
    return { problem: `the broker socket directory ${dirname(socketPath)} cannot be examined (${(error as NodeJS.ErrnoException).code ?? 'error'})` }
  }
  try {
    const parent = lstatSync(directory)
    if (parent.uid !== owner) return { problem: `the broker socket directory ${describe(directory, parent)} is not owned by uid ${owner}` }
    if ((parent.mode & 0o022) !== 0) return { problem: `the broker socket directory ${describe(directory, parent)} is writable by its group or by others` }
    for (let above = dirname(directory); above !== dirname(above); above = dirname(above)) {
      const stats = lstatSync(above)
      if (stats.uid !== 0 && stats.uid !== owner) return { problem: `${describe(above, stats)}, above the broker socket, belongs to another user` }
      if ((stats.mode & 0o022) !== 0 && (stats.mode & 0o1000) === 0) {
        return { problem: `${describe(above, stats)}, above the broker socket, is writable by its group or by others` }
      }
    }
  } catch (error) {
    return { problem: `the path above the broker socket cannot be examined (${(error as NodeJS.ErrnoException).code ?? 'error'})` }
  }
  const resolved = join(directory, basename(socketPath))
  let socket: Stats
  try {
    socket = lstatSync(resolved)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { path: resolved }
    return { problem: `the broker socket ${resolved} cannot be examined (${(error as NodeJS.ErrnoException).code ?? 'error'})` }
  }
  if (!socket.isSocket()) return { problem: `${describe(resolved, socket)} is not a Unix socket` }
  if (socket.uid !== owner) return { problem: `the broker socket ${describe(resolved, socket)} is not owned by uid ${owner}` }
  if ((socket.mode & 0o007) !== 0) return { problem: `the broker socket ${describe(resolved, socket)} is accessible to others` }
  const groups = new Set([process.getegid?.(), process.getgid?.(), ...process.getgroups?.() ?? []])
  if (process.geteuid?.() !== 0 && !groups.has(socket.gid)) {
    return { problem: `the group of the broker socket ${describe(resolved, socket)} is not one this process belongs to` }
  }
  return { path: resolved }
}

// -- reconnecting ---------------------------------------------------------------------------------------------------

/** What a loss may be answered with a new connection: the transport went away, Core did not decide to end it. */
const TRANSPORT_LOSS: ReadonlySet<string> = new Set(['control-closed', 'control-unavailable'])

/**
 * The client Core runs when a socket is configured: one {@link BrokerControlClient} at a time, replaced by a fresh
 * one when the transport is lost. A replacement is a new connection, so a new `register-core`, a new epoch and no
 * bindings at all; nothing granted before the loss is usable after it. While a replacement is not registered every
 * call is refused `control-reconnecting`, never queued, so nothing waits on a broker that is not there. After the
 * bounded attempts are used, or on any close Core asked for, the client is closed for good (`control-closed`).
 * The socket is checked with {@link socketDistrust} before every connection, the first and each replacement.
 */
export class ReconnectingBrokerClient implements BindingBrokerClient {
  private current: BrokerControlClient | undefined
  private phase: 'up' | 'reconnecting' | 'closed' = 'reconnecting'
  private finalReason: BindingRefusalReason = 'control-closed'
  private finalDetail: string | undefined
  /** Why the last attempt did not connect, when it was the socket check. */
  private lastProblem: string | undefined
  private timer: ReturnType<typeof setTimeout> | undefined
  private attempt = 0
  private lifetimeLeft: number
  private readonly attempts: number
  private readonly baseDelayMs: number
  private readonly listeners: Array<(reason: BindingRefusalReason) => void> = []

  constructor(
    readonly socketPath: string,
    private readonly options: BrokerClientOptions = {},
    reconnect: ReconnectOptions = {},
    private readonly trust: SocketTrust = {},
  ) {
    this.attempts = reconnect.attempts ?? DEFAULT_RECONNECT_ATTEMPTS
    this.baseDelayMs = reconnect.baseDelayMs ?? DEFAULT_RECONNECT_DELAY_MS
    this.lifetimeLeft = reconnect.lifetimeAttempts ?? DEFAULT_LIFETIME_RECONNECTS
    this.open(true)
  }

  get epoch(): number | undefined { return this.phase === 'up' ? this.current?.epoch : undefined }
  get closed(): boolean { return this.phase === 'closed' }

  private open(initial: boolean): void {
    // `options.connect` is a test seam for a socket that is not a path on disk.
    let path = this.socketPath
    if (this.options.connect === undefined) {
      const verdict = socketDistrust(this.socketPath, this.trust)
      if ('problem' in verdict) {
        // Never connected to. It may be a broker that is still setting its socket up (created, not yet chowned), so it
        // is an attempt like any other: retried within the bounds, and refused as `untrusted-socket` meanwhile.
        this.lastProblem = verdict.problem
        this.retryOrFinish('untrusted-socket', false, verdict.problem)
        return
      }
      path = verdict.path
    }
    this.lastProblem = undefined
    const client = new BrokerControlClient(path, this.options)
    this.current = client
    // The first connection is usable at once (requests queue behind its registration inside the client). A
    // replacement is not usable until it has registered: until then calls are refused, not queued.
    this.phase = initial ? 'up' : 'reconnecting'
    client.onClosed((reason) => { this.lost(client, reason) })
    void client.whenReady().then((ready) => {
      if (ready && this.current === client && this.phase === 'reconnecting') this.phase = 'up'
    })
  }

  private notify(reason: BindingRefusalReason): void {
    for (const listener of this.listeners) { try { listener(reason) } catch { /* see BrokerControlClient.close */ } }
  }

  private finish(reason: BindingRefusalReason, detail?: string): void {
    this.phase = 'closed'
    this.finalReason = reason
    this.finalDetail = detail
    clearTimeout(this.timer)
    this.current = undefined
    this.notify(reason)
  }

  private lost(client: BrokerControlClient, reason: BindingRefusalReason): void {
    if (this.current !== client || this.phase === 'closed') return
    // Whether the connection ever held grants: a replacement that never registered held none.
    const registered = client.epoch !== undefined
    if (registered) this.attempt = 0
    this.retryOrFinish(reason, registered)
  }

  /**
   * After a connection ended or could not be made: try again after a growing delay, or close for good. A connection
   * that held grants is only replaced after a transport loss; one that never registered is retried unless the broker
   * broke the protocol.
   */
  private retryOrFinish(reason: BindingRefusalReason, registered: boolean, detail?: string): void {
    const retryable = registered ? TRANSPORT_LOSS.has(reason) : reason !== 'broker-malformed'
    if (!retryable || this.attempt >= this.attempts || this.lifetimeLeft <= 0) {
      this.finish(reason, detail)
      return
    }
    this.current = undefined
    this.phase = 'reconnecting'
    // Told before anything can be asked of a replacement: the issuer forgets its bindings, the broker revoked them.
    if (registered) this.notify(reason)
    const delay = this.baseDelayMs * 2 ** this.attempt
    this.attempt += 1
    this.lifetimeLeft -= 1
    this.timer = setTimeout(() => {
      this.timer = undefined
      if (this.phase === 'reconnecting') this.open(false)
    }, delay)
    this.timer.unref()
  }

  private away(): BindingRefusal {
    if (this.phase === 'closed') {
      return Object.freeze({ ok: false, reason: this.finalReason, ...this.finalDetail === undefined ? {} : { detail: this.finalDetail } })
    }
    if (this.lastProblem !== undefined) return Object.freeze({ ok: false, reason: 'untrusted-socket', detail: this.lastProblem })
    return refusal('control-reconnecting')
  }

  private live(): BrokerControlClient | undefined {
    return this.phase === 'up' && this.current !== undefined ? this.current : undefined
  }

  bind(request: BindingRequest): Promise<BindingGrant | BindingRefusal> {
    return this.live()?.bind(request) ?? Promise.resolve(this.away())
  }

  revoke(request: RevokeRequest): Promise<{ readonly ok: true } | BindingRefusal> {
    return this.live()?.revoke(request) ?? Promise.resolve(this.away())
  }

  assign(request: AssignRequest): Promise<AssignmentGrant | BindingRefusal> {
    return this.live()?.assign(request) ?? Promise.resolve(this.away())
  }

  start(request: StartRequest): Promise<StartGrant | BindingRefusal> {
    return this.live()?.start(request) ?? Promise.resolve(this.away())
  }

  signal(workerId: string, name: WorkerSignalName): Promise<{ readonly ok: true } | BindingRefusal> {
    return this.live()?.signal(workerId, name) ?? Promise.resolve(this.away())
  }

  status(): Promise<BrokerStatus | BindingRefusal> {
    return this.live()?.status() ?? Promise.resolve(this.away())
  }

  onClosed(listener: (reason: BindingRefusalReason) => void): void {
    this.listeners.push(listener)
  }

  /** Close for good: no further attempt is made, and the broker revokes everything. Idempotent. */
  closeControl(): void {
    const current = this.current
    if (this.phase === 'closed') return
    this.phase = 'closed'
    this.finalReason = 'control-closed'
    clearTimeout(this.timer)
    this.current = undefined
    if (current !== undefined) {
      // Its own close notifies through `lost`, which ignores a client that is no longer current; tell the listeners here.
      current.closeControl()
    }
    this.notify('control-closed')
  }
}

/**
 * The client Core runs. A real control connection exists only when an explicit broker socket is configured;
 * otherwise the deny client answers `assignment-unavailable` to everything, which is what production does by default.
 * @param broker The trusted broker config, or `undefined` when none was declared.
 * @param options Timeouts and the test seam for the socket.
 * @param reconnect Reconnect bounds; `{ attempts: 0 }` keeps a lost connection lost.
 * @param trust Who must own the socket (root unless a test says otherwise).
 */
export function createBrokerClient(
  broker: BrokerConfig | undefined,
  options: BrokerClientOptions = {},
  reconnect: ReconnectOptions = {},
  trust: SocketTrust = {},
): BindingBrokerClient {
  if (broker === undefined || typeof broker.socketPath !== 'string' || broker.socketPath.length === 0) return assignmentUnavailableClient
  return new ReconnectingBrokerClient(broker.socketPath, options, reconnect, trust)
}
