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
 * Which client Core runs is decided by {@link createBrokerClient}: the real one only when a broker socket is
 * configured, the deny client otherwise.
 * @module @deepseek-ai/dsh/broker-client
 */

import { randomUUID } from 'node:crypto'
import { createConnection, type Socket } from 'node:net'
import {
  assignmentUnavailableClient, type AssignmentGrant, type AssignRequest, type BindingBrokerClient,
  type BindingGrant, type BindingRefusal, type BindingRefusalReason, type BindingRequest, type BrokerStatus,
  type RevokeRequest, type StartGrant, type StartRequest, type WorkerExit,
} from './broker-binding.ts'
import type { BrokerConfig } from './broker-required.ts'
import {
  bindFrame, leaseFrame, parseInbound, refusalOf, registerFrame, revokeFrame, startFrame, startRequestProblem,
  statusFrame, toAssignmentGrant, toBindingGrant, toStatus, WireViolation, type Inbound,
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
    if (problem !== undefined) return Promise.resolve(refusal(problem))
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
  }
}

/**
 * The client Core runs. A real control connection exists only when an explicit broker socket is configured;
 * otherwise the deny client answers `assignment-unavailable` to everything, which is what production does by default.
 * @param broker The trusted broker config, or `undefined` when none was declared.
 * @param options Timeouts and the test seam for the socket.
 */
export function createBrokerClient(broker: BrokerConfig | undefined, options: BrokerClientOptions = {}): BindingBrokerClient {
  if (broker === undefined || typeof broker.socketPath !== 'string' || broker.socketPath.length === 0) return assignmentUnavailableClient
  return new BrokerControlClient(broker.socketPath, options)
}
