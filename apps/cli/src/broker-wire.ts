/**
 * The wire edge between Core and the root broker (S6 contract, `regia/broker/protocol.py` is the source of truth for
 * every field name). Core's internal request types stay Core's own; this module is the only place they turn into
 * frames and the only place broker answers turn back into Core's grant and refusal types.
 *
 * - Every frame carries `type`, `version: 1` and a UUIDv4 `requestId`.
 * - Core never sends an epoch. The broker's integer epoch comes back in `registered` and is the only one.
 * - Builders copy named fields only, so nothing extra on a request object can reach the wire.
 * - `room` is renamed here (`origine`, `stanzaId`, `agente`, `installazione` to `origin`, `roomId`, `agentId`,
 *   `installationId`); `child` carries `parentBindingId` and neither `cwd` nor `orca`.
 * - `lease` always asks `mode: "process"`; `terminal` stays refused in v1 and Core has no way to ask for it.
 * - Answers are parsed strictly. An unknown type, an unknown field or a wrong value is a violation, which the client
 *   turns into a refusal (fail closed), never into a grant.
 * @module @deepseek-ai/dsh/broker-wire
 */

import { constants } from 'node:os'
import type {
  AssignmentGrant, AssignRequest, BindingGrant, BindingRefusal, BindingRefusalReason, BindingRequest, BrokerStatus,
  RevokeRequest, StartRequest,
} from './broker-binding.ts'
import { isBrokerLabel } from './broker-binding.ts'

export const WIRE_VERSION = 1

/** Mode of every `lease` Core sends. `terminal` is refused by the v1 broker. */
export const LEASE_MODE = 'process'

type Frame = Record<string, unknown>

/** Thrown by the parsers; the client turns it into a refusal and, for unsolicited frames, a closed connection. */
export class WireViolation extends Error {}

export function registerFrame(requestId: string): Frame {
  return { type: 'register-core', version: WIRE_VERSION, requestId }
}

export function bindFrame(request: BindingRequest, requestId: string): Frame {
  return {
    type: 'bind', version: WIRE_VERSION, requestId,
    sessionId: request.sessionId, workspaceId: request.workspaceId, kind: request.kind,
    ...request.parentBindingId === undefined ? {} : { parentBindingId: request.parentBindingId },
    ...request.cwd === undefined ? {} : { cwd: request.cwd },
    ...request.orca === undefined ? {} : { orca: { instanceId: request.orca.instanceId } },
    ...request.room === undefined ? {} : {
      room: {
        origin: request.room.origine, roomId: request.room.stanzaId, agentId: request.room.agente,
        installationId: request.room.installazione,
      },
    },
    ...request.orgId === undefined ? {} : { orgId: request.orgId },
  }
}

export function revokeFrame(request: RevokeRequest, requestId: string): Frame {
  return { type: 'revoke', version: WIRE_VERSION, requestId, bindingId: request.bindingId }
}

export function leaseFrame(request: AssignRequest, requestId: string): Frame {
  return { type: 'lease', version: WIRE_VERSION, requestId, bindingId: request.bindingId, revision: request.revision, mode: LEASE_MODE }
}

/** v1 stdio is `ignore` for all three streams: the broker refuses anything else (`stdio-unsupported`). */
export function startFrame(request: StartRequest, requestId: string): Frame {
  return {
    type: 'start', version: WIRE_VERSION, requestId, mode: 'process', assignmentId: request.assignmentId,
    argv: [...request.argv], cwd: request.cwd, env: { ...request.env },
    stdio: { stdin: { mode: 'ignore' }, stdout: { mode: 'ignore' }, stderr: { mode: 'ignore' } },
    graceMs: request.graceMs,
  }
}

export function statusFrame(requestId: string): Frame {
  return { type: 'status', version: WIRE_VERSION, requestId }
}

// -- limits the broker enforces on `start`, checked here so a frame it would reject is never sent ----------------

const MAX_ARGV = 128
const MAX_ENV = 256
const MAX_STRING = 4096
const MAX_START_BYTES = 256 * 1024
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/u
const ASSIGNMENT = /^[A-Za-z0-9_-]{1,128}$/u

/**
 * Why the broker would refuse this `start` frame by shape, or `undefined`. A frame the broker cannot parse is
 * answered without a request id, which Core could not attribute to the caller and would have to treat as a broken
 * connection, so the launcher refuses locally first.
 */
export function startRequestProblem(request: StartRequest): string | undefined {
  if (!ASSIGNMENT.test(request.assignmentId)) return 'invalid-assignment'
  if (request.argv.length < 1 || request.argv.length > MAX_ARGV) return 'invalid-argv'
  for (const [index, arg] of request.argv.entries()) {
    if (typeof arg !== 'string' || arg.length > MAX_STRING || arg.includes('\0') || (index === 0 && arg.length === 0)) return 'invalid-argv'
  }
  if (typeof request.cwd !== 'string' || request.cwd.length === 0 || request.cwd.length > MAX_STRING
    || request.cwd.includes('\0') || !request.cwd.startsWith('/')) return 'invalid-cwd'
  const names = Object.keys(request.env)
  if (names.length > MAX_ENV) return 'invalid-env'
  for (const name of names) {
    const value = request.env[name]
    const upper = name.toUpperCase()
    if (!ENV_NAME.test(name) || name.length > 128 || typeof value !== 'string' || value.length > MAX_STRING || value.includes('\0')) return 'invalid-env'
    if (upper === 'DSH_SESSION_ID' || upper.startsWith('DSH_BROKER_') || upper === 'BROKER_ASSIGNMENT_ID') return 'invalid-env'
  }
  if (!Number.isSafeInteger(request.graceMs) || request.graceMs < 1 || request.graceMs > 2_147_483_647) return 'invalid-grace'
  if (Buffer.byteLength(JSON.stringify(startFrame(request, '00000000-0000-4000-8000-000000000000'))) > MAX_START_BYTES) return 'frame-too-large'
  return undefined
}

// -- answers ---------------------------------------------------------------------------------------------------------

export type Inbound =
  | { readonly type: 'registered'; readonly requestId: string; readonly revision: number }
  | { readonly type: 'bound'; readonly requestId: string; readonly bindingId: string; readonly revision: number }
  | { readonly type: 'leased'; readonly requestId: string; readonly assignmentId: string; readonly revision: number }
  | { readonly type: 'revoked'; readonly requestId: string; readonly bindingId: string }
  | { readonly type: 'started'; readonly requestId: string; readonly pid: number; readonly workerId: string }
  | { readonly type: 'status'; readonly requestId: string; readonly status: Readonly<Record<string, unknown>> }
  | {
    readonly type: 'error'
    readonly requestId: string | undefined
    readonly code: string
    readonly reason: string | undefined
  }
  | {
    readonly type: 'worker-exited'
    readonly workerId: string
    readonly bindingId: string
    readonly code: number | null
    readonly signal: string | null
  }

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u
const TOKEN = /^[a-z0-9][a-z0-9._:-]{0,63}$/u

function object(value: unknown, required: readonly string[], optional: readonly string[] = []): Frame {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new WireViolation('frame is not an object')
  const record = value as Frame
  const keys = Object.keys(record)
  if (!required.every(key => key in record) || keys.some(key => !required.includes(key) && !optional.includes(key))) {
    throw new WireViolation('unexpected fields')
  }
  return record
}

function requestIdOf(value: unknown): string {
  if (typeof value !== 'string' || !UUID_V4.test(value)) throw new WireViolation('invalid request id')
  return value
}

function integer(value: unknown, minimum: number): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum) throw new WireViolation('invalid integer')
  return value
}

function labelOf(value: unknown): string {
  if (!isBrokerLabel(value)) throw new WireViolation('invalid identifier')
  return value
}

function assignmentOf(value: unknown): string {
  if (typeof value !== 'string' || !ASSIGNMENT.test(value)) throw new WireViolation('invalid assignment id')
  return value
}

function signalOf(value: unknown): string | null {
  if (value === null) return null
  if (typeof value === 'number') {
    const name = Object.entries(constants.signals).find(([, number]) => number === value)?.[0]
    if (name === undefined) throw new WireViolation('invalid signal')
    return name
  }
  if (typeof value !== 'string' || !/^SIG[A-Z0-9]{1,12}$/u.test(value)) throw new WireViolation('invalid signal')
  return value
}

/**
 * Parse one line from the broker.
 * @throws {WireViolation} for anything outside the contract: unknown type or field, wrong value, bad request id.
 */
export function parseInbound(raw: unknown): Inbound {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) throw new WireViolation('frame is not an object')
  const { type } = raw as Frame
  switch (type) {
    case 'registered': {
      const f = object(raw, ['type', 'requestId', 'revision'])
      return { type, requestId: requestIdOf(f.requestId), revision: integer(f.revision, 1) }
    }
    case 'bound': {
      const f = object(raw, ['type', 'requestId', 'bindingId', 'revision'])
      return { type, requestId: requestIdOf(f.requestId), bindingId: labelOf(f.bindingId), revision: integer(f.revision, 1) }
    }
    case 'leased': {
      const f = object(raw, ['type', 'requestId', 'assignmentId', 'revision'], ['ttlMs'])
      if (f.ttlMs !== undefined) integer(f.ttlMs, 1)
      return { type, requestId: requestIdOf(f.requestId), assignmentId: assignmentOf(f.assignmentId), revision: integer(f.revision, 1) }
    }
    case 'revoked': {
      const f = object(raw, ['type', 'requestId', 'bindingId'], ['count'])
      if (f.count !== undefined) integer(f.count, 0)
      return { type, requestId: requestIdOf(f.requestId), bindingId: labelOf(f.bindingId) }
    }
    case 'started': {
      const f = object(raw, ['type', 'requestId', 'pid', 'workerId'])
      if (typeof f.workerId !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/u.test(f.workerId)) throw new WireViolation('invalid worker id')
      return { type, requestId: requestIdOf(f.requestId), pid: integer(f.pid, 1), workerId: f.workerId }
    }
    case 'status': {
      // A read-back for the operator: relayed, never used as authority, so its inner shape is not pinned.
      const { type: _type, requestId, ...rest } = raw as Frame
      return { type, requestId: requestIdOf(requestId), status: Object.freeze(rest) }
    }
    case 'error': {
      const f = object(raw, ['type', 'code'], ['message', 'requestId', 'reason'])
      if (typeof f.code !== 'string' || !TOKEN.test(f.code)) throw new WireViolation('invalid error code')
      if (f.reason !== undefined && (typeof f.reason !== 'string' || f.reason.length === 0 || f.reason.length > 128)) {
        throw new WireViolation('invalid error reason')
      }
      const reason = typeof f.reason === 'string' ? f.reason : undefined
      return { type, code: f.code, requestId: f.requestId === undefined ? undefined : requestIdOf(f.requestId), reason }
    }
    case 'worker-exited': {
      const f = object(raw, ['type', 'workerId', 'bindingId', 'code', 'signal'])
      if (typeof f.workerId !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/u.test(f.workerId)) throw new WireViolation('invalid worker id')
      const code = f.code === null ? null : integer(f.code, -1_000_000)
      return { type, workerId: f.workerId, bindingId: labelOf(f.bindingId), code, signal: signalOf(f.signal) }
    }
    default:
      throw new WireViolation('unknown frame type')
  }
}

/** The refusal an `error` frame stands for. The broker's `reason` is the specific token; `code` is the category. */
export function refusalOf(frame: Extract<Inbound, { type: 'error' }>): BindingRefusal {
  const reason: BindingRefusalReason = frame.reason ?? frame.code
  return Object.freeze({ ok: false, reason })
}

/** Translations into Core's own types. Each takes the frame already matched to the request that asked for it. */
export function toBindingGrant(frame: Extract<Inbound, { type: 'bound' }>): BindingGrant {
  return Object.freeze({ ok: true, bindingId: frame.bindingId, revision: frame.revision })
}

export function toAssignmentGrant(frame: Extract<Inbound, { type: 'leased' }>): AssignmentGrant {
  return Object.freeze({ ok: true, assignmentId: frame.assignmentId, revision: frame.revision })
}

export function toStatus(frame: Extract<Inbound, { type: 'status' }>): BrokerStatus {
  return Object.freeze({ ok: true, status: frame.status })
}
