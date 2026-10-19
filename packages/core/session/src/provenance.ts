/**
 * Runtime provenance for a live Session. Provenance is held outside the durable
 * SessionHeader: restoring or forking a Session does not restore its authority.
 * Callers are trusted server code and must obtain claims from an authoritative
 * creator or policy source. User input, SessionHeader fields and failed lookups
 * are not claims.
 *
 * The API issuer object is an internal accidental-misuse guard, not a security
 * boundary against hostile in-process plugins. Never treat its claim alone as
 * a permission grant; effect handlers must recheck their authoritative policy.
 */
import { apiSessionControllerIssuer, dshRoomLeaseIssuer, dshSvegliaCreationIssuer } from './issuers.ts'

declare const creationReceiptBrand: unique symbol

/** One-use proof that AgentRegistry created this exact live session. */
export interface AgentSessionCreationReceipt {
  readonly [creationReceiptBrand]: true
}

export type SessionProvenanceClaim =
  | {
      readonly kind: 'room'
      readonly source: string
      readonly sourceVersion: string
      readonly roomId: number
      readonly orgId: string
      readonly agent: string
    }
  | {
      readonly kind: 'nonroom'
      readonly source: string
      readonly sourceVersion: string
      readonly scope: { readonly kind: 'conversation' } | { readonly kind: 'workspace'; readonly id: string }
    }

export type VerifiedSessionProvenance =
  | {
      readonly state: 'room_verified'
      readonly sessionId: string
      readonly roomId: number
      readonly orgId: string
      readonly agent: string
      readonly source: string
      readonly sourceVersion: string
      readonly revision: number
      readonly issuedAt: number
    }
  | {
      readonly state: 'nonroom_verified'
      readonly sessionId: string
      readonly scope: { readonly kind: 'conversation' } | { readonly kind: 'workspace'; readonly id: string }
      readonly source: string
      readonly sourceVersion: string
      readonly revision: number
      readonly issuedAt: number
    }

export interface UnknownSessionProvenance {
  readonly state: 'unknown'
  readonly sessionId: string
  readonly revision: number
  readonly reason: 'not_live' | 'not_attested' | 'revoked' | 'stale'
}

export type SessionProvenance = VerifiedSessionProvenance | UnknownSessionProvenance

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  return Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key))
}

function requiredText(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 200
}

function validateClaim(value: unknown): SessionProvenanceClaim {
  if (!isRecord(value) || typeof value.kind !== 'string'
    || !requiredText(value.source) || !requiredText(value.sourceVersion)) {
    throw new TypeError('session provenance claim is invalid')
  }

  if (value.kind === 'room') {
    if (!exactKeys(value, ['kind', 'source', 'sourceVersion', 'roomId', 'orgId', 'agent'])
      || !Number.isSafeInteger(value.roomId) || Number(value.roomId) <= 0
      || typeof value.orgId !== 'string' || !/^[1-9]\d{0,18}$/.test(value.orgId)
      || typeof value.agent !== 'string' || !/^[0-9a-z][a-z0-9_-]{1,100}$/.test(value.agent)) {
      throw new TypeError('room provenance claim is invalid')
    }
    return Object.freeze({
      kind: 'room', source: value.source, sourceVersion: value.sourceVersion,
        roomId: Number(value.roomId), orgId: value.orgId, agent: value.agent,
    })
  }

  if (value.kind === 'nonroom') {
    if (!exactKeys(value, ['kind', 'source', 'sourceVersion', 'scope']) || !isRecord(value.scope)) {
      throw new TypeError('non-room provenance claim is invalid')
    }
    const scope = value.scope
    if (scope.kind === 'conversation' && exactKeys(scope, ['kind'])) {
      return Object.freeze({
        kind: 'nonroom', source: value.source, sourceVersion: value.sourceVersion,
        scope: Object.freeze({ kind: 'conversation' }),
      })
    }
    if (scope.kind === 'workspace' && exactKeys(scope, ['kind', 'id']) && requiredText(scope.id)) {
      return Object.freeze({
        kind: 'nonroom', source: value.source, sourceVersion: value.sourceVersion,
        scope: Object.freeze({ kind: 'workspace', id: scope.id }),
      })
    }
  }

  throw new TypeError('session provenance claim is invalid')
}

/**
 * Holds server-issued provenance against the exact live Session object.
 * Claims never come from session ids, names, argv, cwd, or durable metadata.
 */
export class SessionProvenanceRegistry<Session extends { readonly id: string }> {
  private readonly claims = new WeakMap<Session, VerifiedSessionProvenance>()
  private revision = 0
  private readonly isLive: (session: Session) => boolean
  private readonly options: {
    readonly now?: () => number
    readonly ttlMs?: number
    readonly consumeAgentCreationReceipt?: (session: Session, receipt: AgentSessionCreationReceipt) => boolean
  }

  /**
   * @param isLive - exact-object liveness check owned by SessionStore.
   * @param options - runtime expiry and the Core-owned AgentRegistry receipt consumer.
   */
  constructor(
    isLive: (session: Session) => boolean,
    options: {
      readonly now?: () => number
      readonly ttlMs?: number
      readonly consumeAgentCreationReceipt?: (session: Session, receipt: AgentSessionCreationReceipt) => boolean
    } = {},
  ) {
    if (options.ttlMs !== undefined && (!Number.isSafeInteger(options.ttlMs) || options.ttlMs <= 0)) {
      throw new RangeError('session provenance ttlMs must be a positive safe integer')
    }
    this.isLive = isLive
    this.options = options
  }

  /** Attest a fresh Sveglia root session once, using its AgentRegistry creation receipt. */
  issueSvegliaSession(session: Session, receipt: AgentSessionCreationReceipt): VerifiedSessionProvenance {
    const createdHere = this.options.consumeAgentCreationReceipt?.(session, receipt) === true
    if (!createdHere || !this.isLive(session)) {
      throw new Error('Sveglia provenance requires a receipt for this exact newly created live session')
    }
    return this.issueClaim(session, dshSvegliaCreationIssuer, {
      kind: 'nonroom', source: '@myh/dsh-stanze/sveglia', sourceVersion: '1',
      scope: { kind: 'conversation' },
    })
  }

  /** Issue an immutable, runtime-only claim for one exact live Session. */
  issue(session: Session, issuer: object, rawClaim: SessionProvenanceClaim): VerifiedSessionProvenance {
    if (issuer === dshSvegliaCreationIssuer) throw new Error('Sveglia provenance requires an AgentRegistry creation receipt')
    return this.issueClaim(session, issuer, rawClaim)
  }

  private issueClaim(session: Session, issuer: object, rawClaim: SessionProvenanceClaim): VerifiedSessionProvenance {
    if (issuer !== apiSessionControllerIssuer && issuer !== dshRoomLeaseIssuer && issuer !== dshSvegliaCreationIssuer) {
      throw new Error('session provenance issuer is not authorized')
    }
    if (!this.isLive(session)) throw new Error(`session "${session.id}" is not live in this store`)
    const claim = validateClaim(rawClaim)
    if (issuer === apiSessionControllerIssuer && (claim.kind !== 'nonroom' || claim.source !== '@deepseek-ai/dsh-api-session-controller')) {
      throw new Error('API Session issuer can only attest ordinary conversations')
    }
    if (issuer === dshRoomLeaseIssuer && (claim.kind !== 'room' || claim.source !== '@myh/dsh-stanze')) {
      throw new Error('DSH ROOM lease issuer can only attest DSH ROOM sessions')
    }
    if (issuer === dshSvegliaCreationIssuer && (claim.kind !== 'nonroom' || claim.source !== '@myh/dsh-stanze/sveglia'
      || claim.scope.kind !== 'conversation')) {
      throw new Error('Sveglia creation can only attest a scheduled conversation')
    }
    const previous = this.claims.get(session)
    if (previous !== undefined && !this.sameClaim(previous, claim)) {
      throw new Error(`session "${session.id}" has a different live provenance claim; revoke it before reassignment`)
    }
    const revision = ++this.revision
    const evidence: VerifiedSessionProvenance = claim.kind === 'room'
      ? Object.freeze({ state: 'room_verified', sessionId: session.id, roomId: claim.roomId,
        orgId: claim.orgId, agent: claim.agent, source: claim.source,
        sourceVersion: claim.sourceVersion, revision, issuedAt: this.now() })
      : Object.freeze({ state: 'nonroom_verified', sessionId: session.id, scope: claim.scope,
        source: claim.source, sourceVersion: claim.sourceVersion, revision, issuedAt: this.now() })
    this.claims.set(session, evidence)
    return evidence
  }

  /** Read current authority, optionally requiring the exact revision previously observed. */
  attest(session: Session, expectedRevision?: number): SessionProvenance {
    if (!this.isLive(session)) return this.unknown(session, 'not_live')
    const current = this.claims.get(session)
    if (current === undefined) return this.unknown(session, 'not_attested')
    if (expectedRevision !== undefined && expectedRevision !== current.revision) return this.unknown(session, 'stale')
    if (this.options.ttlMs !== undefined && this.now() - current.issuedAt >= this.options.ttlMs) {
      this.claims.delete(session)
      return this.unknown(session, 'stale')
    }
    return current
  }

  /** Revoke the current claim. Future effects see unknown until a source reissues it. */
  revoke(session: Session): UnknownSessionProvenance {
    const revision = ++this.revision
    this.claims.delete(session)
    return Object.freeze({ state: 'unknown', sessionId: session.id, revision, reason: 'revoked' })
  }

  private unknown(session: Session, reason: UnknownSessionProvenance['reason']): UnknownSessionProvenance {
    return Object.freeze({ state: 'unknown', sessionId: session.id, revision: this.revision, reason })
  }

  private now(): number {
    return (this.options.now ?? (() => performance.now()))()
  }

  private sameClaim(previous: VerifiedSessionProvenance, claim: SessionProvenanceClaim): boolean {
    if (claim.kind === 'room') return previous.state === 'room_verified'
      && previous.source === claim.source && previous.sourceVersion === claim.sourceVersion
      && previous.roomId === claim.roomId && previous.orgId === claim.orgId && previous.agent === claim.agent
    return previous.state === 'nonroom_verified' && previous.source === claim.source && previous.sourceVersion === claim.sourceVersion
      && previous.scope.kind === claim.scope.kind
      && (previous.scope.kind === 'conversation' || (claim.scope.kind === 'workspace' && previous.scope.id === claim.scope.id))
  }
}
