import assert from 'node:assert/strict'
import test from 'node:test'
import { SessionProvenanceRegistry } from '../src/provenance.ts'
import { apiSessionControllerIssuer, dshRoomLeaseIssuer } from '../src/issuers.ts'

test('provenance is exact-object bound and ordinary API claims are per session', () => {
  const live = new Map()
  const registry = new SessionProvenanceRegistry(session => live.get(session.id) === session)
  const room = { id: 'room-session' }
  const ordinary = { id: 'ordinary-session' }
  live.set(room.id, room)
  live.set(ordinary.id, ordinary)

  const ordinaryProof = registry.issue(ordinary, apiSessionControllerIssuer, {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1',
    scope: { kind: 'conversation' },
  })

  assert.equal(registry.attest(room).state, 'unknown')
  assert.equal(registry.attest(ordinary).state, 'nonroom_verified')
  assert.equal(registry.attest({ id: 'room-session' }).reason, 'not_live')
  assert.equal(registry.attest(ordinary, ordinaryProof.revision + 1).reason, 'stale')
  assert.equal(ordinaryProof.sessionId, 'ordinary-session')
  const ordinaryClaim = {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1',
    scope: { kind: 'conversation' },
  }
  assert.throws(() => registry.issue(ordinary, Object.freeze({}), ordinaryClaim), /issuer is not authorized/)
  assert.throws(() => registry.issue(ordinary, ordinaryClaim), /issuer is not authorized/)
  assert.throws(() => registry.issue(ordinary, apiSessionControllerIssuer, {
    kind: 'room', source: '@myh/dsh-stanze', sourceVersion: '1', roomId: 8, orgId: '2', agent: 'cervello',
  }), /only attest ordinary conversations/)
  assert.throws(() => registry.issue(ordinary, apiSessionControllerIssuer, {
    kind: 'nonroom', source: 'any-plugin', sourceVersion: '1', scope: { kind: 'conversation' },
  }), /only attest ordinary conversations/)
  assert.throws(() => registry.issue(ordinary, apiSessionControllerIssuer, {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1',
    scope: { kind: 'workspace', id: 'different' },
  }), /revoke it before reassignment/)
  registry.revoke(ordinary)
  assert.equal(registry.issue(ordinary, apiSessionControllerIssuer, {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1',
    scope: { kind: 'conversation' },
  }).state, 'nonroom_verified')
})

test('restore and child sessions stay unknown without explicit server issuance', () => {
  const live = new Map()
  const registry = new SessionProvenanceRegistry(session => live.get(session.id) === session)
  const restored = { id: 'restored', header: { origin: 'restored', cwd: '/tmp' } }
  const child = { id: 'child', header: { origin: 'subagent', parentSession: 'parent' } }
  live.set(restored.id, restored)
  live.set(child.id, child)

  assert.equal(registry.attest(restored).state, 'unknown')
  assert.equal(registry.attest(child).state, 'unknown')
})

test('detached session and replacement object with the same id lose old provenance', () => {
  const live = new Map()
  const registry = new SessionProvenanceRegistry(session => live.get(session.id) === session)
  const detached = { id: 'reused-id' }
  live.set(detached.id, detached)
  registry.issue(detached, apiSessionControllerIssuer, {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1', scope: { kind: 'conversation' },
  })
  live.delete(detached.id)
  assert.equal(registry.attest(detached).reason, 'not_live')

  const replacement = { id: 'reused-id' }
  live.set(replacement.id, replacement)
  assert.equal(registry.attest(replacement).state, 'unknown')
})

test('revocation and TTL make prior proofs unknown', () => {
  const live = new Map()
  let now = 100
  const registry = new SessionProvenanceRegistry(session => live.get(session.id) === session, {
    now: () => now, ttlMs: 10,
  })
  const session = { id: 'live' }
  live.set(session.id, session)
  registry.issue(session, apiSessionControllerIssuer, {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1', scope: { kind: 'conversation' },
  })
  assert.equal(registry.attest(session).state, 'nonroom_verified')
  now = 110
  assert.equal(registry.attest(session).reason, 'stale')

  registry.issue(session, apiSessionControllerIssuer, {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1', scope: { kind: 'conversation' },
  })
  assert.equal(registry.revoke(session).reason, 'revoked')
  assert.equal(registry.attest(session).state, 'unknown')

  let noExpiryClock = 0
  const noExpiry = new SessionProvenanceRegistry(candidate => live.get(candidate.id) === candidate, {
    now: () => noExpiryClock,
  })
  noExpiry.issue(session, apiSessionControllerIssuer, {
    kind: 'nonroom', source: '@deepseek-ai/dsh-api-session-controller', sourceVersion: '1',
    scope: { kind: 'conversation' },
  })
  noExpiryClock = Number.MAX_SAFE_INTEGER
  assert.equal(noExpiry.attest(session).state, 'nonroom_verified')
})

test('ROOM claims require the ROOM lease issuer, bind exact identity, and revoke cleanly', () => {
  const live = new Map()
  let now = 5
  const registry = new SessionProvenanceRegistry(session => live.get(session.id) === session, { now: () => now, ttlMs: 10 })
  const session = { id: 'room-seat' }
  live.set(session.id, session)
  const claim = { kind: 'room', source: '@myh/dsh-stanze', sourceVersion: '1', roomId: 45, orgId: '7', agent: 'analyst' }

  assert.throws(() => registry.issue(session, apiSessionControllerIssuer, claim), /only attest ordinary/)
  const proof = registry.issue(session, dshRoomLeaseIssuer, claim)
  assert.throws(() => registry.issue(session, dshRoomLeaseIssuer, { ...claim, orgId: '8' }), /different live provenance/)
  assert.deepEqual(registry.attest(session), proof)
  assert.equal(registry.attest({ id: session.id }).reason, 'not_live')
  assert.equal(registry.attest(session, proof.revision + 1).reason, 'stale')
  now = 15
  assert.equal(registry.attest(session).reason, 'stale')
  assert.equal(registry.issue(session, dshRoomLeaseIssuer, claim).state, 'room_verified')
  assert.equal(registry.revoke(session).reason, 'revoked')
  assert.equal(registry.attest(session).state, 'unknown')
})

test('Sveglia provenance consumes a one-use receipt for the exact live AgentRegistry-created session', () => {
  const live = new Map()
  const receipts = new WeakMap()
  const registry = new SessionProvenanceRegistry(session => live.get(session.id) === session, {
    consumeAgentCreationReceipt(session, receipt) {
      if (receipts.get(receipt) !== session) return false
      receipts.delete(receipt)
      return true
    },
  })
  const created = { id: 'scheduled-session' }
  const existing = { id: 'already-existing-session' }
  live.set(created.id, created)
  live.set(existing.id, existing)

  const receipt = Object.freeze({})
  receipts.set(receipt, created)
  const proof = registry.issueSvegliaSession(created, receipt)
  assert.deepEqual(proof, {
    state: 'nonroom_verified', sessionId: created.id, scope: { kind: 'conversation' },
    source: '@myh/dsh-stanze/sveglia', sourceVersion: '1', revision: 1,
    issuedAt: proof.issuedAt,
  })
  assert.equal(registry.attest(created).state, 'nonroom_verified')
  assert.throws(() => registry.issueSvegliaSession(created, receipt), /exact newly created live session/)
  assert.throws(() => registry.issueSvegliaSession(existing, receipt), /exact newly created live session/)
  assert.throws(() => registry.issueSvegliaSession(existing, {}), /exact newly created live session/)
  assert.equal(registry.revoke(created).reason, 'revoked')
  assert.equal(registry.attest(created).state, 'unknown')

  const staleReceipt = Object.freeze({})
  receipts.set(staleReceipt, created)
  const replacement = { id: created.id }
  live.set(created.id, replacement)
  assert.throws(() => registry.issueSvegliaSession(created, staleReceipt), /exact newly created live session/)
  assert.equal(registry.attest(created).reason, 'not_live')
  assert.equal(registry.attest(replacement).state, 'unknown')
})
