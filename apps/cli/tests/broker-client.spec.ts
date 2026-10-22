import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { assignmentUnavailableClient } from '../src/broker-binding.ts'
import { BrokerControlClient, createBrokerClient, ReconnectingBrokerClient } from '../src/broker-client.ts'
import { broker, cleanups, UUID, type FakeServer } from './socket-broker.ts'

afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup() })

function client(server: FakeServer, options: ConstructorParameters<typeof BrokerControlClient>[1] = {}): BrokerControlClient {
  const instance = new BrokerControlClient(server.socketPath, options)
  cleanups.push(() => { instance.closeControl() })
  return instance
}

const bindRequest = { sessionId: 'session-1', workspaceId: 'workspace-1', kind: 'attach', cwd: '/srv/wt', orca: { instanceId: 'i-1' } } as const

describe('the control connection', () => {
  it('registers first, with exactly register-core, and adopts the integer epoch the broker returns', async () => {
    const server = await broker()
    const control = client(server)
    const hello = await server.next('register-core')
    expect(hello).toEqual({ type: 'register-core', version: 1, requestId: expect.stringMatching(UUID) as string })
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    expect(server.frames).toHaveLength(1)
  })

  it('sends every request over that one connection, each with its own UUIDv4 request id', async () => {
    const server = await broker()
    const control = client(server)
    const pending = [control.bind(bindRequest), control.revoke({ sessionId: 's', bindingId: 'b' }), control.status()]
    await server.next('status')
    expect(server.connections).toHaveLength(1)
    const ids = server.frames.map(frame => frame.requestId)
    expect(ids.every(id => UUID.test(id))).toBe(true)
    expect(new Set(ids).size).toBe(ids.length)
    control.closeControl()
    await Promise.all(pending)
  })

  it('holds requests made before registration completes and sends them only after', async () => {
    let release!: () => void
    const server = await broker({ register: (frame, self) => { release = () => { self.send({ type: 'registered', requestId: frame.requestId, revision: 3 }) } } })
    const control = client(server)
    const answer = control.bind(bindRequest)
    await server.next('register-core')
    await new Promise(resolve => setTimeout(resolve, 30))
    expect(server.frames.map(frame => frame.type)).toEqual(['register-core'])
    release()
    const bind = await server.next('bind')
    server.send({ type: 'bound', requestId: bind.requestId, bindingId: 'b0ff', revision: 1 })
    expect(await answer).toEqual({ ok: true, bindingId: 'b0ff', revision: 1 })
  })

  it('is refused, and sends nothing, when the broker answers the registration with an error', async () => {
    const server = await broker({ register: (frame, self) => { self.send({ type: 'error', code: 'control-active', message: 'control active', requestId: frame.requestId }) } })
    const control = client(server)
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'control-active' })
    expect(server.frames.map(frame => frame.type)).toEqual(['register-core'])
    expect(control.closed).toBe(true)
  })

  it('gives up when the broker never answers the registration', async () => {
    const server = await broker({ register: 'silent' })
    const control = client(server, { registerTimeoutMs: 60 })
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'broker-timeout' })
    await server.closed()
    expect(server.frames.map(frame => frame.type)).toEqual(['register-core'])
  })

  it('refuses everything when the socket does not exist, without throwing', async () => {
    const control = new BrokerControlClient(join(tmpdir(), 'dsh-no-such-broker.sock'))
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'control-unavailable' })
    expect(await control.status()).toEqual({ ok: false, reason: 'control-unavailable' })
  })
})

describe('requests and answers, with the frames the broker parses', () => {
  it('bind: the frame is the contract frame, and bound is translated into a grant', async () => {
    const server = await broker()
    const control = client(server)
    const answer = control.bind({ ...bindRequest, kind: 'room-seat', room: { origine: 'dsh', stanzaId: 'r-1', agente: 'a-1', installazione: 'x-1' } })
    const frame = await server.next('bind')
    expect(frame).toEqual({
      type: 'bind', version: 1, requestId: expect.stringMatching(UUID) as string, sessionId: 'session-1', workspaceId: 'workspace-1',
      kind: 'room-seat', cwd: '/srv/wt', orca: { instanceId: 'i-1' },
      room: { origin: 'dsh', roomId: 'r-1', agentId: 'a-1', installationId: 'x-1' },
    })
    server.send({ type: 'bound', requestId: frame.requestId, bindingId: 'b0ff', revision: 4 })
    expect(await answer).toEqual({ ok: true, bindingId: 'b0ff', revision: 4 })
  })

  it('revoke: {bindingId}, answered by revoked', async () => {
    const server = await broker()
    const control = client(server)
    const answer = control.revoke({ sessionId: 'session-1', bindingId: 'b0ff' })
    const frame = await server.next('revoke')
    expect(frame).toEqual({ type: 'revoke', version: 1, requestId: expect.stringMatching(UUID) as string, bindingId: 'b0ff' })
    server.send({ type: 'revoked', requestId: frame.requestId, bindingId: 'b0ff', count: 1 })
    expect(await answer).toEqual({ ok: true })
  })

  it('revoke: an ack that names another binding is not an ack', async () => {
    const server = await broker()
    const control = client(server)
    const answer = control.revoke({ sessionId: 's', bindingId: 'b0ff' })
    const frame = await server.next('revoke')
    server.send({ type: 'revoked', requestId: frame.requestId, bindingId: 'other', count: 1 })
    expect(await answer).toEqual({ ok: false, reason: 'broker-malformed' })
  })

  it('lease: {bindingId, revision, mode: "process"}, answered by leased', async () => {
    const server = await broker()
    const control = client(server)
    const answer = control.assign({ sessionId: 'session-1', bindingId: 'b0ff', revision: 4 })
    const frame = await server.next('lease')
    expect(frame).toEqual({ type: 'lease', version: 1, requestId: expect.stringMatching(UUID) as string, bindingId: 'b0ff', revision: 4, mode: 'process' })
    server.send({ type: 'leased', requestId: frame.requestId, assignmentId: 'opaque-1', revision: 4, ttlMs: 5000 })
    expect(await answer).toEqual({ ok: true, assignmentId: 'opaque-1', revision: 4 })
  })

  it('status: relays the broker read-back', async () => {
    const server = await broker()
    const control = client(server)
    const answer = control.status()
    const frame = await server.next('status')
    expect(frame).toEqual({ type: 'status', version: 1, requestId: expect.stringMatching(UUID) as string })
    server.send({ type: 'status', requestId: frame.requestId, bindings: 2, leases: 1, undrained: 0, epoch: 7, workers: [] })
    expect(await answer).toEqual({ ok: true, status: { bindings: 2, leases: 1, undrained: 0, epoch: 7, workers: [] } })
  })

  it('correlates by request id: answers may arrive in any order', async () => {
    const server = await broker()
    const control = client(server)
    const first = control.bind({ ...bindRequest, sessionId: 'one' })
    const second = control.bind({ ...bindRequest, sessionId: 'two' })
    const a = await server.next('bind', 1)
    const b = await server.next('bind', 2)
    server.send({ type: 'bound', requestId: b.requestId, bindingId: 'for-two', revision: 2 })
    server.send({ type: 'bound', requestId: a.requestId, bindingId: 'for-one', revision: 1 })
    expect(await first).toEqual({ ok: true, bindingId: 'for-one', revision: 1 })
    expect(await second).toEqual({ ok: true, bindingId: 'for-two', revision: 2 })
  })

  it('turns an error frame with a request id into a refusal with the broker reason and keeps the connection', async () => {
    const server = await broker()
    const control = client(server)
    const refused = control.bind(bindRequest)
    const frame = await server.next('bind')
    server.send({ type: 'error', code: 'bind-refused', message: 'bind refused', requestId: frame.requestId, reason: 'cwd-mismatch' })
    expect(await refused).toEqual({ ok: false, reason: 'cwd-mismatch' })
    expect(control.closed).toBe(false)
    const next = control.status()
    const status = await server.next('status')
    server.send({ type: 'status', requestId: status.requestId, bindings: 0, leases: 0, undrained: 0, epoch: 7, workers: [] })
    expect(await next).toMatchObject({ ok: true })
  })

  it('refuses locally, without sending, a bind frame the broker would reject by size', async () => {
    const server = await broker()
    const control = client(server)
    expect(await control.bind({ ...bindRequest, cwd: `/${'a'.repeat(9000)}` })).toEqual({ ok: false, reason: 'invalid-request' })
    expect(server.frames.filter(frame => frame.type === 'bind')).toHaveLength(0)
  })
})

describe('start and worker-exited', () => {
  async function leased(server: FakeServer, control: BrokerControlClient, bindingId = 'b0ff'): Promise<string> {
    const answer = control.assign({ sessionId: 'session-1', bindingId, revision: 1 })
    const frame = await server.next('lease', server.frames.filter(f => f.type === 'lease').length + 1)
    server.send({ type: 'leased', requestId: frame.requestId, assignmentId: 'opaque-1', revision: 1, ttlMs: 5000 })
    const grant = await answer
    if (!grant.ok) throw new Error('lease refused')
    return grant.assignmentId
  }
  const startRequest = (assignmentId: string) => ({ assignmentId, argv: ['/bin/echo', 'hi'], cwd: '/srv/wt', env: { A: 'b' }, graceMs: 200 })

  it('start: the contract frame with ignore stdio, answered by started; the exit comes from worker-exited', async () => {
    const server = await broker()
    const control = client(server)
    const assignmentId = await leased(server, control)
    const started = control.start(startRequest(assignmentId))
    const frame = await server.next('start')
    expect(frame).toEqual({
      type: 'start', version: 1, requestId: expect.stringMatching(UUID) as string, mode: 'process', assignmentId: 'opaque-1',
      argv: ['/bin/echo', 'hi'], cwd: '/srv/wt', env: { A: 'b' },
      stdio: { stdin: { mode: 'ignore' }, stdout: { mode: 'ignore' }, stderr: { mode: 'ignore' } }, graceMs: 200,
    })
    server.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' })
    const grant = await started
    expect(grant).toMatchObject({ ok: true, pid: 4321, workerId: 'w1' })
    if (!grant.ok) return
    let settled = false
    void grant.exited.then(() => { settled = true })
    await new Promise(resolve => setTimeout(resolve, 20))
    expect(settled).toBe(false)
    server.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ff', code: 3, signal: null })
    expect(await grant.exited).toEqual({ ok: true, code: 3, signal: null })
  })

  it('keeps an exit that arrives before the started frame that names it', async () => {
    const server = await broker()
    const control = client(server)
    const assignmentId = await leased(server, control)
    const started = control.start(startRequest(assignmentId))
    const frame = await server.next('start')
    server.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ff', code: 0, signal: null })
    server.send({ type: 'started', requestId: frame.requestId, pid: 4321, workerId: 'w1' })
    const grant = await started
    expect(grant.ok && await grant.exited).toEqual({ ok: true, code: 0, signal: null })
  })

  it('reports a signal death by name', async () => {
    const server = await broker()
    const control = client(server)
    const assignmentId = await leased(server, control)
    const started = control.start(startRequest(assignmentId))
    const frame = await server.next('start')
    server.send({ type: 'started', requestId: frame.requestId, pid: 5, workerId: 'w9' })
    const grant = await started
    server.send({ type: 'worker-exited', workerId: 'w9', bindingId: 'b0ff', code: null, signal: 9 })
    expect(grant.ok && await grant.exited).toEqual({ ok: true, code: null, signal: 'SIGKILL' })
  })

  it('treats an exit that names another binding than the lease as a violation: the worker is unknown and the connection ends', async () => {
    const server = await broker()
    const control = client(server)
    const assignmentId = await leased(server, control)
    const started = control.start(startRequest(assignmentId))
    const frame = await server.next('start')
    server.send({ type: 'started', requestId: frame.requestId, pid: 5, workerId: 'w9' })
    const grant = await started
    server.send({ type: 'worker-exited', workerId: 'w9', bindingId: 'someone-else', code: 0, signal: null })
    expect(grant.ok && await grant.exited).toEqual({ ok: false, reason: 'broker-malformed' })
    await server.closed()
    expect(control.closed).toBe(true)
  })

  it('settles every live worker as lost when the connection ends, and refuses later calls', async () => {
    const server = await broker()
    const control = client(server)
    const assignmentId = await leased(server, control)
    const started = control.start(startRequest(assignmentId))
    const frame = await server.next('start')
    server.send({ type: 'started', requestId: frame.requestId, pid: 5, workerId: 'w9' })
    const grant = await started
    server.peer().destroy()
    expect(grant.ok && await grant.exited).toEqual({ ok: false, reason: 'control-closed' })
    expect(await control.status()).toEqual({ ok: false, reason: 'control-closed' })
  })

  it('relays a start refusal, and refuses a start the broker would reject by shape without sending it', async () => {
    const server = await broker()
    const control = client(server)
    const assignmentId = await leased(server, control)
    const refused = control.start(startRequest(assignmentId))
    const frame = await server.next('start')
    server.send({ type: 'error', code: 'start-refused', message: 'start refused', requestId: frame.requestId, reason: 'stdio-unsupported' })
    expect(await refused).toEqual({ ok: false, reason: 'stdio-unsupported' })
    expect(await control.start({ ...startRequest(assignmentId), env: { DSH_SESSION_ID: 'x' } })).toMatchObject({ ok: false, reason: 'invalid-env' })
    expect(server.frames.filter(f => f.type === 'start')).toHaveLength(1)
  })

  it('gives start its own, longer timeout', async () => {
    const server = await broker()
    const control = client(server, { requestTimeoutMs: 40, startTimeoutMs: 400 })
    const assignmentId = await leased(server, control)
    const started = control.start(startRequest(assignmentId))
    const frame = await server.next('start')
    await new Promise(resolve => setTimeout(resolve, 120))
    expect(control.closed).toBe(false)
    server.send({ type: 'started', requestId: frame.requestId, pid: 5, workerId: 'w9' })
    expect(await started).toMatchObject({ ok: true })
  })
})

describe('fail closed', () => {
  it('drops the connection and refuses the request when the broker is silent', async () => {
    const server = await broker()
    const control = client(server, { requestTimeoutMs: 60 })
    expect(await control.revoke({ sessionId: 's', bindingId: 'b' })).toEqual({ ok: false, reason: 'broker-timeout' })
    await server.closed()
    expect(control.closed).toBe(true)
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'broker-timeout' })
  })

  it.each([
    ['an unparseable line', 'this is not json\n'],
    ['an unknown frame type', `${JSON.stringify({ type: 'surprise', requestId: 'x' })}\n`],
    ['a worker-exited that names no known worker, repeated past the early-exit bound', null],
    ['an answer that matches no request', `${JSON.stringify({ type: 'bound', requestId: '3f2b8f0e-9c1d-4b57-8a43-1d2e3f4a5b6c', bindingId: 'b', revision: 1 })}\n`],
    ['an error without a request id', `${JSON.stringify({ type: 'error', code: 'invalid-frame', message: 'invalid start frame' })}\n`],
    ['a second registered frame', `${JSON.stringify({ type: 'registered', requestId: '3f2b8f0e-9c1d-4b57-8a43-1d2e3f4a5b6c', revision: 8 })}\n`],
  ])('closes the connection on %s, and refuses what was pending', async (_name, line) => {
    const server = await broker()
    const control = client(server)
    const pending = control.status()
    await server.next('status')
    if (line === null) {
      for (let i = 0; i < 70; i++) server.send({ type: 'worker-exited', workerId: `w${i}`, bindingId: 'b', code: 0, signal: null })
    } else server.peer().write(line)
    const answer = await pending
    expect(answer.ok).toBe(false)
    await server.closed()
    expect(control.closed).toBe(true)
  })

  it('names the broker code when it ends the connection with an error that has no request id', async () => {
    const server = await broker()
    const control = client(server)
    const pending = control.status()
    await server.next('status')
    server.send({ type: 'error', code: 'peer-denied', message: 'peer denied' })
    expect(await pending).toEqual({ ok: false, reason: 'peer-denied' })
  })

  it('refuses an answer of the wrong kind for the request, and closes', async () => {
    const server = await broker()
    const control = client(server)
    const pending = control.revoke({ sessionId: 's', bindingId: 'b0ff' })
    const frame = await server.next('revoke')
    server.send({ type: 'bound', requestId: frame.requestId, bindingId: 'b0ff', revision: 1 })
    expect(await pending).toEqual({ ok: false, reason: 'broker-malformed' })
    await server.closed()
  })

  it('refuses an answer with an unknown field, and never turns it into a grant', async () => {
    const server = await broker()
    const control = client(server)
    const pending = control.bind(bindRequest)
    const frame = await server.next('bind')
    server.send({ type: 'bound', requestId: frame.requestId, bindingId: 'b0ff', revision: 1, ok: true })
    expect(await pending).toEqual({ ok: false, reason: 'broker-malformed' })
  })

  it('closeControl drops the connection, refuses what is pending, and is idempotent', async () => {
    const server = await broker()
    const control = client(server)
    const pending = control.status()
    await server.next('status')
    control.closeControl()
    control.closeControl()
    expect(await pending).toEqual({ ok: false, reason: 'control-closed' })
    await server.closed()
  })
})

describe('which client Core runs (S6 contract 10)', () => {
  it('is the deny client when no broker socket is configured', async () => {
    expect(createBrokerClient(undefined)).toBe(assignmentUnavailableClient)
    expect(createBrokerClient({ socketPath: '' })).toBe(assignmentUnavailableClient)
    expect(await createBrokerClient(undefined).bind(bindRequest)).toEqual({ ok: false, reason: 'assignment-unavailable' })
  })

  it('is the real client, connected to that socket and to no other, when one is configured', async () => {
    const server = await broker()
    // The test server's socket belongs to the test user, not to root.
    const real = createBrokerClient({ socketPath: server.socketPath }, {}, {}, { ownerUid: process.getuid!() })
    cleanups.push(() => { real.closeControl() })
    expect(real).toBeInstanceOf(ReconnectingBrokerClient)
    expect((await server.next('register-core')).type).toBe('register-core')
  })
})
