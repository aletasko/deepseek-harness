/**
 * S6a-fix, client side: the `signal` frame (contract 11), `version` on answers (F3), a second `registered` (F6),
 * a connection that ends (listeners, bounded reconnect, no hang) and the check of the socket path (F9).
 */
import { chmodSync, mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { BrokerControlClient, createBrokerClient, ReconnectingBrokerClient, socketDistrust, type ReconnectOptions } from '../src/broker-client.ts'
import { broker, cleanups, UUID, type FakeServer, type Frame } from './socket-broker.ts'

afterEach(async () => {
  vi.restoreAllMocks()
  for (const cleanup of cleanups.splice(0)) await cleanup()
})

const SELF = process.getuid!()
const bindRequest = { sessionId: 'session-1', workspaceId: 'workspace-1', kind: 'attach', cwd: '/srv/wt', orca: { instanceId: 'i-1' } } as const

function client(server: FakeServer, options: ConstructorParameters<typeof BrokerControlClient>[1] = {}): BrokerControlClient {
  const instance = new BrokerControlClient(server.socketPath, options)
  cleanups.push(() => { instance.closeControl() })
  return instance
}

/** A lease, a start and its `started`, so there is a worker `w1` of binding `b0ff` to signal. */
async function startedWorker(server: FakeServer, control: BrokerControlClient, workerId = 'w1'): Promise<{ exited: Promise<unknown> }> {
  const lease = control.assign({ sessionId: 'session-1', bindingId: 'b0ff', revision: 1 })
  const leaseFrame = await server.next('lease', server.frames.filter(f => f.type === 'lease').length + 1)
  server.send({ type: 'leased', requestId: leaseFrame.requestId, assignmentId: 'opaque-1', revision: 1 })
  const granted = await lease
  if (!granted.ok) throw new Error('lease refused')
  const started = control.start({ assignmentId: granted.assignmentId, argv: ['/bin/sleep', '9'], cwd: '/srv/wt', env: {}, graceMs: 100 })
  const startFrame = await server.next('start', server.frames.filter(f => f.type === 'start').length + 1)
  server.send({ type: 'started', requestId: startFrame.requestId, pid: 9, workerId })
  const worker = await started
  if (!worker.ok) throw new Error('start refused')
  return { exited: worker.exited }
}

describe('signal (S6 contract 11)', () => {
  it('sends {workerId, signal} and resolves when the broker answers signalled', async () => {
    const server = await broker()
    const control = client(server)
    await startedWorker(server, control)
    const answer = control.signal('w1', 'TERM')
    const frame = await server.next('signal')
    expect(frame).toEqual({ type: 'signal', version: 1, requestId: expect.stringMatching(UUID) as string, workerId: 'w1', signal: 'TERM' })
    server.send({ type: 'signalled', version: 1, requestId: frame.requestId, workerId: 'w1' })
    expect(await answer).toEqual({ ok: true })
    const kill = control.signal('w1', 'KILL')
    expect((await server.next('signal', 2)).signal).toBe('KILL')
    control.closeControl()
    await kill
  })

  it('relays a not-found refusal and keeps the connection', async () => {
    const server = await broker()
    const control = client(server)
    await startedWorker(server, control)
    const answer = control.signal('w1', 'TERM')
    const frame = await server.next('signal')
    server.send({ type: 'error', version: 1, code: 'not-found', reason: 'unknown-worker', message: 'not found', requestId: frame.requestId })
    expect(await answer).toEqual({ ok: false, reason: 'unknown-worker' })
    expect(control.closed).toBe(false)
  })

  it('sends nothing for a worker this connection did not start, or that already exited', async () => {
    const server = await broker()
    const control = client(server)
    const { exited } = await startedWorker(server, control)
    expect(await control.signal('someone-elses', 'KILL')).toEqual({ ok: false, reason: 'not-found' })
    server.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ff', code: 0, signal: null })
    await exited
    expect(await control.signal('w1', 'TERM')).toEqual({ ok: false, reason: 'not-found' })
    expect(server.frames.filter(frame => frame.type === 'signal')).toHaveLength(0)
  })

  it('treats an ack for another worker as a violation: the connection ends', async () => {
    const server = await broker()
    const control = client(server)
    await startedWorker(server, control)
    const answer = control.signal('w1', 'TERM')
    const frame = await server.next('signal')
    server.send({ type: 'signalled', requestId: frame.requestId, workerId: 'w2' })
    expect(await answer).toEqual({ ok: false, reason: 'broker-malformed' })
    await server.closed()
    expect(control.closed).toBe(true)
  })

  it('a signalled answer to a request that was not a signal is a violation', async () => {
    const server = await broker()
    const control = client(server)
    const pending = control.status()
    const frame = await server.next('status')
    server.send({ type: 'signalled', requestId: frame.requestId, workerId: 'w1' })
    expect(await pending).toEqual({ ok: false, reason: 'broker-malformed' })
  })

  it('worker-exited still settles the worker after a signal', async () => {
    const server = await broker()
    const control = client(server)
    const lease = control.assign({ sessionId: 'session-1', bindingId: 'b0ff', revision: 1 })
    const leaseFrame = await server.next('lease')
    server.send({ type: 'leased', requestId: leaseFrame.requestId, assignmentId: 'opaque-1', revision: 1 })
    const granted = await lease
    if (!granted.ok) throw new Error('lease refused')
    const started = control.start({ assignmentId: granted.assignmentId, argv: ['/bin/sleep', '9'], cwd: '/srv/wt', env: {}, graceMs: 100 })
    const startFrame = await server.next('start')
    server.send({ type: 'started', requestId: startFrame.requestId, pid: 9, workerId: 'w1' })
    const worker = await started
    if (!worker.ok) throw new Error('start refused')
    const signalled = control.signal('w1', 'KILL')
    const frame = await server.next('signal')
    server.send({ type: 'signalled', requestId: frame.requestId, workerId: 'w1' })
    server.send({ type: 'worker-exited', workerId: 'w1', bindingId: 'b0ff', code: null, signal: 'SIGKILL' })
    expect(await signalled).toEqual({ ok: true })
    expect(await worker.exited).toEqual({ ok: true, code: null, signal: 'SIGKILL' })
  })
})

describe('version on answers (F3)', () => {
  it('accepts version 1 on an answer, and an answer without it', async () => {
    const server = await broker()
    const control = client(server)
    const first = control.bind(bindRequest)
    const a = await server.next('bind')
    server.send({ type: 'bound', version: 1, requestId: a.requestId, bindingId: 'b1', revision: 1 })
    expect(await first).toEqual({ ok: true, bindingId: 'b1', revision: 1 })
    const second = control.bind({ ...bindRequest, sessionId: 'session-2' })
    const b = await server.next('bind', 2)
    server.send({ type: 'bound', requestId: b.requestId, bindingId: 'b2', revision: 2 })
    expect(await second).toEqual({ ok: true, bindingId: 'b2', revision: 2 })
  })

  it('accepts a registered frame that carries version 1', async () => {
    const server = await broker({ register: (frame, self) => { self.send({ type: 'registered', version: 1, requestId: frame.requestId, revision: 5 }) } })
    const control = client(server)
    await vi.waitFor(() => { expect(control.epoch).toBe(5) })
  })

  it('refuses an answer of another version and ends the connection', async () => {
    const server = await broker()
    const control = client(server)
    const pending = control.bind(bindRequest)
    const frame = await server.next('bind')
    server.send({ type: 'bound', version: 2, requestId: frame.requestId, bindingId: 'b1', revision: 1 })
    expect(await pending).toMatchObject({ ok: false })
    await server.closed()
    expect(control.closed).toBe(true)
  })
})

describe('a second registered frame (F6)', () => {
  it('with the registration request id ends the connection and does not change the epoch', async () => {
    const server = await broker()
    const control = client(server)
    const hello = await server.next('register-core')
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    const pending = control.status()
    await server.next('status')
    server.send({ type: 'registered', requestId: hello.requestId, revision: 8 })
    expect(await pending).toMatchObject({ ok: false })
    await server.closed()
    expect(control.epoch).toBe(7)
    expect(control.closed).toBe(true)
  })
})

describe('a connection that ends (listeners, no hang)', () => {
  it('tells every listener once, with the reason, when the broker drops the connection', async () => {
    const server = await broker()
    const control = client(server)
    await server.next('register-core')
    const heard: string[] = []
    control.onClosed(reason => heard.push(reason))
    control.onClosed(() => { throw new Error('a listener that throws') })
    control.onClosed(reason => heard.push(`${reason}!`))
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    server.peer().destroy()
    await vi.waitFor(() => { expect(heard).toEqual(['control-closed', 'control-closed!']) })
    control.closeControl()
    expect(heard).toHaveLength(2)
  })

  it('tells a listener added after the connection ended', async () => {
    const server = await broker()
    const control = client(server)
    control.closeControl()
    const heard: string[] = []
    control.onClosed(reason => heard.push(reason))
    await vi.waitFor(() => { expect(heard).toEqual(['control-closed']) })
  })

  it('rejects the calls pending on a lost connection instead of leaving them waiting', async () => {
    const server = await broker()
    const control = client(server)
    const pending = [control.status(), control.bind(bindRequest), control.revoke({ sessionId: 's', bindingId: 'b' })]
    await server.next('revoke')
    server.peer().destroy()
    expect(await Promise.all(pending)).toEqual(Array.from({ length: 3 }, () => ({ ok: false, reason: 'control-closed' })))
  })
})

/** Wait until the n-th connection to the server has registered. */
async function connected(server: FakeServer, n: number): Promise<void> {
  await vi.waitFor(() => {
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(n)
  }, { timeout: 5000 })
}

function reconnecting(
  server: FakeServer,
  reconnect: ReconnectOptions = { baseDelayMs: 10 },
  options: ConstructorParameters<typeof BrokerControlClient>[1] = {},
): ReconnectingBrokerClient {
  const instance = new ReconnectingBrokerClient(server.socketPath, options, reconnect, { ownerUid: SELF })
  cleanups.push(() => { instance.closeControl() })
  return instance
}

describe('bounded reconnect', () => {
  it('tells the listeners at once, refuses meanwhile, then registers again as a new epoch', async () => {
    let epoch = 6
    const server = await broker({ register: (frame, self) => { epoch += 1; self.send({ type: 'registered', requestId: frame.requestId, revision: epoch }) } })
    const control = reconnecting(server, { baseDelayMs: 60 })
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    const heard: string[] = []
    control.onClosed(reason => heard.push(reason))
    server.peer().destroy()
    await vi.waitFor(() => { expect(heard).toEqual(['control-closed']) })
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'control-reconnecting' })
    expect(await control.signal('w1', 'KILL')).toEqual({ ok: false, reason: 'control-reconnecting' })
    await connected(server, 2)
    await vi.waitFor(() => { expect(control.epoch).toBe(8) })
    const answer = control.bind(bindRequest)
    const frame = await server.next('bind')
    expect(server.connections).toHaveLength(2)
    server.send({ type: 'bound', requestId: frame.requestId, bindingId: 'fresh', revision: 1 })
    expect(await answer).toEqual({ ok: true, bindingId: 'fresh', revision: 1 })
    // One loss is one notification; the successful reconnect did not announce another.
    expect(heard).toEqual(['control-closed'])
  })

  it('settles a worker of the lost connection as lost, never as exited', async () => {
    const server = await broker()
    const control = reconnecting(server)
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    const lease = control.assign({ sessionId: 'session-1', bindingId: 'b0ff', revision: 1 })
    const leaseFrame = await server.next('lease')
    server.send({ type: 'leased', requestId: leaseFrame.requestId, assignmentId: 'opaque-1', revision: 1 })
    const granted = await lease
    if (!granted.ok) throw new Error('lease refused')
    const started = control.start({ assignmentId: granted.assignmentId, argv: ['/bin/sleep', '9'], cwd: '/srv/wt', env: {}, graceMs: 100 })
    const startFrame = await server.next('start')
    server.send({ type: 'started', requestId: startFrame.requestId, pid: 9, workerId: 'w1' })
    const worker = await started
    if (!worker.ok) throw new Error('start refused')
    server.peer().destroy()
    expect(await worker.exited).toEqual({ ok: false, reason: 'control-closed' })
  })

  it('gives up after the bounded attempts and then refuses for good', async () => {
    // A broker that accepts and hangs up each time: the attempts register nothing.
    const server = await broker({ register: (_frame, self) => { self.peer().destroy() } })
    const control = reconnecting(server, { baseDelayMs: 10, attempts: 2 })
    await vi.waitFor(() => { expect(control.closed).toBe(true) }, { timeout: 5000 })
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(3)
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'control-closed' })
    await new Promise(resolve => setTimeout(resolve, 80))
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(3)
  })

  it('does not reconnect when attempts is 0', async () => {
    const server = await broker()
    const control = reconnecting(server, { baseDelayMs: 10, attempts: 0 })
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    server.peer().destroy()
    await vi.waitFor(() => { expect(control.closed).toBe(true) })
    await new Promise(resolve => setTimeout(resolve, 60))
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(1)
  })

  it('does not reconnect after a close Core asked for', async () => {
    const server = await broker()
    const control = reconnecting(server)
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    const heard: string[] = []
    control.onClosed(reason => heard.push(reason))
    control.closeControl()
    control.closeControl()
    await server.closed()
    await new Promise(resolve => setTimeout(resolve, 80))
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(1)
    expect(heard).toEqual(['control-closed'])
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'control-closed' })
  })

  it('does not reconnect after a protocol violation: a broker Core cannot understand is not retried', async () => {
    const server = await broker()
    const control = reconnecting(server)
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    const heard: string[] = []
    control.onClosed(reason => heard.push(reason))
    server.peer().write('this is not json\n')
    await vi.waitFor(() => { expect(control.closed).toBe(true) })
    await new Promise(resolve => setTimeout(resolve, 80))
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(1)
    expect(heard).toEqual(['broker-malformed'])
  })

  it('does not reconnect after a silent broker timed a request out', async () => {
    const server = await broker()
    const control = reconnecting(server, { baseDelayMs: 10 }, { requestTimeoutMs: 50 })
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    expect(await control.status()).toEqual({ ok: false, reason: 'broker-timeout' })
    await vi.waitFor(() => { expect(control.closed).toBe(true) })
    await new Promise(resolve => setTimeout(resolve, 80))
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(1)
  })

  it('keeps trying, within its bound, while the broker is not there yet', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'dsh-late-'))
    const socketPath = join(directory, 'late.sock')
    cleanups.push(() => { rmSync(directory, { recursive: true, force: true }) })
    const control = new ReconnectingBrokerClient(socketPath, {}, { baseDelayMs: 40, attempts: 5 }, { ownerUid: SELF })
    cleanups.push(() => { control.closeControl() })
    const heard: string[] = []
    control.onClosed(reason => heard.push(reason))
    await new Promise(resolve => setTimeout(resolve, 20))
    const seen: Frame[] = []
    const listener = createServer((socket) => {
      socket.setEncoding('utf8')
      socket.on('data', (chunk: string) => {
        const frame = JSON.parse(chunk.split('\n')[0]!) as Frame
        seen.push(frame)
        socket.write(`${JSON.stringify({ type: 'registered', requestId: frame.requestId, revision: 4 })}\n`)
      })
      socket.on('error', () => {})
    })
    listener.listen(socketPath)
    cleanups.push(() => new Promise<void>((resolve) => { listener.close(() => { resolve() }) }))
    // Until it is chmod-ed the socket is open to others, which the client refuses and retries.
    await new Promise<void>(resolve => listener.once('listening', () => { resolve() }))
    chmodSync(socketPath, 0o660)
    await vi.waitFor(() => { expect(control.epoch).toBe(4) }, { timeout: 5000 })
    expect(seen).toHaveLength(1)
    // Nothing had been granted on the connection that never registered, so nobody needed telling.
    expect(heard).toEqual([])
  })
})

describe('bounded reconnect, state kept between attempts', () => {
  it('refuses calls while a replacement connection has not registered yet, and sends nothing on it', async () => {
    let connections = 0
    const server = await broker({ register: (frame, self) => {
      connections += 1
      if (connections === 1) self.send({ type: 'registered', requestId: frame.requestId, revision: 7 })
    } })
    const control = reconnecting(server, { baseDelayMs: 10 }, { registerTimeoutMs: 5000 })
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    server.peer().destroy()
    await connected(server, 2)
    expect(await control.bind(bindRequest)).toEqual({ ok: false, reason: 'control-reconnecting' })
    expect(await control.status()).toEqual({ ok: false, reason: 'control-reconnecting' })
    expect(server.frames.map(frame => frame.type)).toEqual(['register-core', 'register-core'])
  })

  it('counts the attempts again after each successful reconnect', async () => {
    const server = await broker()
    const control = reconnecting(server, { baseDelayMs: 10, attempts: 1 })
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    server.peer().destroy()
    await connected(server, 2)
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    server.peer().destroy()
    await connected(server, 3)
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    expect(control.closed).toBe(false)
  })

  it('connects once a socket that was open to others has been fixed, instead of giving up on it', async () => {
    const server = await broker()
    chmodSync(server.socketPath, 0o666)
    const control = reconnecting(server, { baseDelayMs: 40, attempts: 6 })
    cleanups.push(() => { control.closeControl() })
    await new Promise(resolve => setTimeout(resolve, 60))
    expect(server.connections).toHaveLength(0)
    expect(await control.bind(bindRequest)).toMatchObject({ ok: false, reason: 'untrusted-socket' })
    chmodSync(server.socketPath, 0o660)
    await vi.waitFor(() => { expect(control.epoch).toBe(7) }, { timeout: 5000 })
  })
})

describe('the socket path is checked before every connection (F9)', () => {
  function scratch(): string {
    const directory = mkdtempSync(join(tmpdir(), 'dsh-trust-'))
    cleanups.push(() => { rmSync(directory, { recursive: true, force: true }) })
    return directory
  }
  const problem = (verdict: ReturnType<typeof socketDistrust>): string => 'problem' in verdict ? verdict.problem : ''

  /** A directory with a real listening socket in it. */
  async function withSocket(mode = 0o660): Promise<{ directory: string; socketPath: string }> {
    const directory = scratch()
    const socketPath = join(directory, 'broker.sock')
    const listener = createServer(() => {})
    listener.listen(socketPath)
    await new Promise<void>(resolve => listener.once('listening', () => { resolve() }))
    cleanups.push(() => new Promise<void>((resolve) => { listener.close(() => { resolve() }) }))
    chmodSync(socketPath, mode)
    return { directory, socketPath }
  }

  it('trusts a socket in a directory that belongs to the owner and is closed to group and others', async () => {
    const { socketPath } = await withSocket()
    expect(socketDistrust(socketPath, { ownerUid: SELF })).toEqual({ path: expect.stringContaining('broker.sock') as string })
  })

  it('does not distrust a socket that is not there yet', () => {
    const directory = scratch()
    expect(socketDistrust(join(directory, 'later.sock'), { ownerUid: SELF })).toEqual({ path: join(directory, 'later.sock') })
    expect(socketDistrust(join(directory, 'no', 'such', 'later.sock'), { ownerUid: SELF })).toEqual({ path: join(directory, 'no', 'such', 'later.sock') })
  })

  it('refuses a directory and a socket that belong to someone other than the expected owner', async () => {
    const { socketPath } = await withSocket()
    expect(problem(socketDistrust(socketPath, { ownerUid: SELF + 1 }))).toMatch(/directory .* is not owned by uid/u)
  })

  it.each([
    ['group', 0o770],
    ['others', 0o707],
    ['group and others', 0o777],
  ])('refuses a directory writable by %s', async (_who, mode) => {
    const { directory, socketPath } = await withSocket()
    chmodSync(directory, mode)
    expect(problem(socketDistrust(socketPath, { ownerUid: SELF }))).toMatch(/writable by its group or by others/u)
  })

  it('refuses a world-writable directory above, but accepts one with the sticky bit (like /tmp)', async () => {
    const outer = scratch()
    const inner = join(outer, 'run')
    mkdirSync(inner, { mode: 0o700 })
    chmodSync(outer, 0o777)
    expect(problem(socketDistrust(join(inner, 'broker.sock'), { ownerUid: SELF }))).toMatch(/above the broker socket, is writable/u)
    chmodSync(outer, 0o1777)
    expect(socketDistrust(join(inner, 'broker.sock'), { ownerUid: SELF })).toHaveProperty('path')
  })

  it('refuses a path that is a regular file, or a symlink, instead of a socket', () => {
    const directory = scratch()
    writeFileSync(join(directory, 'file.sock'), '')
    expect(problem(socketDistrust(join(directory, 'file.sock'), { ownerUid: SELF }))).toMatch(/is not a Unix socket/u)
    symlinkSync(join(directory, 'file.sock'), join(directory, 'link.sock'))
    expect(problem(socketDistrust(join(directory, 'link.sock'), { ownerUid: SELF }))).toMatch(/is not a Unix socket/u)
  })

  it('refuses a socket that others can reach', async () => {
    const { socketPath } = await withSocket(0o666)
    expect(problem(socketDistrust(socketPath, { ownerUid: SELF }))).toMatch(/accessible to others/u)
  })

  it('refuses a socket whose group Core is not in, unless Core is root', async () => {
    const { socketPath } = await withSocket()
    vi.spyOn(process, 'getegid').mockReturnValue(424_242)
    vi.spyOn(process, 'getgid').mockReturnValue(424_243)
    vi.spyOn(process, 'getgroups').mockReturnValue([424_244])
    vi.spyOn(process, 'geteuid').mockReturnValue(SELF === 0 ? 1 : SELF)
    expect(problem(socketDistrust(socketPath, { ownerUid: SELF })))
      .toMatch(/group of the broker socket .* not one this process belongs to/u)
    vi.spyOn(process, 'geteuid').mockReturnValue(0)
    expect(socketDistrust(socketPath, { ownerUid: SELF })).toHaveProperty('path')
  })

  it('connects to the resolved path, so a symlink in the way cannot be swapped after the check', async () => {
    const { directory, socketPath } = await withSocket()
    const holder = scratch()
    const alias = join(holder, 'alias')
    symlinkSync(directory, alias)
    const verdict = socketDistrust(join(alias, 'broker.sock'), { ownerUid: SELF })
    expect(verdict).toEqual({ path: join(realpathSync(directory), 'broker.sock') })
    expect(socketPath).toBeTruthy()
  })

  it('refuses a relative path', () => {
    expect(problem(socketDistrust('broker.sock', { ownerUid: SELF }))).toMatch(/not an absolute path/u)
  })

  it('makes the client refuse everything, without connecting, when the socket is not trusted', async () => {
    const server = await broker()
    const closed: string[] = []
    const control = createBrokerClient({ socketPath: server.socketPath }, {}, { baseDelayMs: 10 }, { ownerUid: SELF + 1 })
    control.onClosed?.(reason => closed.push(reason))
    cleanups.push(() => { control.closeControl() })
    const answer = await control.bind(bindRequest)
    expect(answer).toMatchObject({ ok: false, reason: 'untrusted-socket' })
    expect((answer as { detail?: string }).detail).toMatch(/not owned by uid/u)
    expect(await control.status()).toMatchObject({ ok: false, reason: 'untrusted-socket' })
    await new Promise(resolve => setTimeout(resolve, 40))
    expect(server.connections).toHaveLength(0)
  })

  it('checks again before a reconnect: a socket that stopped being trustworthy is not connected to', async () => {
    const server = await broker()
    const control = new ReconnectingBrokerClient(server.socketPath, {}, { baseDelayMs: 20 }, { ownerUid: SELF })
    cleanups.push(() => { control.closeControl() })
    await vi.waitFor(() => { expect(control.epoch).toBe(7) })
    // The directory is opened to others while the connection is down.
    chmodSync(join(server.socketPath, '..'), 0o777)
    server.peer().destroy()
    await vi.waitFor(() => { expect(control.closed).toBe(true) }, { timeout: 5000 })
    expect(await control.bind(bindRequest)).toMatchObject({ ok: false, reason: 'untrusted-socket' })
    expect(((await control.bind(bindRequest)) as { detail?: string }).detail).toMatch(/writable by its group or by others/u)
    expect(server.frames.filter(frame => frame.type === 'register-core')).toHaveLength(1)
  })
})
