import { once } from 'node:events'
import { mkdtempSync, rmSync } from 'node:fs'
import { createServer, type Server, type Socket } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u

export type Frame = Record<string, unknown> & { type: string; requestId: string }

export interface FakeServer {
  readonly socketPath: string
  readonly frames: Frame[]
  readonly connections: Socket[]
  /** The last connection. */
  peer(): Socket
  send(value: object): void
  /** Wait for the n-th frame (1-based) of `type`. */
  next(type: string, n?: number): Promise<Frame>
  closed(): Promise<void>
  close(): Promise<void>
}

export const cleanups: Array<() => Promise<void> | void> = []

/** A broker on a Unix socket. By default it answers `register-core` with `registered{revision: 7}`. */
export async function broker(options: { register?: 'ok' | 'silent' | ((frame: Frame, server: FakeServer) => void) } = {}): Promise<FakeServer> {
  const directory = mkdtempSync(join(tmpdir(), 'dsh-client-'))
  const socketPath = join(directory, 'broker.sock')
  const frames: Frame[] = []
  const connections: Socket[] = []
  const waiters: Array<{ type: string; n: number; resolve: (frame: Frame) => void }> = []
  const closedWaiters: Array<() => void> = []
  const server: FakeServer = {
    socketPath, frames, connections,
    peer: () => connections.at(-1)!,
    send: (value) => { server.peer().write(`${JSON.stringify(value)}\n`) },
    next: (type, n = 1) => {
      const seen = frames.filter(frame => frame.type === type)
      if (seen.length >= n) return Promise.resolve(seen[n - 1]!)
      return new Promise((resolve) => { waiters.push({ type, n, resolve }) })
    },
    closed: () => new Promise((resolve) => { closedWaiters.push(resolve) }),
    close: async () => {
      for (const socket of connections) socket.destroy()
      await new Promise<void>((resolve) => { listener.close(() => { resolve() }) })
      rmSync(directory, { recursive: true, force: true })
    },
  }
  const listener: Server = createServer((socket) => {
    connections.push(socket)
    socket.setEncoding('utf8')
    socket.on('close', () => { for (const resolve of closedWaiters.splice(0)) resolve() })
    socket.on('error', () => {})
    let buffer = ''
    socket.on('data', (chunk: string) => {
      buffer += chunk
      for (let end = buffer.indexOf('\n'); end >= 0; end = buffer.indexOf('\n')) {
        const frame = JSON.parse(buffer.slice(0, end)) as Frame
        buffer = buffer.slice(end + 1)
        frames.push(frame)
        if (frame.type === 'register-core') {
          if (typeof options.register === 'function') options.register(frame, server)
          else if (options.register !== 'silent') server.send({ type: 'registered', requestId: frame.requestId, revision: 7 })
        }
        const count = frames.filter(f => f.type === frame.type).length
        for (const waiter of waiters.filter(entry => entry.type === frame.type && count >= entry.n)) {
          waiters.splice(waiters.indexOf(waiter), 1)
          waiter.resolve(frames.filter(f => f.type === waiter.type)[waiter.n - 1]!)
        }
      }
    })
  })
  listener.listen(socketPath)
  await once(listener, 'listening')
  cleanups.push(() => server.close())
  return server
}
