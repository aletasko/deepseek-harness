/** Opt-in Unix-socket client for a trusted host subprocess broker. */
import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'
import { connect } from 'node:net'
import { isAbsolute } from 'node:path'
import { PassThrough } from 'node:stream'
import type { Context } from '@deepseek-ai/cordis'
import type {
  SubprocessOutcome,
  SubprocessSpawnSpec,
  SubprocessTerminalHandle,
  SubprocessTerminalSignal,
  SubprocessTerminalSpawnSpec,
} from '@deepseek-ai/dsh-subprocess'
import type { ManagedProcessLaunch } from './managed-owner.ts'
import { bindManagedProcess } from './spawn.ts'

export interface SubprocessBrokerConfig {
  /** Trusted local Unix socket path. Never supplied by the model-facing request. */
  socketPath: string
}

/** Host-owned mapping from Core-attested session identity to broker policy. */
export interface SubprocessBrokerAssignmentResolver {
  /** Return the opaque broker assignment for this session, or deny with undefined. */
  resolve(sessionId: string): string | undefined
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    subprocessBrokerConfig?: SubprocessBrokerConfig
    subprocessBrokerAssignmentResolver?: SubprocessBrokerAssignmentResolver
  }
}

type JsonRecord = Record<string, unknown>

class BrokerConnection {
  readonly socket
  private buffer = ''
  private readonly listeners = new Set<(message: JsonRecord) => void>()
  private readonly closed = Promise.withResolvers<Error | undefined>()
  get onClosed(): Promise<Error | undefined> { return this.closed.promise }

  constructor(socketPath: string, signal?: AbortSignal) {
    this.socket = connect(socketPath)
    this.socket.setEncoding('utf8')
    this.socket.on('data', (chunk: string) => this.read(chunk))
    this.socket.on('error', error => this.fail(error))
    this.socket.once('close', () => {
      signal?.removeEventListener('abort', abort)
      const error = new Error('subprocess-local broker: socket closed before terminal event')
      this.closed.resolve(error)
    })
    const abort = (): void => { this.socket.destroy(abortError(signal!)) }
    signal?.addEventListener('abort', abort, { once: true })
  }

  onMessage(listener: (message: JsonRecord) => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  send(message: JsonRecord): void {
    if (this.socket.destroyed) throw new Error('subprocess-local broker: connection is closed')
    this.socket.write(`${JSON.stringify(message)}\n`)
  }

  fail(error: Error): void {
    this.closed.resolve(error)
  }

  private read(chunk: string): void {
    this.buffer += chunk
    if (this.buffer.length > 8 * 1024 * 1024) {
      this.fail(new Error('subprocess-local broker: protocol frame exceeds limit'))
      this.socket.destroy()
      return
    }
    while (true) {
      const end = this.buffer.indexOf('\n')
      if (end < 0) return
      const line = this.buffer.slice(0, end)
      this.buffer = this.buffer.slice(end + 1)
      try {
        const message: unknown = JSON.parse(line)
        if (message === null || typeof message !== 'object' || Array.isArray(message)) throw new Error('expected object')
        for (const listener of this.listeners) listener(message as JsonRecord)
      } catch (cause) {
        this.fail(new Error('subprocess-local broker: invalid protocol frame', { cause }))
        this.socket.destroy()
        return
      }
    }
  }
}

function abortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error('subprocess-local broker: request aborted')
}

function outcomeOf(message: JsonRecord): SubprocessOutcome {
  if (message.type !== 'outcome'
    || !(message.exitCode === null || Number.isInteger(message.exitCode))
    || !(message.signal === null || typeof message.signal === 'string')) {
    throw new Error('subprocess-local broker: invalid outcome event')
  }
  return { exitCode: message.exitCode as number | null, signal: message.signal as NodeJS.Signals | null }
}

function decodeBase64(value: unknown, label: string): Buffer {
  if (typeof value !== 'string' || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u.test(value)) {
    throw new Error(`subprocess-local broker: invalid ${label} data`)
  }
  return Buffer.from(value, 'base64')
}

function stdioRequest(spec: SubprocessSpawnSpec): JsonRecord {
  const mode = (value: unknown, input = false): JsonRecord => {
    if (value === 'pipe' || value === 'inherit' || value === 'ignore') return { mode: value }
    if (input && value !== null && typeof value === 'object' && typeof (value as { data?: unknown }).data === 'string') {
      return { mode: 'data', data: (value as { data: string }).data }
    }
    if (value !== null && typeof value === 'object') {
      const collect = value as { maxBytes?: unknown; spill?: { maxBytes?: unknown } }
      return { mode: 'collect', maxBytes: collect.maxBytes, spillMaxBytes: collect.spill?.maxBytes }
    }
    throw new Error('subprocess-local broker: invalid stdio disposition')
  }
  return {
    stdin: mode(spec.stdio.stdin, true), stdout: mode(spec.stdio.stdout), stderr: mode(spec.stdio.stderr),
    ...(spec.stdio.control === undefined ? {} : { control: { mode: 'pipe' } }),
  }
}

/** Start a broker process and adapt its streams to the normal local lifecycle binder. */
export function launchBrokerProcess(
  spec: SubprocessSpawnSpec,
  env: NodeJS.ProcessEnv,
  config: SubprocessBrokerConfig,
  assignmentId: string,
  signal?: AbortSignal,
): ManagedProcessLaunch {
  const connection = new BrokerConnection(config.socketPath, signal)
  const stdin = spec.stdio.stdin === 'pipe' ? new PassThrough() : null
  const stdout = new PassThrough()
  const stderr = new PassThrough()
  if (spec.stdio.stdout === 'inherit') stdout.resume()
  if (spec.stdio.stderr === 'inherit') stderr.resume()
  const control = spec.stdio.control === 'pipe' ? new PassThrough() : null
  const direct = Promise.withResolvers<SubprocessOutcome>()
  const empty = Promise.withResolvers<void>()
  let accepted = false
  let registered = false
  const registrationRequestId = randomUUID()
  let handleId = ''
  let pendingSignal: 'SIGTERM' | 'SIGKILL' | undefined
  let finished = false
  let outcomeSeen = false
  let exitOutcome: SubprocessOutcome | undefined
  let protocolError: Error | undefined
  let startupTimer: ReturnType<typeof setTimeout> | undefined
  const fail = (error: Error): void => {
    if (protocolError !== undefined || finished) return
    protocolError = error
    if (startupTimer !== undefined) clearTimeout(startupTimer)
    direct.reject(error)
    stdin?.destroy()
    stdout?.destroy()
    stderr?.destroy()
    empty.reject(error)
    connection.socket.destroy()
  }
  const dispose = connection.onMessage(message => {
    try {
      if (message.type === 'registered') {
        if (registered || accepted || message.requestId !== registrationRequestId
          || !Number.isSafeInteger(message.revision) || (message.revision as number) < 1) {
          throw new Error('invalid Core registration event')
        }
        registered = true
        connection.send({
          type: 'start', version: 1, requestId: randomUUID(), mode: 'process',
          assignmentId, argv: [...spec.argv], cwd: spec.cwd, env,
          stdio: stdioRequest(spec), graceMs: spec.graceMs,
        })
        return
      }
      if (message.type === 'accepted') {
        if (!registered || accepted || typeof message.handle !== 'string' || message.handle.length === 0) throw new Error('invalid accepted event')
        accepted = true
        handleId = message.handle
        if (pendingSignal !== undefined) {
          connection.send({ type: 'cancel', handle: handleId, signal: pendingSignal })
          pendingSignal = undefined
        }
        if (startupTimer !== undefined) clearTimeout(startupTimer)
        stdin?.on('data', chunk => {
          if (!connection.socket.write(`${JSON.stringify({ type: 'stdin', handle: message.handle, data: Buffer.from(chunk).toString('base64') })}\n`)) {
            stdin.pause()
            connection.socket.once('drain', () => stdin.resume())
          }
        })
        stdin?.on('end', () => connection.send({ type: 'stdin-eof', handle: message.handle }))
        control?.on('data', chunk => connection.send({ type: 'control-input', handle: message.handle, data: Buffer.from(chunk).toString('base64') }))
        control?.on('end', () => connection.send({ type: 'control-eof', handle: message.handle }))
        return
      }
      if (message.type === 'error') {
        throw new Error(typeof message.message === 'string' ? message.message : 'broker rejected request')
      }
      if (!accepted) throw new Error('event arrived before accepted')
      if (message.handle !== handleId) throw new Error('event handle does not match accepted request')
      if (message.type === 'stdout' || message.type === 'stderr') {
        const target = message.type === 'stdout' ? stdout : stderr
        const bytes = decodeBase64(message.data, 'process output')
        if (!target.write(bytes)) {
          connection.socket.pause()
          target.once('drain', () => connection.socket.resume())
        }
        if ((message.type === 'stdout' ? spec.stdio.stdout : spec.stdio.stderr) === 'inherit') {
          ;(message.type === 'stdout' ? process.stdout : process.stderr).write(bytes)
        }
      } else if (message.type === 'control') {
        if (control === null) throw new Error('unexpected control data')
        control.write(decodeBase64(message.data, 'control'))
      } else if (message.type === 'outcome') {
        if (outcomeSeen) throw new Error('duplicate outcome')
        outcomeSeen = true
        exitOutcome = outcomeOf(message)
        direct.resolve(exitOutcome)
      } else if (message.type === 'empty') {
        if (!outcomeSeen || finished) throw new Error('unexpected empty event')
        finished = true
        // The broker owns the full managed range. Descendants may still write
        // after the direct command exits, so close streams only at quiescence.
        stdout.end(); stderr.end(); control?.end()
        empty.resolve()
        dispose()
        connection.socket.end()
      } else throw new Error('unknown event type')
    } catch (error) {
      fail(error instanceof Error ? error : new Error(String(error)))
    }
  })
  connection.onClosed.then(error => { if (error !== undefined && !finished) fail(error) })
  try {
    connection.send({ type: 'register-core', version: 1, requestId: registrationRequestId })
    startupTimer = setTimeout(() => fail(new Error('subprocess-local broker: start acceptance timed out')), 10_000)
  } catch (error) {
    fail(error instanceof Error ? error : new Error(String(error)))
  }
  const owner: ManagedProcessLaunch['owner'] = {
    signal: sig => {
      if (finished || protocolError !== undefined || connection.socket.destroyed) return
      if (handleId.length === 0) pendingSignal = sig
      else connection.send({ type: 'cancel', handle: handleId, signal: sig })
    },
    waitForExit: () => empty.promise,
    terminateForHostExit: () => { if (!finished) connection.socket.destroy() },
    cleanup: () => { dispose(); connection.socket.destroy() },
  }
  return { stdin, stdout, stderr, control: control ?? undefined, direct: direct.promise, owner }
}

/** Terminal implementation for the same framed broker protocol. */
export class BrokerTerminalHandle implements SubprocessTerminalHandle {
  readonly output = new PassThrough()
  readonly done: Promise<SubprocessOutcome>
  private pidValue = -1
  private readonly accepted = Promise.withResolvers<void>()
  private outcomeReceived = false
  private startupTimer: ReturnType<typeof setTimeout> | undefined
  private readonly connection: BrokerConnection
  private readonly doneResult = Promise.withResolvers<SubprocessOutcome>()
  private readonly quiescence = Promise.withResolvers<void>()
  private readonly pending = new Map<string, ReturnType<typeof Promise.withResolvers<JsonRecord>>>()
  private nextId = 0
  private closed = false
  private registered = false

  constructor(config: SubprocessBrokerConfig, assignmentId: string, spec: SubprocessTerminalSpawnSpec, env: Record<string, string>) {
    const registrationRequestId = randomUUID()
    this.connection = new BrokerConnection(config.socketPath, spec.signal)
    this.done = this.doneResult.promise
    void this.done.catch(() => {})
    void this.quiescence.promise.catch(() => {})
    this.ready = this.accepted.promise
    this.connection.onMessage(message => {
      try {
        if (message.type === 'registered') {
          if (this.registered || this.handleId.length > 0 || message.requestId !== registrationRequestId
            || !Number.isSafeInteger(message.revision) || (message.revision as number) < 1) {
            throw new Error('invalid terminal Core registration event')
          }
          this.registered = true
          this.connection.send({
            type: 'start', version: 1, requestId: randomUUID(), mode: 'terminal',
            assignmentId, argv: [...spec.argv], cwd: spec.cwd, env,
            stdio: { stdin: { mode: 'pty' }, stdout: { mode: 'pty' }, stderr: { mode: 'pty' } },
            rows: spec.rows, cols: spec.cols, terminalType: spec.terminalType,
            graceMs: spec.graceMs, shellActivity: spec.shellActivity === true,
          })
          return
        }
        if (message.type !== 'accepted' && this.handleId.length > 0 && message.handle !== this.handleId) {
          throw new Error('terminal event handle mismatch')
        }
        if (message.type === 'accepted') {
          if (!this.registered || this.handleId.length > 0 || !Number.isInteger(message.pid) || (message.pid as number) <= 0) throw new Error('invalid terminal accepted event')
          if (this.startupTimer !== undefined) clearTimeout(this.startupTimer)
          this.pidValue = message.pid as number
          this.handleId = message.handle as string
          if (typeof message.handle !== 'string' || message.handle.length === 0) throw new Error('invalid terminal handle')
          this.accepted.resolve()
        } else if (message.type === 'output') {
          if (!this.output.write(decodeBase64(message.data, 'terminal output'))) {
            this.connection.socket.pause()
            this.output.once('drain', () => this.connection.socket.resume())
          }
        } else if (message.type === 'outcome') {
          this.outcomeReceived = true
          this.doneResult.resolve(outcomeOf(message))
        } else if (message.type === 'empty') {
          if (!this.outcomeReceived) throw new Error('empty event arrived before terminal outcome')
          this.closed = true
          this.output.end()
          this.quiescence.resolve()
          this.connection.socket.end()
        } else if (message.type === 'response' && typeof message.id === 'string') {
          const waiter = this.pending.get(message.id)
          if (waiter === undefined) throw new Error('unexpected terminal response')
          this.pending.delete(message.id)
          if (message.ok !== true) waiter.reject(new Error(typeof message.message === 'string' ? message.message : 'broker operation failed'))
          else waiter.resolve(message)
        } else if (message.type === 'error') throw new Error(typeof message.message === 'string' ? message.message : 'broker rejected request')
        else throw new Error('unknown terminal event')
      } catch (error) {
        if (this.startupTimer !== undefined) clearTimeout(this.startupTimer)
        this.accepted.reject(error)
        this.doneResult.reject(error)
        this.output.destroy()
        this.quiescence.reject(error)
        this.rejectPending(error instanceof Error ? error : new Error(String(error)))
        this.connection.socket.destroy()
      }
    })
    this.connection.onClosed.then(error => {
      if (error !== undefined && !this.closed) {
        if (this.startupTimer !== undefined) clearTimeout(this.startupTimer)
        this.accepted.reject(error)
        this.doneResult.reject(error)
        this.quiescence.reject(error)
        this.rejectPending(error)
        this.output.destroy()
      }
    })
    this.connection.send({ type: 'register-core', version: 1, requestId: registrationRequestId })
    this.startupTimer = setTimeout(() => {
      const error = new Error('subprocess-local broker: terminal start acceptance timed out')
      this.accepted.reject(error)
      this.doneResult.reject(error)
      this.quiescence.reject(error)
      this.connection.socket.destroy()
    }, 10_000)
    spec.signal?.addEventListener('abort', () => { void this.terminate().catch(() => {}) }, { once: true })
  }

  readonly ready: Promise<void>
  private handleId = ''
  get pid(): number { return this.pidValue }

  private rejectPending(error: Error): void {
    for (const waiter of this.pending.values()) waiter.reject(error)
    this.pending.clear()
  }

  private async request(type: string, fields: JsonRecord = {}): Promise<JsonRecord> {
    if (this.closed) throw new Error('terminal process has exited')
    const id = String(++this.nextId)
    const pending = Promise.withResolvers<JsonRecord>()
    this.pending.set(id, pending)
    const timer = setTimeout(() => {
      if (!this.pending.delete(id)) return
      const error = new Error(`subprocess-local broker: terminal ${type} request timed out`)
      pending.reject(error)
      // An unresponsive operation leaves PTY state unknown. Drop the
      // connection; the broker contract requires disconnect teardown.
      this.connection.socket.destroy(error)
    }, 10_000)
    void pending.promise.finally(() => clearTimeout(timer)).catch(() => {})
    try {
      this.connection.send({ type, id, handle: this.handleId, ...fields })
    } catch (error) {
      this.pending.delete(id)
      pending.reject(error)
    }
    return pending.promise
  }

  async write(data: string): Promise<void> { await this.request('stdin', { data: Buffer.from(data).toString('base64') }) }
  async resize(cols: number, rows: number): Promise<void> { await this.request('resize', { cols, rows }) }
  async inspectForeground(): Promise<{ processGroupId: number; inputWaiting: boolean } | undefined> {
    const result = await this.request('inspect-foreground')
    if (result.processGroupId === null) return undefined
    if (!Number.isInteger(result.processGroupId) || typeof result.inputWaiting !== 'boolean') throw new Error('subprocess-local broker: invalid foreground response')
    return { processGroupId: result.processGroupId as number, inputWaiting: result.inputWaiting }
  }
  async inspectActivity(): Promise<{ state: 'idle' | 'busy' | 'unknown'; revision: number }> {
    const result = await this.request('inspect-activity')
    if (!['idle', 'busy', 'unknown'].includes(String(result.state)) || !Number.isInteger(result.revision)) throw new Error('subprocess-local broker: invalid activity response')
    return { state: result.state as 'idle' | 'busy' | 'unknown', revision: result.revision as number }
  }
  async signalForeground(signal: SubprocessTerminalSignal): Promise<number> {
    const result = await this.request('signal-foreground', { signal })
    if (!Number.isInteger(result.processGroupId)) throw new Error('subprocess-local broker: invalid signal response')
    return result.processGroupId as number
  }
  async terminate(): Promise<void> {
    if (!this.closed) {
      await this.ready
      if (!this.closed) this.connection.send({ type: 'cancel', handle: this.handleId, signal: 'SIGTERM' })
    }
    await this.quiescence.promise
  }

  terminateForHostExit(): void {
    if (!this.closed) this.connection.socket.destroy()
  }
}

export function bindBrokerProcess(
  spec: SubprocessSpawnSpec,
  launch: ManagedProcessLaunch,
  internals?: Parameters<typeof bindManagedProcess>[2],
): ReturnType<typeof bindManagedProcess> {
  return bindManagedProcess(spec, launch, internals)
}

/**
 * Whether broker mode has ever been declared in this Core process. `provide('subprocessBrokerConfig')` records the
 * name in `ctx.reflect.props` as soon as the provider runs, and Cordis never removes it when the provider is
 * disposed, so a pending, replaced, or disposed provider still counts. The same check is duplicated in
 * `@deepseek-ai/dsh-plugin-manager` and `@deepseek-ai/dsh-cordis-host-runner`, which cannot import this package
 * without a new dependency edge; keep the three in step.
 *
 * This is a same-process guard, not a security boundary: Host code that shares the process can mutate
 * `reflect.props`, and the record is lost when the process restarts. A broker-required profile must be enforced
 * statically.
 * @param ctx Context whose Cordis root is checked.
 * @returns True when the name was provided in this process or is provided now.
 */
export function brokerModeDeclared(ctx: Context): boolean {
  return ctx.reflect.props['subprocessBrokerConfig'] !== undefined
    || ctx.reflect.get('subprocessBrokerConfig', false) !== undefined
}

export function assertBrokerConfig(config: SubprocessBrokerConfig | undefined): SubprocessBrokerConfig | undefined {
  if (config === undefined) return undefined
  if (typeof config.socketPath !== 'string' || !isAbsolute(config.socketPath)) {
    throw new Error('subprocess-local: invalid trusted broker configuration')
  }
  return config
}

export function resolveBrokerAssignment(
  resolver: SubprocessBrokerAssignmentResolver | undefined,
  sessionId: string | undefined,
): string {
  if (resolver === undefined) throw new Error('subprocess-local: broker assignment resolver is unavailable')
  if (typeof sessionId !== 'string' || sessionId.length === 0) {
    throw new Error('subprocess-local: broker launch requires a Core-attested agent session')
  }
  const assignmentId = resolver.resolve(sessionId)
  if (typeof assignmentId !== 'string' || assignmentId.length === 0) {
    throw new Error('subprocess-local: no broker assignment for the active session')
  }
  return assignmentId
}

export function terminalEnv(spec: SubprocessTerminalSpawnSpec, env: Record<string, string>): Record<string, string> {
  return { ...env, PWD: spec.cwd, TERM: spec.terminalType }
}

/** Prevent caller environment from spoofing the identity used for assignment resolution. */
export function assertNoBrokerIdentityOverride(env: Record<string, string>): void {
  if (Object.keys(env).some(key => key.toUpperCase() === 'DSH_SESSION_ID')) {
    throw new Error('subprocess-local: DSH_SESSION_ID cannot be set for broker launches')
  }
}
