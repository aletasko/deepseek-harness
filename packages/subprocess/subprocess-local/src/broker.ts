/**
 * Broker mode for the local subprocess runtime: the launch path that hands a process to the root broker.
 *
 * Core keeps ONE control connection to the broker (S6 contract), owned by the launcher. This package does not open
 * sockets: it asks the launcher's {@link SubprocessBrokerLauncher}, installed on the root context by the CLI, to
 * lease and start a worker for the attested execution, and maps the broker's `worker-exited` to the handle's
 * outcome. What v1 does not carry, because the broker's `start` accepts only `ignore` for every stream:
 *
 * - no stdout or stderr capture: `collect`, `pipe` and `inherit` yield empty, ended streams, so a caller that reads
 *   the output of a brokered process sees none (the exit status is real);
 * - no stdin: whatever the caller writes is discarded;
 * - no control channel (refused at spawn) and no terminal sessions (refused: lease `terminal` is refused in v1);
 * - no per-process signal: the broker has no frame to signal one worker, so `signal()` cannot end it. A worker ends
 *   by exiting, or when its binding is revoked or the control connection closes (the broker kills the whole range).
 */
import { PassThrough } from 'node:stream'
import { isAbsolute } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import type { SubprocessOutcome, SubprocessSpawnSpec } from '@deepseek-ai/dsh-subprocess'
import type { ManagedProcessLaunch } from './managed-owner.ts'
import { bindManagedProcess } from './spawn.ts'

export interface SubprocessBrokerConfig {
  /** Trusted local Unix socket path. Never supplied by the model-facing request. */
  socketPath: string
}

/** What the runtime asks the launcher for. The session identity is NOT in it: the launcher reads it from `execution`. */
export interface BrokerLaunchRequest {
  /** The ToolRuntime's active execution, captured in the caller's own synchronous turn. */
  readonly execution: unknown
  readonly argv: readonly string[]
  readonly cwd: string
  readonly env: Readonly<Record<string, string>>
  readonly graceMs: number
}

/** How a brokered worker ended, as the broker reported it, or why that is unknown (the connection was lost). */
export type BrokerWorkerExit =
  | { readonly ok: true; readonly code: number | null; readonly signal: string | null }
  | { readonly ok: false; readonly reason: string }

export type BrokerLaunchResult =
  | { readonly ok: true; readonly pid: number; readonly workerId: string; readonly exited: Promise<BrokerWorkerExit> }
  | { readonly ok: false; readonly reason: string }

/**
 * Host-owned bridge from a Core-attested execution to a broker worker: lease and start over the shared control
 * connection. Asynchronous on purpose, because both steps are broker round trips.
 */
export interface SubprocessBrokerLauncher {
  /** Never throws; a refusal is `{ ok: false, reason }`. */
  launch(request: BrokerLaunchRequest): Promise<BrokerLaunchResult>
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    subprocessBrokerConfig?: SubprocessBrokerConfig
  }
}

/**
 * Registry-wide key under which the launcher defines the broker launcher on the ROOT context. It is deliberately not
 * a Context name: any plugin can `provide` a name, and whichever provider Cordis picks would decide what a launch
 * runs against. A symbol-keyed own property that the launcher defines once, before the tree mounts, as non-writable
 * and non-configurable, cannot be shadowed by `ctx.provide` or replaced afterwards. Anything in this process can
 * still read it, and code that runs before the launcher could define it first, which is why
 * {@link readBrokerLauncher} only accepts a locked property and the launcher claims the slot at boot (see
 * `installBrokerLauncher` in the CLI's `broker-capabilities`).
 */
export const BROKER_LAUNCHER_KEY: unique symbol = Symbol.for('dsh.subprocess-local.broker-launcher')

/**
 * The launcher-installed broker launcher, or `undefined` (deny) when the root has none, or one that is replaceable,
 * inherited, an accessor, or not an object with a `launch` function. Read per launch, never cached.
 * @param ctx Any context of this process; the root is consulted.
 */
export function readBrokerLauncher(ctx: Context): SubprocessBrokerLauncher | undefined {
  const descriptor = Object.getOwnPropertyDescriptor(ctx.root, BROKER_LAUNCHER_KEY)
  if (descriptor === undefined || !('value' in descriptor) || descriptor.writable !== false || descriptor.configurable !== false) {
    return undefined
  }
  const value: unknown = descriptor.value
  if (value === null || typeof value !== 'object' || typeof (value as { launch?: unknown }).launch !== 'function') return undefined
  return value as SubprocessBrokerLauncher
}

function refused(reason: string): Error {
  return new Error(`subprocess-local: the broker refused the launch (${reason})`)
}

/**
 * Start a process through the broker and adapt it to the normal local lifecycle binder. Returns at once: the lease,
 * the start and the wait for `worker-exited` happen behind `direct`, which settles with the worker's real exit status
 * or rejects when the broker refused or the connection was lost. An already aborted request never gets here
 * (`validateSubprocessSpec` rejects it), and one aborted later cannot be delivered (see the module comment).
 * stdout and stderr are empty streams that end with the worker (see the module comment).
 * @param spec The spawn request. Its stdio dispositions are not forwarded: v1 is `ignore` for all three.
 * @param env The environment the worker gets.
 * @param launcher The launcher installed by the CLI.
 * @param execution The ToolRuntime's active execution, taken by the caller in its own synchronous turn.
 */
export function launchBrokerProcess(
  spec: SubprocessSpawnSpec,
  env: NodeJS.ProcessEnv,
  launcher: SubprocessBrokerLauncher,
  execution: unknown,
): ManagedProcessLaunch {
  const stdin = spec.stdio.stdin === 'pipe' ? new PassThrough() : null
  stdin?.resume()
  const stdout = new PassThrough()
  const stderr = new PassThrough()
  const direct = Promise.withResolvers<SubprocessOutcome>()
  const settled = Promise.withResolvers<void>()
  const closeStreams = (): void => {
    stdin?.destroy()
    stdout.end()
    stderr.end()
  }
  const environment: Record<string, string> = {}
  for (const [name, value] of Object.entries(env)) if (typeof value === 'string') environment[name] = value
  void (async () => {
    try {
      const started = await launcher.launch({
        execution, argv: [...spec.argv], cwd: spec.cwd, env: environment, graceMs: Math.max(1, spec.graceMs),
      })
      if (!started.ok) throw refused(started.reason)
      const exit = await started.exited
      if (!exit.ok) throw new Error(`subprocess-local: the broker connection ended before the process was reported done (${exit.reason})`)
      direct.resolve({ exitCode: exit.code, signal: exit.signal as NodeJS.Signals | null })
    } catch (error) {
      direct.reject(error instanceof Error ? error : new Error(String(error)))
    } finally {
      closeStreams()
      settled.resolve()
    }
  })()
  const owner: ManagedProcessLaunch['owner'] = {
    // The broker has no frame that signals one worker. A termination request cannot be delivered; the worker ends by
    // exiting, or with its binding when that is revoked or the connection closes.
    signal: () => {},
    waitForExit: () => settled.promise,
    terminateForHostExit: () => {},
    cleanup: () => {},
  }
  return { stdin, stdout, stderr, direct: direct.promise, owner }
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

/** Prevent caller environment from spoofing the identity used for assignment resolution. */
export function assertNoBrokerIdentityOverride(env: Record<string, string>): void {
  if (Object.keys(env).some(key => key.toUpperCase() === 'DSH_SESSION_ID')) {
    throw new Error('subprocess-local: DSH_SESSION_ID cannot be set for broker launches')
  }
}
