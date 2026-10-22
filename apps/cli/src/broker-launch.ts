/**
 * The resolver bridge (S5a W-2). The local subprocess runtime starts a process in one synchronous call, while a
 * broker launch is two round trips: `lease` the session's binding for mode `process`, then `start` a worker, both
 * over the launcher's one control connection. {@link BindingLauncher} is the asynchronous half. The runtime attests
 * its active execution synchronously, hands that execution (never a session id) to {@link BindingLauncher.launch},
 * and waits on the promise it returns; the worker's end comes from the broker's `worker-exited` frame.
 *
 * The session is read from the execution by the ToolRuntime's attestation and from nothing else; the lease is the
 * broker's answer relayed unchanged (Core mints no `assignmentId`); the very binding (id and revision) the lease was
 * asked for must still be the session's, and the gate still open, when the worker has started, or the worker is
 * killed. A caller that aborts before `start` never gets a worker; after `start` it stops the worker with the
 * `signal` of the result (S6 contract 11).
 * Nothing here is provided on a Context: the launcher reaches `subprocess-local` only through the locked root slot
 * (`installBrokerLauncher` in `broker-capabilities.ts`).
 * @module @deepseek-ai/dsh/broker-launch
 */

import type {
  BindingBrokerClient, BindingIssuer, BindingRefusal, BindingResolver, BrokerGate, WorkerExit, WorkerSignalName,
} from './broker-binding.ts'

/** What the runtime asks for; the same shape `subprocess-local` declares (`BrokerLaunchRequest`). */
export interface LaunchRequest {
  readonly execution: unknown
  readonly argv: readonly string[]
  readonly cwd: string
  readonly env: Readonly<Record<string, string>>
  readonly graceMs: number
  /** Aborted when the caller no longer wants the process. Once aborted before `start`, nothing is started. */
  readonly signal?: AbortSignal
}

export type LaunchResult =
  | {
    readonly ok: true
    readonly pid: number
    readonly workerId: string
    readonly exited: Promise<WorkerExit>
    /** Stop this worker (contract 11). The broker's `worker-exited` still settles `exited`. */
    readonly signal: (name: WorkerSignalName) => Promise<{ readonly ok: true } | BindingRefusal>
  }
  | { readonly ok: false; readonly reason: string; readonly detail?: string }

function denied(reason: string, detail?: string): LaunchResult {
  return Object.freeze({ ok: false, reason, ...detail === undefined ? {} : { detail } })
}

export class BindingLauncher {
  constructor(
    /** The ToolRuntime's attestation of an execution's session id; supplied by `broker-binding.ts`. */
    private readonly attest: (execution: unknown) => string | undefined,
    private readonly gate: BrokerGate,
    private readonly issuer: BindingIssuer,
    private readonly resolver: BindingResolver,
    private readonly client: BindingBrokerClient,
  ) {}

  /**
   * Lease and start. Everything up to the lease frame runs in the caller's synchronous turn.
   *
   * Trust boundary of this entry point. It is reachable by any code in the process that can read the locked root slot
   * (`installBrokerLauncher`), and it skips what `ctx.subprocess.spawn` does before it gets here (the approval and
   * process-execution gates, the credential-scrubbing environment). What it does not let a caller choose: the session
   * (read from the ToolRuntime's attestation of a live, direct tool execution and nothing else), the binding
   * (Core's own record of that session), the lease (the broker's) and the root and jail the worker runs in (the
   * broker's). So code that is already running inside an agent's tool call can start a worker of its choice in that
   * agent's own jail, which is what `spawn` would let it do; it cannot start one for another session, outside a tool
   * call, or without a binding. What keeps hostile Host code out of the process is the boot-deny gate checked below,
   * and what contains a worker is the broker, not this module.
   * @param input A {@link LaunchRequest}; anything else is refused, because the slot this is reached through is
   *   readable by any code in the process.
   * @returns The started worker, whose `exited` settles from `worker-exited`, or a refusal. Never throws.
   */
  async launch(input: unknown): Promise<LaunchResult> {
    try {
      if (input === null || typeof input !== 'object') return denied('invalid-request')
      const { execution, argv, cwd, env, graceMs, signal } = input as Record<keyof LaunchRequest, unknown>
      if (!Array.isArray(argv) || typeof cwd !== 'string' || env === null || typeof env !== 'object' || typeof graceMs !== 'number') {
        return denied('invalid-request')
      }
      if (signal !== undefined && !(signal instanceof AbortSignal)) return denied('invalid-request')
      const cancelled = signal
      // A function, so the second look is not narrowed away by the first.
      const gone = (): boolean => cancelled?.aborted === true
      if (gone()) return denied('aborted')
      // All three read the execution before any await: the session, and the binding it holds right now.
      const sessionId = this.attest(execution)
      const before = sessionId === undefined ? undefined : this.issuer.lookup(sessionId)
      const lease = await this.resolver.lease(execution)
      if (!lease.ok) return denied(lease.reason)
      // The caller gave up while the broker leased: the lease is left to expire and no worker is ever started.
      if (gone()) return denied('aborted')
      const started = await this.client.start({
        assignmentId: lease.assignmentId, argv: argv as string[], cwd, env: env as Record<string, string>, graceMs,
      })
      if (!started.ok) return denied(started.reason, started.detail)
      // The worker exists. It is only kept if it still runs under the binding it was leased from, id and revision,
      // and the gate is still open. A binding revoked and bound again for the same session is another binding.
      const now = sessionId === undefined ? undefined : this.issuer.lookup(sessionId)
      const same = before !== undefined && 'bindingId' in before && now !== undefined && 'bindingId' in now
        && now.bindingId === before.bindingId && now.revision === before.revision
      const gateOpen = this.gate.isActive()
      if (!gateOpen || !same) {
        // A closed gate ends the session's binding; a replaced binding is somebody else's and stays.
        if (!gateOpen && sessionId !== undefined) void this.issuer.revoke(sessionId)
        void this.client.signal(started.workerId, 'KILL')
        return denied(gateOpen ? 'revoked' : 'gate-inactive')
      }
      return Object.freeze({
        ok: true, pid: started.pid, workerId: started.workerId, exited: started.exited,
        signal: (name: WorkerSignalName) => this.client.signal(started.workerId, name),
      })
    } catch {
      return denied('assignment-unavailable')
    }
  }
}
