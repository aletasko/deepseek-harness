/**
 * The resolver bridge (S5a W-2). The local subprocess runtime starts a process in one synchronous call, while a
 * broker launch is two round trips: `lease` the session's binding for mode `process`, then `start` a worker, both
 * over the launcher's one control connection. {@link BindingLauncher} is the asynchronous half. The runtime attests
 * its active execution synchronously, hands that execution (never a session id) to {@link BindingLauncher.launch},
 * and waits on the promise it returns; the worker's end comes from the broker's `worker-exited` frame.
 *
 * The session is read from the execution by the ToolRuntime's attestation and from nothing else; the lease is the
 * broker's answer relayed unchanged (Core mints no `assignmentId`); the binding must still be live, and the gate
 * still open, when the worker has started. A worker that started under a binding that died meanwhile is revoked.
 * Nothing here is provided on a Context: the launcher reaches `subprocess-local` only through the locked root slot
 * (`installBrokerLauncher` in `broker-capabilities.ts`).
 * @module @deepseek-ai/dsh/broker-launch
 */

import type {
  BindingBrokerClient, BindingIssuer, BindingResolver, BrokerGate, WorkerExit,
} from './broker-binding.ts'

/** What the runtime asks for; the same shape `subprocess-local` declares (`BrokerLaunchRequest`). */
export interface LaunchRequest {
  readonly execution: unknown
  readonly argv: readonly string[]
  readonly cwd: string
  readonly env: Readonly<Record<string, string>>
  readonly graceMs: number
}

export type LaunchResult =
  | { readonly ok: true; readonly pid: number; readonly workerId: string; readonly exited: Promise<WorkerExit> }
  | { readonly ok: false; readonly reason: string }

function denied(reason: string): LaunchResult {
  return Object.freeze({ ok: false, reason })
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
   * @param input A {@link LaunchRequest}; anything else is refused, because the slot this is reached through is
   *   readable by any code in the process.
   * @returns The started worker, whose `exited` settles from `worker-exited`, or a refusal. Never throws.
   */
  async launch(input: unknown): Promise<LaunchResult> {
    try {
      if (input === null || typeof input !== 'object') return denied('invalid-request')
      const { execution, argv, cwd, env, graceMs } = input as Record<keyof LaunchRequest, unknown>
      if (!Array.isArray(argv) || typeof cwd !== 'string' || env === null || typeof env !== 'object' || typeof graceMs !== 'number') {
        return denied('invalid-request')
      }
      // Both read the execution before any await.
      const sessionId = this.attest(execution)
      const lease = await this.resolver.lease(execution)
      if (!lease.ok) return denied(lease.reason)
      const started = await this.client.start({
        assignmentId: lease.assignmentId, argv: argv as string[], cwd, env: env as Record<string, string>, graceMs,
      })
      if (!started.ok) return denied(started.reason)
      // The worker exists. If the binding was revoked or the gate closed while the broker started it, kill it.
      if (sessionId === undefined || !this.gate.isActive() || !('bindingId' in this.issuer.lookup(sessionId))) {
        if (sessionId !== undefined) void this.issuer.revoke(sessionId)
        return denied(this.gate.isActive() ? 'revoked' : 'gate-inactive')
      }
      return Object.freeze({ ok: true, pid: started.pid, workerId: started.workerId, exited: started.exited })
    } catch {
      return denied('assignment-unavailable')
    }
  }
}
