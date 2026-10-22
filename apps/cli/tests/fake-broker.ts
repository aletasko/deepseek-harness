import type {
  AssignmentGrant, AssignRequest, BindingBrokerClient, BindingGrant, BindingRefusal, BindingRequest, BrokerStatus,
  RevokeRequest, StartGrant, StartRequest, WorkerExit,
} from '../src/broker-binding.ts'

/** The root broker, simulated. It owns ids, revisions and assignments; Core owns none of them. */
export class FakeBroker implements BindingBrokerClient {
  readonly log: string[] = []
  readonly binds: BindingRequest[] = []
  readonly revokes: RevokeRequest[] = []
  readonly assigns: AssignRequest[] = []
  readonly starts: StartRequest[] = []
  readonly issuedAssignments = new Map<string, string>()
  closed = 0
  /** Test hooks. */
  bindAnswer?: (request: BindingRequest) => Promise<unknown>
  assignAnswer?: (request: AssignRequest) => Promise<unknown>
  revokeAnswer?: (request: RevokeRequest) => Promise<unknown>
  startAnswer?: (request: StartRequest) => Promise<unknown>
  onRevoke?: (request: RevokeRequest) => void
  /** Settle the worker started last, as the broker's `worker-exited` would. */
  exitWorker: (exit: WorkerExit) => void = () => {}
  private counter = 0
  private readonly registry = new Map<string, { bindingId: string; revision: number }>()

  async bind(request: BindingRequest): Promise<BindingGrant | BindingRefusal> {
    this.log.push(`bind:${request.sessionId}`)
    this.binds.push(request)
    if (this.bindAnswer !== undefined) return await this.bindAnswer(request) as BindingGrant
    const grant = { bindingId: `binding-${++this.counter}`, revision: this.counter }
    this.registry.set(request.sessionId, grant)
    return { ok: true, ...grant }
  }

  async revoke(request: RevokeRequest): Promise<{ ok: true } | BindingRefusal> {
    this.log.push(`revoke:${request.sessionId}`)
    this.revokes.push(request)
    this.onRevoke?.(request)
    if (this.revokeAnswer !== undefined) return await this.revokeAnswer(request) as { ok: true }
    this.registry.delete(request.sessionId)
    return { ok: true }
  }

  async assign(request: AssignRequest): Promise<AssignmentGrant | BindingRefusal> {
    this.log.push(`assign:${request.sessionId}`)
    this.assigns.push(request)
    if (this.assignAnswer !== undefined) return await this.assignAnswer(request) as AssignmentGrant
    const current = this.registry.get(request.sessionId)
    if (current?.bindingId !== request.bindingId || current.revision !== request.revision) return { ok: false, reason: 'stale-binding' }
    const assignmentId = `assignment-${this.issuedAssignments.size + 1}-opaque`
    this.issuedAssignments.set(assignmentId, request.bindingId)
    return { ok: true, assignmentId, revision: current.revision }
  }

  async start(request: StartRequest): Promise<StartGrant | BindingRefusal> {
    this.log.push(`start:${request.assignmentId}`)
    this.starts.push(request)
    if (this.startAnswer !== undefined) return await this.startAnswer(request) as StartGrant
    if (!this.issuedAssignments.has(request.assignmentId)) return { ok: false, reason: 'no-lease' }
    const exited = Promise.withResolvers<WorkerExit>()
    this.exitWorker = (exit) => { exited.resolve(exit) }
    return { ok: true, pid: 4242, workerId: `worker-${this.starts.length}`, exited: exited.promise }
  }

  status(): Promise<BrokerStatus | BindingRefusal> {
    return Promise.resolve({ ok: true, status: { bindings: this.registry.size } })
  }

  closeControl(): void { this.closed += 1; this.log.push('close-control') }
}
