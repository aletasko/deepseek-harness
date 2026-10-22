import type {
  AssignmentGrant, AssignRequest, BindingBrokerClient, BindingGrant, BindingRefusal, BindingRequest, RevokeRequest,
} from '../src/broker-binding.ts'

/** The root broker, simulated. It owns ids, revisions, epochs and assignments; Core owns none of them. */
export class FakeBroker implements BindingBrokerClient {
  readonly log: string[] = []
  readonly binds: BindingRequest[] = []
  readonly revokes: RevokeRequest[] = []
  readonly assigns: AssignRequest[] = []
  readonly issuedAssignments = new Set<string>()
  closed = 0
  /** Test hooks. */
  bindAnswer?: (request: BindingRequest) => Promise<unknown>
  assignAnswer?: (request: AssignRequest) => Promise<unknown>
  revokeAnswer?: (request: RevokeRequest) => Promise<unknown>
  onRevoke?: (request: RevokeRequest) => void
  private epoch: string | undefined
  private counter = 0
  private readonly registry = new Map<string, { bindingId: string; revision: number }>()

  async bind(request: BindingRequest): Promise<BindingGrant | BindingRefusal> {
    this.log.push(`bind:${request.sessionId}`)
    this.binds.push(request)
    if (this.bindAnswer !== undefined) return await this.bindAnswer(request) as BindingGrant
    // A new Core epoch empties the registry, exactly as the broker's CoreOwner revision change does.
    if (this.epoch !== request.coreEpoch) { this.registry.clear(); this.epoch = request.coreEpoch }
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
    if (request.coreEpoch !== this.epoch) return { ok: false, reason: 'stale-core-epoch' }
    if (current?.bindingId !== request.bindingId || current.revision !== request.revision) return { ok: false, reason: 'stale-binding' }
    const assignmentId = `assignment-${this.issuedAssignments.size + 1}-opaque`
    this.issuedAssignments.add(assignmentId)
    return { ok: true, assignmentId, revision: current.revision }
  }

  closeControl(): void { this.closed += 1; this.log.push('close-control') }
}
