import { describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { authorizeSubprocessStart } from '../src/index.ts'
import type { SubprocessStartAuthorizer, SubprocessStartRequest, SubprocessToolExecutionRuntime } from '../src/index.ts'

const request: SubprocessStartRequest = { kind: 'spawn', argv: ['/bin/echo', 'ok'], cwd: '/work' }

function setup(current: () => unknown, attest: (execution: unknown) => unknown) {
  const ctx = new Context()
  const tools: SubprocessToolExecutionRuntime = {
    currentActiveExecution: current,
    attestActiveExecution: attest,
  }
  ctx.provide('tools', tools)
  const authorize = vi.fn((_execution: unknown, _request: SubprocessStartRequest) => true)
  const policy: SubprocessStartAuthorizer = {
    authorize,
  }
  ctx.provide('subprocessStartAuthorizer', policy)
  return { ctx, tools, authorize }
}

describe('per-call subprocess start policy', () => {
  it('denies guarded starts without an active ToolRuntime execution', () => {
    const { ctx, authorize } = setup(() => undefined, () => undefined)
    expect(() => authorizeSubprocessStart(ctx, true, request)).toThrow(/requires an active ToolRuntime execution/)
    expect(authorize).not.toHaveBeenCalled()
  })

  it('fails closed when ToolRuntime rejects forged or stale identity', () => {
    const forged = Object.freeze({ name: 'search', agent: { id: 'room-session' } })
    const { ctx, authorize } = setup(() => forged, () => { throw new Error('not an active body execution') })
    expect(() => authorizeSubprocessStart(ctx, true, request)).toThrow(/no valid active ToolRuntime execution/)
    expect(authorize).not.toHaveBeenCalled()
  })

  it('passes the exact active execution to the trusted policy before the effect', () => {
    const roomExecution = Object.freeze({ name: 'search', agent: { id: 'room-session' } })
    const otherExecution = Object.freeze({ name: 'search', agent: { id: 'other-session' } })
    let current: unknown = roomExecution
    const active = new Set([roomExecution, otherExecution])
    const { ctx, authorize } = setup(() => current, execution => {
      if (!active.has(execution as object)) throw new Error('not active')
      return { direct: true }
    })
    authorize.mockImplementation(execution => execution === roomExecution ? false : true)

    expect(() => authorizeSubprocessStart(ctx, true, request)).toThrow(/denied by host policy/)
    current = otherExecution
    const attackerChosenRequest = { kind: 'spawn' as const, argv: ['/bin/bash', '-c', 'anything'], cwd: '/tmp' }
    expect(() => authorizeSubprocessStart(ctx, true, attackerChosenRequest)).not.toThrow()
    expect(authorize.mock.calls.map(([execution]) => execution)).toEqual([roomExecution, otherExecution])
    expect(authorize.mock.calls.map(([, launch]) => launch)).toEqual([request, attackerChosenRequest])
  })

  it('leaves unguarded requests untouched and does not consult identity', () => {
    const ctx = new Context()
    const authorize = vi.fn()
    expect(() => authorizeSubprocessStart(ctx, false, request)).not.toThrow()
    expect(authorize).not.toHaveBeenCalled()
  })
})
