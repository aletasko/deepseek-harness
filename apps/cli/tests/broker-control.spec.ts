/**
 * The broker control connection is opened once by the launcher, before the tree mounts (see `src/broker-control.ts`).
 * Nothing here talks to a broker: the client is the deny client, or a stand-in that counts.
 */
import { Context } from '@deepseek-ai/cordis'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { assignmentUnavailableClient, type BindingBrokerClient } from '../src/broker-binding.ts'
import { claimBrokerControl, CONTROL_CLAIM_KEY, openBrokerControl } from '../src/broker-control.ts'

const BROKER = Object.freeze({ socketPath: '/run/dsh/broker.sock' })
const live: Context[] = []
afterEach(async () => { await Promise.all(live.splice(0).map(ctx => ctx.fiber.dispose())) })

function root(): Context {
  const ctx = new Context()
  live.push(ctx)
  return ctx
}

describe('the process-wide control claim', () => {
  it('lets one opener hold it and refuses a second until the first releases', () => {
    const holder = {}
    const release = claimBrokerControl(holder)
    expect(() => claimBrokerControl(holder)).toThrow(/already open in this process.*second control client/)
    release()
    expect(() => { claimBrokerControl(holder)() }).not.toThrow()
  })

  it('is released only by its own holder, and releasing twice frees nothing that belongs to a later opener', () => {
    const holder = {}
    const first = claimBrokerControl(holder)
    first()
    const second = claimBrokerControl(holder)
    first()
    expect(() => claimBrokerControl(holder)).toThrow(/already open/)
    second()
    claimBrokerControl(holder)()
  })

  it('is shared by every copy of the module: the claim lives on the holder under a registry symbol', () => {
    const holder = {}
    claimBrokerControl(holder)
    const descriptor = Object.getOwnPropertyDescriptor(holder, CONTROL_CLAIM_KEY)!
    expect(CONTROL_CLAIM_KEY).toBe(Symbol.for('dsh.broker-control.claim'))
    expect(descriptor).toMatchObject({ writable: false, configurable: false, enumerable: false })
    // The box cannot be swapped for a fresh one to reset the claim.
    expect(() => { (holder as Record<symbol, unknown>)[CONTROL_CLAIM_KEY] = { owner: undefined } }).toThrow(TypeError)
    expect(Reflect.deleteProperty(holder, CONTROL_CLAIM_KEY)).toBe(false)
    expect(() => claimBrokerControl(holder)).toThrow(/already open/)
  })

  it('fails closed on a claim record that is not one', () => {
    const holder = {}
    Object.defineProperty(holder, CONTROL_CLAIM_KEY, { value: 'planted', writable: false, configurable: false })
    expect(() => claimBrokerControl(holder)).toThrow(/not in a usable state/)
  })

  it('defaults to the process: a second claim through the default holder is refused, and releasing frees it', () => {
    const release = claimBrokerControl()
    try {
      expect(() => claimBrokerControl()).toThrow(/already open/)
    } finally {
      release()
    }
    claimBrokerControl()()
  })
})

describe('opening the control connection', () => {
  it('claims before it connects, so a second opener never creates a client', () => {
    const holder = {}
    const connect = vi.fn((): BindingBrokerClient => assignmentUnavailableClient)
    const ctx = root()
    expect(openBrokerControl(ctx, BROKER, connect, holder)).toBe(assignmentUnavailableClient)
    expect(connect).toHaveBeenCalledTimes(1)
    expect(connect).toHaveBeenCalledWith(BROKER)
    expect(() => openBrokerControl(root(), BROKER, connect, holder)).toThrow(/already open in this process/)
    expect(connect).toHaveBeenCalledTimes(1)
  })

  it('releases the claim when the owning context is disposed', async () => {
    const holder = {}
    const ctx = root()
    openBrokerControl(ctx, BROKER, () => assignmentUnavailableClient, holder)
    await ctx.fiber.dispose()
    expect(() => { claimBrokerControl(holder)() }).not.toThrow()
  })

  it('closes the connection when the owning context is disposed, before the claim is released', async () => {
    const holder = {}
    const events: string[] = []
    const client: BindingBrokerClient = {
      ...assignmentUnavailableClient,
      closeControl: () => { events.push(`closed; claim held: ${(() => { try { claimBrokerControl(holder); return 'no' } catch { return 'yes' } })()}`) },
    }
    const ctx = root()
    openBrokerControl(ctx, BROKER, () => client, holder)
    await ctx.fiber.dispose()
    expect(events).toEqual(['closed; claim held: yes'])
    expect(() => { claimBrokerControl(holder)() }).not.toThrow()
  })

  it('releases the claim at once when connecting throws, and rethrows', () => {
    const holder = {}
    expect(() => openBrokerControl(root(), BROKER, () => { throw new Error('connect failed') }, holder)).toThrow('connect failed')
    claimBrokerControl(holder)()
  })

  it('hands back the client the launcher passed (here the deny client; the launcher passes createBrokerClient)', async () => {
    const client = openBrokerControl(root(), BROKER, () => assignmentUnavailableClient, {})
    expect(await client.bind({ sessionId: 's', workspaceId: 'w', kind: 'attach', cwd: '/x' })).toEqual({ ok: false, reason: 'assignment-unavailable' })
    expect(Object.isFrozen(client)).toBe(true)
  })
})
