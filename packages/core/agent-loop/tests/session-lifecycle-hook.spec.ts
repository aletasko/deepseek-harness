/**
 * The host lifecycle hook (see `src/lifecycle-hook.ts`): the broker-required launcher revokes what it bound to a
 * session BEFORE the agent loop cancels anything, and is told when a child is published under a live parent.
 */
import { describe, expect, it, onTestFinished } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import LlmRuntime from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime, { defineContentToolFixture } from '@deepseek-ai/dsh-tools'
import AgentRegistry, { type Agent } from '@deepseek-ai/dsh-agent'
import AgentLoop, { SESSION_LIFECYCLE_HOOK, type SessionLifecycleHook } from '@deepseek-ai/dsh-agent-loop'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import { MockAdapter, textResponse, toolCallResponse } from './mock-adapter.ts'

async function harness(adapter: MockAdapter): Promise<Context> {
  const ctx = new Context()
  await ctx.plugin(LlmRuntime)
  await ctx.plugin(SessionStore)
  await ctx.plugin(SessionProjectionRegistry)
  await ctx.plugin(SystemPrompt, { personaPrefix: '' })
  await ctx.plugin(ToolRuntime)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(AgentLoop, { agents: [] })
  ctx.llm.registerAdapter(['mock'], adapter)
  onTestFinished(() => ctx.fiber.dispose())
  return ctx
}

/** Define the hook the way the launcher does: on the root, once, non-writable and non-configurable. */
function install(ctx: Context, hook: SessionLifecycleHook): void {
  Object.defineProperty(ctx.root, SESSION_LIFECYCLE_HOOK, { value: hook, writable: false, configurable: false, enumerable: false })
}

const send = (agent: Agent, text: string): void => {
  agent.followup(createUserMessage({ content: [{ type: 'text', text }], source: { kind: 'user' } }))
}

async function until(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1000 && !predicate(); i++) await new Promise(resolve => setTimeout(resolve, 0))
  if (!predicate()) throw new Error('until: condition never held')
}

const model = { provider: 'mock', model: 'mock' }
const idle = (): Promise<never> => new Promise(() => {})

describe('revocation comes first in teardown', () => {
  it('is awaited before the loop is cancelled, while a tool is still running and the session is still live', async () => {
    const ctx = await harness(new MockAdapter([toolCallResponse('call-1', 'hold', {}), textResponse('done')]))
    const log: string[] = []
    let toolSignal: AbortSignal | undefined
    ctx.tools.register(defineContentToolFixture({
      name: 'hold', description: 'runs until aborted', parameters: {},
      execute: async (_args, exec) => {
        toolSignal = exec.signal
        await new Promise<void>((resolve) => { exec.signal.addEventListener('abort', () => { log.push('tool-aborted'); resolve() }) })
        return [{ type: 'text', text: 'aborted' }]
      },
    }))
    const ack = Promise.withResolvers<undefined>()
    install(ctx, {
      revoke: async (sessionId) => {
        log.push(`revoke:${sessionId}:toolAborted=${toolSignal?.aborted}:inStore=${ctx.sessions.get(SessionId(sessionId)) !== undefined}:agent=${ctx.agents.get(SessionId(sessionId)) !== undefined}`)
        await ack.promise
        log.push('revoke-acked')
      },
      childPublished: idle,
    })
    const handle = await ctx.agents.create({ sessionId: SessionId('s1'), agentOptions: model })
    send(handle.agent, 'go')
    await until(() => toolSignal !== undefined)

    const disposing = handle.dispose()
    await until(() => log.length > 0)
    await new Promise(resolve => setTimeout(resolve, 30))
    // The revoke is outstanding: nothing has been cancelled and nothing has left the registries.
    expect(log).toEqual(['revoke:s1:toolAborted=false:inStore=true:agent=true'])
    expect(toolSignal?.aborted).toBe(false)
    ack.resolve(undefined)
    await disposing
    expect(log).toEqual(['revoke:s1:toolAborted=false:inStore=true:agent=true', 'revoke-acked', 'tool-aborted'])
    expect(ctx.sessions.get(SessionId('s1'))).toBeUndefined()
  })

  it('runs once per agent however many owners race the teardown, and for an agent that never published', async () => {
    const ctx = await harness(new MockAdapter([textResponse('ok')]))
    const revoked: string[] = []
    install(ctx, { revoke: async (id) => { revoked.push(id) }, childPublished: idle })
    const handle = await ctx.agents.create({ sessionId: SessionId('s1'), agentOptions: model })
    await Promise.all([handle.dispose(), handle.dispose(), ctx.fiber.dispose()])
    expect(revoked).toEqual(['s1'])

    const second = await harness(new MockAdapter([textResponse('ok')]))
    const rolledBack: string[] = []
    install(second, { revoke: async (id) => { rolledBack.push(id) }, childPublished: idle })
    await expect(second.agents.create({
      sessionId: SessionId('s2'), agentOptions: model,
      setup: () => { throw new Error('setup failed') },
    })).rejects.toThrow('setup failed')
    expect(rolledBack).toEqual(['s2'])
  })

  it('does not stop the teardown when the hook fails, and the failure is reported', async () => {
    const ctx = await harness(new MockAdapter([textResponse('ok')]))
    install(ctx, { revoke: async () => { throw new Error('broker down') }, childPublished: idle })
    const handle = await ctx.agents.create({ sessionId: SessionId('s1'), agentOptions: model })
    await expect(handle.dispose()).rejects.toThrow('broker down')
    expect(ctx.sessions.get(SessionId('s1'))).toBeUndefined()
    expect(ctx.agents.get(SessionId('s1'))).toBeUndefined()
  })

  it('changes nothing without a hook, or with something that is not a hook', async () => {
    const ctx = await harness(new MockAdapter([textResponse('ok')]))
    const plain = await ctx.agents.create({ sessionId: SessionId('plain'), agentOptions: model })
    await plain.dispose()
    Object.defineProperty(ctx.root, SESSION_LIFECYCLE_HOOK, { value: { revoke: 'nope' }, configurable: false })
    const odd = await ctx.agents.create({ sessionId: SessionId('odd'), agentOptions: model })
    await odd.dispose()
    expect(ctx.sessions.get(SessionId('odd'))).toBeUndefined()
  })
})

describe('children are announced, not awaited', () => {
  it('tells the hook about a child published under a live parent, from inside the parent tool execution', async () => {
    const ctx = await harness(new MockAdapter([toolCallResponse('call-1', 'spawn', {}), textResponse('done'), textResponse('child')]))
    const seen: { sessionId: string; parentSessionId: string; attestedAs: string | undefined; childLive: boolean }[] = []
    install(ctx, {
      revoke: async () => {},
      childPublished: async (child) => {
        const exec = ctx.tools.currentActiveExecution()
        seen.push({
          ...child,
          attestedAs: exec === undefined ? undefined : ctx.tools.attestActiveExecution(exec).sessionId,
          childLive: ctx.sessions.get(SessionId(child.sessionId)) !== undefined && ctx.agents.get(SessionId(child.sessionId)) !== undefined,
        })
      },
    })
    let child: Agent | undefined
    ctx.tools.register(defineContentToolFixture({
      name: 'spawn', description: 'create one child agent', parameters: {},
      execute: async (_args, exec) => {
        const handle = await exec.agent!.ctx.agents.create({
          sessionId: SessionId('child-1'), agentOptions: model, parentAgent: exec.agent!,
          meta: { parentSession: exec.agent!.id },
        })
        child = handle.agent
        return [{ type: 'text', text: 'spawned' }]
      },
    }))
    const root = await ctx.agents.create({ sessionId: SessionId('root'), agentOptions: model })
    send(root.agent, 'go')
    await until(() => child !== undefined)
    // The root itself is not a child; the child is announced once, live, and attributed to the parent's execution.
    expect(seen).toEqual([{ sessionId: 'child-1', parentSessionId: 'root', attestedAs: 'root', childLive: true }])
    await root.agent.whenIdle()
  })

  it('never fails or delays publication because of the hook', async () => {
    const ctx = await harness(new MockAdapter([textResponse('ok')]))
    install(ctx, {
      revoke: async () => {},
      childPublished: () => { throw new Error('sync failure') },
    })
    const root = await ctx.agents.create({ sessionId: SessionId('root'), agentOptions: model })
    const child = await root.agent.ctx.agents.create({ sessionId: SessionId('c1'), agentOptions: model, parentAgent: root.agent })
    expect(ctx.agents.get(SessionId('c1'))).toBe(child.agent)

    const second = await harness(new MockAdapter([textResponse('ok')]))
    install(second, { revoke: async () => {}, childPublished: idle })
    const secondRoot = await second.agents.create({ sessionId: SessionId('root'), agentOptions: model })
    // A hook that never answers cannot hold the child back.
    const pending = await secondRoot.agent.ctx.agents.create({ sessionId: SessionId('c1'), agentOptions: model, parentAgent: secondRoot.agent })
    expect(second.agents.get(SessionId('c1'))).toBe(pending.agent)

    const third = await harness(new MockAdapter([textResponse('ok')]))
    install(third, { revoke: async () => {}, childPublished: () => Promise.reject(new Error('async failure')) })
    const thirdRoot = await third.agents.create({ sessionId: SessionId('root'), agentOptions: model })
    const rejected = await thirdRoot.agent.ctx.agents.create({ sessionId: SessionId('c1'), agentOptions: model, parentAgent: thirdRoot.agent })
    expect(third.agents.get(SessionId('c1'))).toBe(rejected.agent)
  })

  it('does not announce a root agent, or an agent created without a parent', async () => {
    const ctx = await harness(new MockAdapter([textResponse('ok')]))
    const announced: unknown[] = []
    install(ctx, { revoke: async () => {}, childPublished: async (child) => { announced.push(child) } })
    await ctx.agents.create({ sessionId: SessionId('root'), agentOptions: model })
    await ctx.agentLoop.create(SessionId('direct'), model)
    expect(announced).toEqual([])
  })
})
