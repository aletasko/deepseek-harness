import { describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import Timer from '@deepseek-ai/cordis-plugin-timer'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRegistry from '@deepseek-ai/dsh-tools'
import DynamicCordisRunnerService from '../src/index.ts'
import { AGENT_A, mount, REVERSE_TOOL_CODE, running, setup } from './helpers.ts'

/**
 * The Host vm is not a security boundary, so model-defined Host code must never
 * run while the trusted subprocess broker is configured, in either order of
 * the two services' initialization.
 */

const BROKER_CONFIG = { socketPath: '/run/dsh-broker/broker.sock' }
const BROKER_REFUSAL = /subprocess broker is configured/

describe('dynamic Host runner with a subprocess broker', () => {
  it('refuses construction when the broker is configured first and registers no service', async () => {
    const ctx = new Context()
    await ctx.plugin(Timer)
    await ctx.plugin(SystemPrompt)
    await ctx.plugin(ToolRegistry)
    ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)

    await expect((async () => { await ctx.plugin(DynamicCordisRunnerService) })()).rejects.toThrow(BROKER_REFUSAL)

    expect(ctx.reflect.get('dynamicCordisRunner', false)).toBeUndefined()
    expect(ctx.tools.get('reverse_text')).toBeUndefined()
  })

  it('refuses activation without evaluating Host code when the broker is configured after construction', async () => {
    const harness = await setup()
    harness.ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)

    await expect(mount(harness, REVERSE_TOOL_CODE)).rejects.toThrow(BROKER_REFUSAL)

    expect(harness.ctx.tools.get('reverse_text')).toBeUndefined()
    expect(running(harness.runner, AGENT_A).every(row => !row.running)).toBe(true)
  })

  it('keeps the existing behavior when no broker is configured', async () => {
    const harness = await setup()

    await mount(harness, REVERSE_TOOL_CODE)

    expect(harness.ctx.tools.get('reverse_text')).toBeDefined()
  })

  it('refuses invoke on an already-live run once the broker is configured, and keeps refusing after the config is gone', async () => {
    const harness = await setup()
    const { pluginId, packageId } = harness.runner.define({
      sessionId: AGENT_A.id, plugin: { kind: 'new', idPrefix: 'live' }, name: 'live-double', purpose: 'spec fixture',
      code: { host: "harness.handle('double', async (args) => args.value * 2)\nreturn { name: 'doubler', apply() {} }" },
    })
    const receipt = await harness.runner.run(AGENT_A, pluginId, packageId, 'run')
    if (!receipt.ok) throw new Error(receipt.message)
    await expect(harness.runner.invoke(pluginId, receipt.pluginRunId, 'double', { value: 2 })).resolves.toEqual({ ok: true, value: 4 })

    const disposeConfig = harness.ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)
    await expect(harness.runner.invoke(pluginId, receipt.pluginRunId, 'double', { value: 2 })).rejects.toThrow(/cannot be invoked while a subprocess broker is configured/)

    await disposeConfig()
    await expect(harness.runner.invoke(pluginId, receipt.pluginRunId, 'double', { value: 3 })).rejects.toThrow(/cannot be invoked while a subprocess broker is configured/)
  })

  it('refuses invoke after the broker config was provided and disposed with no call in between', async () => {
    const harness = await setup()
    const pluginId = await mount(harness, "harness.handle('ping', async () => 'pong')\nreturn { name: 'pinger', apply() {} }")
    const runId = harness.runner.inventory().find(row => row.pluginId === pluginId)?.activeRun?.pluginRunId
    if (runId === undefined) throw new Error('fixture did not start')
    await harness.ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)()
    expect(harness.ctx.reflect.get('subprocessBrokerConfig', false)).toBeUndefined()

    await expect(harness.runner.invoke(pluginId, runId, 'ping', null)).rejects.toThrow(BROKER_REFUSAL)
  })

  it('refuses invoke after a pending broker config provider was disposed before it became active', async () => {
    const harness = await setup()
    const pluginId = await mount(harness, "harness.handle('ping', async () => 'pong')\nreturn { name: 'pinger', apply() {} }")
    const runId = harness.runner.inventory().find(row => row.pluginId === pluginId)?.activeRun?.pluginRunId
    if (runId === undefined) throw new Error('fixture did not start')
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const provider = harness.ctx.plugin({
      name: 'pending-then-disposed-broker-config',
      apply: async (inner: Context) => {
        inner.provide('subprocessBrokerConfig', BROKER_CONFIG)
        await gate
      },
    })
    await vi.waitFor(() => { expect(harness.ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
    release()
    await (await provider).dispose()
    expect(harness.ctx.reflect.get('subprocessBrokerConfig', false)).toBeUndefined()

    await expect(harness.runner.invoke(pluginId, runId, 'ping', null)).rejects.toThrow(BROKER_REFUSAL)
  })

  it('refuses activation without evaluating Host code after the broker config was provided and disposed', async () => {
    const harness = await setup()
    await harness.ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)()

    await expect(mount(harness, REVERSE_TOOL_CODE)).rejects.toThrow(BROKER_REFUSAL)

    expect(harness.ctx.tools.get('reverse_text')).toBeUndefined()
    expect(running(harness.runner, AGENT_A).every(row => !row.running)).toBe(true)
  })

  it('refuses construction after the broker config was provided and disposed and registers no service', async () => {
    const ctx = new Context()
    await ctx.plugin(Timer)
    await ctx.plugin(SystemPrompt)
    await ctx.plugin(ToolRegistry)
    await ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)()

    await expect((async () => { await ctx.plugin(DynamicCordisRunnerService) })()).rejects.toThrow(BROKER_REFUSAL)

    expect(ctx.reflect.get('dynamicCordisRunner', false)).toBeUndefined()
  })

  it('refuses invoke while the broker config provider is registered but not yet active', async () => {
    const harness = await setup()
    const pluginId = await mount(harness, "harness.handle('ping', async () => 'pong')\nreturn { name: 'pinger', apply() {} }")
    const runId = harness.runner.inventory().find(row => row.pluginId === pluginId)?.activeRun?.pluginRunId
    if (runId === undefined) throw new Error('fixture did not start')
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const provider = harness.ctx.plugin({
      name: 'pending-broker-config',
      apply: async (inner: Context) => {
        inner.provide('subprocessBrokerConfig', BROKER_CONFIG)
        await gate
      },
    })
    try {
      await vi.waitFor(() => { expect(harness.ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
      expect(harness.ctx.reflect.get('subprocessBrokerConfig')).toBeUndefined()
      await expect(harness.runner.invoke(pluginId, runId, 'ping', null)).rejects.toThrow(BROKER_REFUSAL)
    } finally {
      release()
      await (await provider).dispose()
    }
  })

  it('refuses to restart Host code for a plugin that is already running, leaving it stopped', async () => {
    const harness = await setup()
    const { pluginId, packageId } = harness.runner.define({
      sessionId: AGENT_A.id, plugin: { kind: 'new', idPrefix: 'again' }, name: 'again', purpose: 'spec fixture',
      code: { host: "return { name: 'again', apply() {} }" },
    })
    const first = await harness.runner.run(AGENT_A, pluginId, packageId, 'run')
    expect(first.ok).toBe(true)
    harness.ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)

    const second = await harness.runner.run(AGENT_A, pluginId, packageId, 'run')

    expect(second).toMatchObject({ ok: false })
    expect(JSON.stringify(second)).toMatch(BROKER_REFUSAL)
    expect(running(harness.runner, AGENT_A).every(row => !row.running)).toBe(true)
  })
})
