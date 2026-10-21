import { describe, expect, it } from 'vitest'
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
})
