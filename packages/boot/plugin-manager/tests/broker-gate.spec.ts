/** Profile management stays unavailable while the trusted subprocess broker is configured, in either initialization order. */
import { chmodSync, existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import { withFileLock } from '@deepseek-ai/dsh-atomic-write'
import { expect, it, onTestFinished, vi } from 'vitest'
import PluginManager, { type Config } from '../src/index.ts'
import * as tool from '../src/tools.ts'

const BROKER_CONFIG = { socketPath: '/run/dsh-broker/broker.sock' }
const MANAGER_REFUSAL = /profile management cannot start while a subprocess broker is configured/
const TOOL_REFUSAL = /installing bundles cannot be offered while a subprocess broker is configured/

const MANAGER_CONFIG: Required<Omit<Config, 'registry'>> = {
  pnpmCommand: 'pnpm', outputBytes: 16384, lockWaitMs: 1000, inspectTimeoutMs: 1000,
  githubConnectionTimeoutMs: 1000, idleTimeoutMs: 1000, fallbackRegistries: [],
}

/** A Context with the manager's injected services stubbed over an empty profile directory. */
function managerFixture(): { ctx: Context; profileDir: string } {
  const profileDir = mkdtempSync(join(tmpdir(), 'plugin-manager-broker-gate-'))
  const ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose(); rmSync(profileDir, { recursive: true, force: true }) })
  ctx.reflect.provide('loader', {} as never)
  ctx.reflect.provide('profileContext', { dir: profileDir, installAnchor: join(profileDir, 'installation.json'), startedBundles: [] } as never)
  return { ctx, profileDir }
}

/** Provide `subprocessBrokerConfig` from a provider that stays pending until the returned function releases it. */
function pendingBrokerConfig(ctx: Context): () => Promise<void> {
  let release!: () => void
  const gate = new Promise<void>(resolve => { release = resolve })
  const provider = ctx.plugin({
    name: 'pending-broker-config',
    apply: async (inner: Context) => {
      inner.provide('subprocessBrokerConfig', BROKER_CONFIG)
      await gate
    },
  })
  return async () => {
    release()
    await (await provider).dispose()
  }
}

/** A Context with the tool's injected services stubbed; `register` records every tool registration. */
function toolFixture(): { ctx: Context; register: ReturnType<typeof vi.fn> } {
  const ctx = new Context()
  onTestFinished(() => ctx.fiber.dispose())
  const register = vi.fn(() => () => {})
  ctx.reflect.provide('tools', { register } as never)
  ctx.reflect.provide('pluginManager', {} as never)
  ctx.reflect.provide('sandboxPolicy', {} as never)
  return { ctx, register }
}

it('refuses manager construction when the broker config is registered first, leaving the profile and service untouched', async () => {
  const { ctx, profileDir } = managerFixture()
  ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)

  await expect((async () => { await ctx.plugin(PluginManager, MANAGER_CONFIG) })()).rejects.toThrow(MANAGER_REFUSAL)

  expect(ctx.reflect.get('pluginManager', false)).toBeUndefined()
  expect(readdirSync(profileDir)).toEqual([])
})

it('refuses manager construction while the broker config provider is registered but not yet active', async () => {
  const { ctx, profileDir } = managerFixture()
  const stop = pendingBrokerConfig(ctx)
  onTestFinished(async () => { await stop() })
  await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
  expect(ctx.reflect.get('subprocessBrokerConfig')).toBeUndefined()

  await expect((async () => { await ctx.plugin(PluginManager, MANAGER_CONFIG) })()).rejects.toThrow(MANAGER_REFUSAL)

  expect(ctx.reflect.get('pluginManager', false)).toBeUndefined()
  expect(readdirSync(profileDir)).toEqual([])
})

it('constructs the manager when no broker is configured', async () => {
  const { ctx } = managerFixture()

  await ctx.plugin(PluginManager, MANAGER_CONFIG)

  expect(ctx.pluginManager).toBeInstanceOf(PluginManager)
})

it('refuses to apply the tool when the broker config is registered, registering nothing', async () => {
  const { ctx, register } = toolFixture()
  ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)

  await expect((async () => { await ctx.plugin(tool) })()).rejects.toThrow(TOOL_REFUSAL)

  expect(register).not.toHaveBeenCalled()
})

it('refuses to apply the tool while the broker config provider is registered but not yet active', async () => {
  const { ctx, register } = toolFixture()
  const stop = pendingBrokerConfig(ctx)
  onTestFinished(async () => { await stop() })
  await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })

  await expect((async () => { await ctx.plugin(tool) })()).rejects.toThrow(TOOL_REFUSAL)

  expect(register).not.toHaveBeenCalled()
})

it('registers the tool when no broker is configured', async () => {
  const { ctx, register } = toolFixture()

  await ctx.plugin(tool)

  expect(register).toHaveBeenCalledTimes(1)
})

const MUTATIONS: [string, (manager: PluginManager) => Promise<unknown>][] = [
  ['setVersionExemption', manager => manager.setVersionExemption('pkg@1.0.0', '0.0.0', false)],
  ['setPluginEnabled', manager => manager.setPluginEnabled('missing' as never, true)],
  ['setBundleEnabled', manager => manager.setBundleEnabled('missing-bundle', true)],
  ['installBundle', manager => manager.installBundle('missing-bundle')],
  ['removeBundle', manager => manager.removeBundle('missing-bundle')],
]
const LATE_REFUSAL = /profile management cannot change the profile while a subprocess broker is configured/

it.each(MUTATIONS)('refuses %s on a live manager once the broker config appears, leaving the profile untouched', async (_name, mutate) => {
  const { ctx, profileDir } = managerFixture()
  await ctx.plugin(PluginManager, MANAGER_CONFIG)
  const manager = ctx.pluginManager
  const disposeConfig = ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)

  await expect(mutate(manager)).rejects.toThrow(LATE_REFUSAL)

  expect(readdirSync(profileDir)).toEqual([])
  await disposeConfig()
})

it.each(MUTATIONS)('refuses %s on a live manager while the broker config provider is registered but not yet active', async (_name, mutate) => {
  const { ctx, profileDir } = managerFixture()
  await ctx.plugin(PluginManager, MANAGER_CONFIG)
  const manager = ctx.pluginManager
  const stop = pendingBrokerConfig(ctx)
  onTestFinished(async () => { await stop() })
  await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
  expect(ctx.reflect.get('subprocessBrokerConfig')).toBeUndefined()

  await expect(mutate(manager)).rejects.toThrow(LATE_REFUSAL)

  expect(readdirSync(profileDir)).toEqual([])
})

const DECLARED_REFUSAL = /profile management cannot change the profile while a subprocess broker is configured/

it.each(MUTATIONS)('keeps refusing %s after the broker config is disposed with no call in between', async (_name, mutate) => {
  const { ctx, profileDir } = managerFixture()
  await ctx.plugin(PluginManager, MANAGER_CONFIG)
  const manager = ctx.pluginManager
  const disposeConfig = ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)
  await disposeConfig()
  expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeUndefined()

  await expect(mutate(manager)).rejects.toThrow(DECLARED_REFUSAL)

  expect(readdirSync(profileDir)).toEqual([])
})

it.each(MUTATIONS)('keeps refusing %s after a pending broker config provider is disposed before it became active', async (_name, mutate) => {
  const { ctx, profileDir } = managerFixture()
  await ctx.plugin(PluginManager, MANAGER_CONFIG)
  const manager = ctx.pluginManager
  const stop = pendingBrokerConfig(ctx)
  onTestFinished(async () => { await stop() })
  await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })
  await stop()
  expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeUndefined()

  await expect(mutate(manager)).rejects.toThrow(DECLARED_REFUSAL)

  expect(readdirSync(profileDir)).toEqual([])
})

it.each(['stays active', 'is disposed'] as const)('refuses a mutation queued on the profile lock when the broker config appears and %s before the lock frees', async (outcome) => {
  const { ctx, profileDir } = managerFixture()
  await ctx.plugin(PluginManager, MANAGER_CONFIG)
  const manager = ctx.pluginManager
  const lockHeld = Promise.withResolvers<undefined>()
  const freeLock = Promise.withResolvers<undefined>()
  const holder = withFileLock(join(profileDir, 'package.json'), async () => {
    lockHeld.resolve(undefined)
    await freeLock.promise
  })
  await lockHeld.promise
  const queued = manager.removeBundle('missing-bundle')
  const settled = queued.then(() => undefined, (error: unknown) => error)
  await new Promise(resolve => setTimeout(resolve, 100))
  const disposeConfig = ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)
  if (outcome === 'is disposed') await disposeConfig()
  else onTestFinished(async () => { await disposeConfig() })

  freeLock.resolve(undefined)
  await holder

  const failure = await settled
  expect(failure).toBeInstanceOf(Error)
  expect((failure as Error).message).toMatch(DECLARED_REFUSAL)
  expect(readdirSync(profileDir)).toEqual([])
})

/** A pnpm stand-in that records every launch in `marker` and prints a registry URL. */
function recordingPnpm(): { command: string; marker: string } {
  const dir = mkdtempSync(join(tmpdir(), 'plugin-manager-broker-gate-pnpm-'))
  onTestFinished(() => { rmSync(dir, { recursive: true, force: true }) })
  const marker = join(dir, 'launched')
  const command = join(dir, 'pnpm')
  writeFileSync(command, `#!/bin/sh\ntouch '${marker}'\necho https://registry.npmjs.org/\n`)
  chmodSync(command, 0o700)
  return { command, marker }
}

const PNPM_READS: [string, (manager: PluginManager) => Promise<unknown>][] = [
  ['registries', manager => manager.registries()],
  ['inspect', manager => manager.inspect('some-package')],
]
const READ_REFUSAL = /profile management cannot (read registries|inspect a spec) while a subprocess broker is configured/

/** A manager over a readable empty profile, so `inspect` reaches its registry read; `pnpm` is the recording stand-in. */
async function readerFixture(): Promise<{ ctx: Context; marker: string }> {
  const { ctx, profileDir } = managerFixture()
  const { command, marker } = recordingPnpm()
  writeFileSync(join(profileDir, 'package.json'), '{}')
  writeFileSync(join(profileDir, 'installation.json'), '{}')
  await ctx.plugin(PluginManager, { ...MANAGER_CONFIG, pnpmCommand: command })
  return { ctx, marker }
}

it.each(PNPM_READS)('launches the configured pnpm for %s when no broker was ever declared', async (_name, read) => {
  const { ctx, marker } = await readerFixture()

  await read(ctx.pluginManager).catch(() => undefined)

  expect(existsSync(marker)).toBe(true)
})

it.each(PNPM_READS)('refuses %s without launching pnpm while the broker config is active', async (_name, read) => {
  const { ctx, marker } = await readerFixture()
  const disposeConfig = ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)
  onTestFinished(async () => { await disposeConfig() })

  await expect(read(ctx.pluginManager)).rejects.toThrow(READ_REFUSAL)

  expect(existsSync(marker)).toBe(false)
})

it.each(PNPM_READS)('refuses %s without launching pnpm after the broker config is disposed with no call in between', async (_name, read) => {
  const { ctx, marker } = await readerFixture()
  await ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)()

  await expect(read(ctx.pluginManager)).rejects.toThrow(READ_REFUSAL)

  expect(existsSync(marker)).toBe(false)
})

it.each(PNPM_READS)('refuses %s without launching pnpm while the broker config provider is registered but not yet active', async (_name, read) => {
  const { ctx, marker } = await readerFixture()
  const stop = pendingBrokerConfig(ctx)
  onTestFinished(async () => { await stop() })
  await vi.waitFor(() => { expect(ctx.reflect.get('subprocessBrokerConfig', false)).toBeDefined() })

  await expect(read(ctx.pluginManager)).rejects.toThrow(READ_REFUSAL)

  expect(existsSync(marker)).toBe(false)
})

it('refuses manager construction after a broker config was provided and disposed', async () => {
  const { ctx, profileDir } = managerFixture()
  await ctx.reflect.provide('subprocessBrokerConfig', BROKER_CONFIG)()

  await expect((async () => { await ctx.plugin(PluginManager, MANAGER_CONFIG) })()).rejects.toThrow(MANAGER_REFUSAL)

  expect(ctx.reflect.get('pluginManager', false)).toBeUndefined()
  expect(readdirSync(profileDir)).toEqual([])
})
