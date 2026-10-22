/**
 * Opt-in broker-required startup guard. It is deny-only: the declaration is an inherited-environment value any
 * same-user process may omit, so these tests assert refusals and tightening, never a grant.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { PatchOptions } from '@deepseek-ai/cordis-plugin-include'
import { Context } from '@deepseek-ai/cordis'
import { boot, composeEntries, readProfilePatches, type Profile } from '@deepseek-ai/dsh-app-boot'
import { createLaunchEnvironmentSnapshot } from '@deepseek-ai/dsh-launch-environment'
import { installProxyFromEnvironment } from '@deepseek-ai/dsh-http-proxy'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  assertBrokerBootInputs, assertBrokerTree, auditBrokerTree, BROKER_SOCKET_ENV, readBrokerRequirement,
} from '../src/broker-required.ts'
import { runProfile } from '../src/profile-boot.ts'

const probe = vi.hoisted(() => ({
  /** One record per constructed probe: what the constructor could see at that moment. */
  constructed: [] as { id: string; config: unknown; declared: boolean }[],
  /** Disposers of `subprocessBrokerConfig` providers, captured as they are registered. */
  disposers: [] as (() => void)[],
  /** Probe ids whose `appReady` listener fired, i.e. readiness was committed. */
  ready: [] as string[],
  /** Set by a test to make the probe withdraw the broker config from inside its constructor. */
  withdrawConfig: false,
}))

vi.mock('@deepseek-ai/dsh-app-boot', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@deepseek-ai/dsh-app-boot')>()
  return { ...actual, boot: vi.fn(), installFailLoud: vi.fn() }
})
vi.mock('@deepseek-ai/dsh-http-proxy', () => ({ installProxyFromEnvironment: vi.fn() }))
// The Loader imports bare specifiers through its own native resolver, which `vi.mock` cannot intercept, so probes
// are real packages written to a temp directory. They report through this shared object.
const PROBE_SOURCE = (id: string): string => `
export default class Probe {
  constructor(ctx) {
    const probe = globalThis.__brokerProbe
    probe.constructed.push({
      id: ${JSON.stringify(id)},
      config: ctx.get('subprocessBrokerConfig'),
      declared: ctx.reflect.props['subprocessBrokerConfig'] !== undefined,
    })
    ctx.get('appReady')?.onReady(() => { probe.ready.push(${JSON.stringify(id)}) })
    if (probe.withdrawConfig) probe.disposers.splice(0).forEach(dispose => dispose())
  }
}
`

function writePackage(root: string, name: string, id: string): void {
  const dir = join(root, 'node_modules', ...name.split('/'))
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, 'package.json'), JSON.stringify({ name, version: '1.0.0', type: 'module', exports: './index.mjs' }))
  writeFileSync(join(dir, 'index.mjs'), PROBE_SOURCE(id))
}

const SOCKET = '/run/dsh/broker.sock'
const homes: string[] = []

function launchEnv(values: Record<string, string>) {
  return createLaunchEnvironmentSnapshot([{ source: 'process', values }])
}
const brokerEnv = launchEnv({ [BROKER_SOCKET_ENV]: SOCKET })

interface Fixture {
  home: string
  profile: Profile
  installAnchor: string
  /** Absolute path of a `--patch` overlay file with `rows` as its content. */
  overlay(name: string, rows: PatchOptions[]): string
}

/** A synthetic application-owned profile whose single bundle layer inserts `rows`. */
function fixture(rows: PatchOptions[], extra: { profilePatches?: PatchOptions[]; localPackage?: boolean } = {}): Fixture {
  const home = mkdtempSync(join(tmpdir(), 'dsh-broker-required-'))
  homes.push(home)
  mkdirSync(join(home, 'runtime'))
  const installed = { 'broker-probe-a': 'a', 'broker-probe-b': 'b', '@deepseek-ai/dsh-hmr': 'hmr' }
  for (const [name, id] of Object.entries(installed)) writePackage(join(home, 'runtime'), name, id)
  writeFileSync(join(home, 'runtime/package.json'), JSON.stringify({
    name: 'test-runtime', version: '1.0.0', dependencies: Object.fromEntries(Object.keys(installed).map(name => [name, '*'])),
  }))
  const dependencies = extra.localPackage === true ? { 'test-local': '*' } : {}
  writeFileSync(join(home, 'package.json'), JSON.stringify({ name: 'test-bundle', version: '1.0.0', dependencies }))
  if (extra.localPackage === true) {
    mkdirSync(join(home, 'node_modules/test-local'), { recursive: true })
    writeFileSync(join(home, 'node_modules/test-local/package.json'), '{"name":"test-local","version":"1.0.0"}')
  }
  const profile: Profile = {
    name: 'broker', dir: home, patchPath: join(home, 'profile.patch.yml'), skippedBundles: [],
    patches: extra.profilePatches ?? [],
    layers: [{ packageName: 'test-bundle', packageDir: home, patchPaths: [], patches: [{ insert: rows }] }],
  }
  return {
    home, profile, installAnchor: join(home, 'runtime/package.json'),
    overlay(name, overlayRows) {
      const file = join(home, name)
      writeFileSync(file, JSON.stringify(overlayRows))
      return file
    },
  }
}

function launch(fx: Fixture, environment = brokerEnv, patchFiles: string[] = []) {
  return runProfile({
    environment, profile: 'broker', patchFiles, args: [],
    resolvedProfile: { profile: fx.profile, installAnchor: fx.installAnchor },
  })
}

const probeRows: PatchOptions[] = [
  { id: 'probe-a', name: 'broker-probe-a' },
  { id: 'probe-b', name: 'broker-probe-b' },
]
const dynamicOff = { __jsExpr: "!ctx.get('profileContext')" }

/** Run the real `boot`, recording the provider disposer and the context state seen before `prepare` runs. */
async function useRealBoot(): Promise<{ before: { propsBeforePrepare: boolean }[]; contexts: Context[] }> {
  const actual = await vi.importActual<typeof import('@deepseek-ai/dsh-app-boot')>('@deepseek-ai/dsh-app-boot')
  const seen = { before: [] as { propsBeforePrepare: boolean }[], contexts: [] as Context[] }
  vi.mocked(boot).mockImplementation((name, root, patches, prepare, base) => actual.boot(name, root, patches, async (hostCtx) => {
    seen.before.push({ propsBeforePrepare: hostCtx.reflect.props['subprocessBrokerConfig'] !== undefined })
    seen.contexts.push(hostCtx)
    const provide = hostCtx.reflect.provide.bind(hostCtx.reflect)
    vi.spyOn(hostCtx.reflect, 'provide').mockImplementation((...args: Parameters<typeof provide>) => {
      const dispose = provide(...args)
      if (args[0] === 'subprocessBrokerConfig') probe.disposers.push(dispose)
      return dispose
    })
    await prepare?.(hostCtx)
  }, base))
  return seen
}

beforeEach(() => {
  ;(globalThis as { __brokerProbe?: unknown }).__brokerProbe = probe
  probe.constructed.length = 0
  probe.disposers.length = 0
  probe.ready.length = 0
  probe.withdrawConfig = false
  vi.stubEnv('DSH_HOME', mkdtempSync(join(tmpdir(), 'dsh-broker-home-')))
  homes.push(process.env.DSH_HOME!)
  vi.spyOn(process, 'on').mockReturnValue(process)
  vi.mocked(installProxyFromEnvironment).mockClear()
  vi.mocked(installProxyFromEnvironment).mockResolvedValue(vi.fn().mockResolvedValue(undefined))
})
afterEach(() => {
  delete (globalThis as { __brokerProbe?: unknown }).__brokerProbe
  vi.restoreAllMocks()
  vi.unstubAllEnvs()
  vi.mocked(boot).mockReset()
  for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true })
})

describe('broker declaration', () => {
  it.each(['relative.sock', './broker.sock', '', 'run/dsh.sock', '/run/dsh\0.sock'])('rejects %j', (value) => {
    expect(() => readBrokerRequirement(launchEnv({ [BROKER_SOCKET_ENV]: value }))).toThrow(/absolute socket path/)
  })

  it('reads an absolute path, and nothing when the variable is absent', () => {
    expect(readBrokerRequirement(brokerEnv)).toEqual({ socketPath: SOCKET })
    expect(readBrokerRequirement(launchEnv({}))).toBeUndefined()
  })

  it('reads only the inherited layer', () => {
    const environment = createLaunchEnvironmentSnapshot([
      { source: 'process', values: {} },
      { source: 'project-env', path: '/p/.env', values: { [BROKER_SOCKET_ENV]: 'relative.sock' } },
    ])
    expect(readBrokerRequirement(environment)).toBeUndefined()
  })
})

describe('composed tree check', () => {
  const forbidden = [
    '@deepseek-ai/dsh-cordis-host-runner',
    '@deepseek-ai/dsh-plugin-manager',
    '@deepseek-ai/dsh-plugin-manager/tools',
    '@deepseek-ai/dsh-hmr',
    '@deepseek-ai/dsh-config-editor',
    '@deepseek-ai/dsh-settings',
  ]

  it.each(forbidden)('rejects an active %s', (name) => {
    expect(() => assertBrokerTree([{ id: 'x', name }])).toThrow(name)
    expect(() => assertBrokerTree([{ id: 'x', name, disabled: false }])).toThrow(name)
    expect(() => assertBrokerTree([{ id: 'x', name, disabled: null }])).toThrow(name)
  })

  it.each(forbidden)('rejects %s whose disabled value is a dynamic expression', (name) => {
    expect(() => assertBrokerTree([{ id: 'x', name, disabled: dynamicOff as never }])).toThrow(/dynamic expression/)
  })

  it('admits a forbidden row only when its disabled value is the literal true', () => {
    expect(() => assertBrokerTree(forbidden.map((name, index) => ({ id: `x${index}`, name, disabled: true })))).not.toThrow()
  })

  it('reads the composed value: a later layer re-enables, or disables, an earlier row', () => {
    const base: PatchOptions[] = [{ insert: [{ id: 'hmr', name: '@deepseek-ai/dsh-hmr', disabled: true }] }]
    expect(() => assertBrokerTree(composeEntries([base]))).not.toThrow()
    expect(() => assertBrokerTree(composeEntries([base, [{ id: 'hmr', disabled: false }]]))).toThrow(/dsh-hmr/)
    expect(() => assertBrokerTree(composeEntries([base, [{ id: 'hmr', disabled: dynamicOff }]]))).toThrow(/dynamic/)
    const active: PatchOptions[] = [{ insert: [{ id: 'hmr', name: '@deepseek-ai/dsh-hmr' }] }]
    expect(() => assertBrokerTree(composeEntries([active, [{ id: 'hmr', disabled: true }]]))).not.toThrow()
  })

  it('descends into groups, including a disabled one, and rejects a non-literal group config', () => {
    const child = { id: 'm', name: '@deepseek-ai/dsh-plugin-manager' }
    expect(() => assertBrokerTree([{ id: 'g', name: 'cordis:group', group: true, config: [child] }])).toThrow(/g\/m/)
    expect(() => assertBrokerTree([{ id: 'g', name: 'cordis:group', group: true, disabled: true, config: [child] }]))
      .toThrow(/g\/m/)
    expect(() => assertBrokerTree([{ id: 'g', name: 'cordis:group', group: true, config: dynamicOff }]))
      .toThrow(/not a literal entry list/)
  })

  it.each(['./local.ts', '/abs/plugin.js', 'file:///x/plugin.js', 'cordis:include', 'node:fs'])(
    'rejects the unidentifiable module specifier %s', (name) => {
      expect(() => assertBrokerTree([{ id: 'x', name }])).toThrow(/not a package specifier/)
    })

  it('rejects a row without a string module specifier', () => {
    expect(() => assertBrokerTree([{ id: 'x' } as never])).toThrow(/plain string/)
  })

  it('admits ordinary packages', () => {
    expect(() => assertBrokerTree([{ id: 'a', name: '@deepseek-ai/dsh-session' }, { id: 'b', name: 'left-pad' }])).not.toThrow()
  })
})

describe('boot inputs', () => {
  const clean = { localPackageNames: [], linkedRoots: [], entries: [] }

  it('admits empty layers and no local packages', () => {
    expect(() => assertBrokerBootInputs({ profilePatches: [], homePatches: undefined, resolution: clean })).not.toThrow()
    expect(() => assertBrokerBootInputs({ profilePatches: [], homePatches: [], resolution: clean })).not.toThrow()
  })

  it.each([
    ['profile patch', { profilePatches: [{ id: 'x', disabled: true }] }, /profile patch layer/],
    ['home patch', { homePatches: [{ id: 'x', disabled: true }] }, /home patch layer/],
    ['local package', { resolution: { ...clean, localPackageNames: ['pkg'] } }, /profile-local packages: pkg/],
    ['linked package', { resolution: { ...clean, linkedRoots: [{ name: 'lnk', realPath: '/x' }] } }, /linked profile packages: lnk/],
    ['profile-scoped package', {
      resolution: { ...clean, entries: [{ name: 'dep', packageDir: '/d', version: '1', declarer: '/p', scope: 'profile' as const }] },
    }, /profile-scoped packages: dep/],
  ])('rejects a %s', (_label, override, message) => {
    expect(() => assertBrokerBootInputs({ profilePatches: [], homePatches: undefined, resolution: clean, ...override }))
      .toThrow(message)
  })
})

describe('runProfile without the declaration', () => {
  it('keeps the normal boot: profile and home layers mount, nothing is provided', async () => {
    const fx = fixture([{ id: 'hmr', name: '@deepseek-ai/dsh-hmr' }], {
      profilePatches: [{ id: 'hmr', config: { profile: true } }], localPackage: true,
    })
    writeFileSync(join(process.env.DSH_HOME!, 'cordis.patch.yml'), '- id: hmr\n  config: { home: true }\n')
    const ctx = new Context()
    ctx.provide('loader', { create: vi.fn() })
    let patches: unknown
    vi.mocked(boot).mockImplementation(async (_name, _root, given, setup) => {
      patches = given
      await setup?.(ctx)
      return ctx
    })
    try {
      await launch(fx, launchEnv({}))
      const expected = readProfilePatches('dsh', {
        name: 'broker', dir: fx.home, patchPath: fx.profile.patchPath, installAnchor: fx.installAnchor,
        startedBundles: ['test-bundle'], cwd: process.cwd(), home: process.env.DSH_HOME!, overlays: [],
        telemetryDisabledEnv: undefined,
      }, fx.profile)
      expect(patches).toEqual(expected)
      expect(expected).toHaveLength(3)
      expect(ctx.reflect.props['subprocessBrokerConfig']).toBeUndefined()
      expect(ctx.get('subprocessBrokerConfig')).toBeUndefined()
    } finally {
      await ctx.fiber.dispose()
    }
  })

  it('boots a real tree, and mounts a forbidden service as before', async () => {
    const seen = await useRealBoot()
    const fx = fixture([...probeRows, { id: 'hmr', name: '@deepseek-ai/dsh-hmr' }])
    const { ctx } = await launch(fx, launchEnv({}))
    try {
      expect(probe.constructed.map(row => row.id).sort()).toEqual(['a', 'b', 'hmr'])
      expect(probe.constructed.every(row => row.config === undefined && !row.declared)).toBe(true)
      expect(seen.contexts).toHaveLength(1)
    } finally {
      await ctx.fiber.dispose()
    }
  })
})

describe('runProfile refuses before anything mounts', () => {
  async function expectRefused(fx: Fixture, message: RegExp, environment = brokerEnv, patchFiles: string[] = []) {
    await expect(launch(fx, environment, patchFiles)).rejects.toThrow(message)
    expect(boot).not.toHaveBeenCalled()
    expect(probe.constructed).toEqual([])
  }

  it.each(['relative.sock', ''])('rejects the socket %j before the proxy or any file work', async (value) => {
    const fx = fixture(probeRows)
    await expectRefused(fx, /absolute socket path/, launchEnv({ [BROKER_SOCKET_ENV]: value }))
    expect(installProxyFromEnvironment).not.toHaveBeenCalled()
  })

  it.each([
    ['plugin-manager', '@deepseek-ai/dsh-plugin-manager'],
    ['host runner', '@deepseek-ai/dsh-cordis-host-runner'],
    ['hmr', '@deepseek-ai/dsh-hmr'],
    ['config editor', '@deepseek-ai/dsh-config-editor'],
    ['settings', '@deepseek-ai/dsh-settings'],
  ])('rejects an active %s inserted by the base layer', async (_label, name) => {
    await expectRefused(fixture([...probeRows, { id: 'forbidden', name }]), new RegExp(name))
  })

  it('rejects a forbidden row inserted by a command-line overlay', async () => {
    const fx = fixture(probeRows)
    const overlay = fx.overlay('overlay.json', [{ insert: [{ id: 'runner', name: '@deepseek-ai/dsh-cordis-host-runner' }] }])
    await expectRefused(fx, /dsh-cordis-host-runner/, brokerEnv, [overlay])
  })

  it('rejects an overlay that re-enables a row the base disabled', async () => {
    const fx = fixture([...probeRows, { id: 'hmr', name: '@deepseek-ai/dsh-hmr', disabled: true }])
    const overlay = fx.overlay('overlay.json', [{ id: 'hmr', disabled: false }])
    await expectRefused(fx, /dsh-hmr/, brokerEnv, [overlay])
  })

  it('rejects the shipped-style dynamic disabled expression', async () => {
    const fx = fixture([...probeRows, { id: 'hmr', name: '@deepseek-ai/dsh-hmr', disabled: dynamicOff as never }])
    await expectRefused(fx, /dynamic expression/)
  })

  it('rejects a non-empty profile patch layer', async () => {
    await expectRefused(fixture(probeRows, { profilePatches: [{ id: 'probe-a', config: {} }] }), /profile patch layer/)
  })

  it('rejects a non-empty home patch layer, read once and never mounted', async () => {
    const fx = fixture(probeRows)
    writeFileSync(join(process.env.DSH_HOME!, 'cordis.patch.yml'), '- id: probe-a\n  config: {}\n')
    await expectRefused(fx, /home patch layer/)
  })

  it('rejects a profile-local package', async () => {
    await expectRefused(fixture(probeRows, { localPackage: true }), /profile-local packages: test-local/)
  })

  it('admits an empty home patch and an overlay that disables every forbidden row', async () => {
    const seen = await useRealBoot()
    const fx = fixture([
      ...probeRows,
      { id: 'hmr', name: '@deepseek-ai/dsh-hmr', disabled: dynamicOff as never },
    ])
    writeFileSync(join(process.env.DSH_HOME!, 'cordis.patch.yml'), '[]\n')
    const { ctx } = await launch(fx, brokerEnv, [fx.overlay('off.json', [{ id: 'hmr', disabled: true }])])
    try {
      expect(seen.contexts).toHaveLength(1)
      expect(probe.constructed.map(row => row.id).sort()).toEqual(['a', 'b'])
    } finally {
      await ctx.fiber.dispose()
    }
  })
})

describe('broker config before the first row', () => {
  it('is visible to every plugin constructor, and absent from the context before prepare runs', async () => {
    const seen = await useRealBoot()
    const { ctx } = await launch(fixture(probeRows))
    try {
      expect(seen.before).toEqual([{ propsBeforePrepare: false }])
      expect(probe.ready.sort()).toEqual(['a', 'b'])
      expect(probe.constructed).toHaveLength(2)
      for (const row of probe.constructed) {
        expect(row).toMatchObject({ config: { socketPath: SOCKET }, declared: true })
      }
    } finally {
      await ctx.fiber.dispose()
    }
  })

  it('does not depend on a prior start: a second cold start provides and refuses the same way', async () => {
    const seen = await useRealBoot()
    for (let attempt = 0; attempt < 2; attempt++) {
      const { ctx } = await launch(fixture(probeRows))
      await ctx.fiber.dispose()
    }
    expect(seen.contexts).toHaveLength(2)
    expect(seen.before).toEqual([{ propsBeforePrepare: false }, { propsBeforePrepare: false }])
    expect(probe.constructed.map(row => row.config)).toEqual(Array(4).fill({ socketPath: SOCKET }))
    vi.mocked(boot).mockClear()
    await expect(launch(fixture(probeRows), launchEnv({ [BROKER_SOCKET_ENV]: 'relative.sock' })))
      .rejects.toThrow(/absolute socket path/)
    expect(boot).not.toHaveBeenCalled()
  })
})

describe('pre-ready audit (abort after mount only)', () => {
  it('aborts without committing readiness when the config provider is gone, after the rows mounted', async () => {
    await useRealBoot()
    probe.withdrawConfig = true
    await expect(launch(fixture(probeRows))).rejects.toThrow(/live tree audit failed after mount[\s\S]*missing or pending/)
    expect(probe.ready).toEqual([])
    // The audit cannot undo mount-time effects: the probes already ran.
    expect(probe.constructed.length).toBeGreaterThan(0)
  })

  it('rejects a forbidden row that is live, and a missing or replaced config, in a booted tree', async () => {
    const actual = await vi.importActual<typeof import('@deepseek-ai/dsh-app-boot')>('@deepseek-ai/dsh-app-boot')
    const root = mkdtempSync(join(tmpdir(), 'dsh-broker-audit-'))
    homes.push(root)
    writeFileSync(join(root, 'cordis.yml'), '[]\n')
    writePackage(root, '@deepseek-ai/dsh-hmr', 'hmr')
    writePackage(root, 'broker-probe-a', 'a')
    const ctx = await actual.boot('audit', join(root, 'cordis.yml'), [
      { insert: [{ id: 'hmr', name: '@deepseek-ai/dsh-hmr' }, { id: 'ok', name: 'broker-probe-a' }] },
    ], (hostCtx) => { hostCtx.provide('subprocessBrokerConfig', { socketPath: SOCKET }) })
    try {
      expect(probe.constructed.map(row => row.id).sort()).toEqual(['a', 'hmr'])
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).toThrow(/dsh-hmr is not allowed/)
      expect(() => auditBrokerTree(ctx, { socketPath: '/other.sock' })).toThrow(/replaced/)
      ctx.set('subprocessBrokerConfig', undefined)
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).toThrow(/missing or pending/)
    } finally {
      await ctx.fiber.dispose()
    }
  })

  it('passes a clean tree without counting the bootstrap include as a row', async () => {
    const actual = await vi.importActual<typeof import('@deepseek-ai/dsh-app-boot')>('@deepseek-ai/dsh-app-boot')
    const root = mkdtempSync(join(tmpdir(), 'dsh-broker-audit-'))
    homes.push(root)
    writeFileSync(join(root, 'cordis.yml'), '[]\n')
    writePackage(root, 'broker-probe-a', 'a')
    const ctx = await actual.boot('audit', join(root, 'cordis.yml'), [{ insert: [{ id: 'ok', name: 'broker-probe-a' }] }],
      (hostCtx) => { hostCtx.provide('subprocessBrokerConfig', { socketPath: SOCKET }) })
    try {
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).not.toThrow()
    } finally {
      await ctx.fiber.dispose()
    }
  })
})
