/**
 * Opt-in broker-required startup guard. It is deny-only: the declaration is an inherited-environment value any
 * same-user process may omit, so these tests assert refusals and tightening, never a grant.
 */
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
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
import { assignmentUnavailableClient, createBindingHost, type BindingHost } from '../src/broker-binding.ts'
import { ASSIGNMENT_RESOLVER_KEY, LIFECYCLE_HOOK_KEY, ROOM_PLUGIN_SPECIFIER } from '../src/broker-capabilities.ts'
import { claimBrokerControl } from '../src/broker-control.ts'
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
  /** What the ROOM-named probe's `apply` received: its argument count and third argument. */
  applied: [] as { arity: number; third: unknown }[],
}))

vi.mock('@deepseek-ai/dsh-app-boot', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@deepseek-ai/dsh-app-boot')>()
  return { ...actual, boot: vi.fn(), installFailLoud: vi.fn() }
})
vi.mock('@deepseek-ai/dsh-http-proxy', () => ({ installProxyFromEnvironment: vi.fn() }))
vi.mock('../src/broker-binding.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/broker-binding.ts')>()
  return { ...actual, createBindingHost: vi.fn(actual.createBindingHost) }
})
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
  probe.applied.length = 0
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
    '@deepseek-ai/cordis-plugin-include',
    '@deepseek-ai/cordis-plugin-include/src/index.ts',
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

  it.each(['#hmr', '#internal/include', '#'])('rejects the import alias %s', (name) => {
    expect(() => assertBrokerTree([{ id: 'x', name }])).toThrow(/import alias/)
    expect(() => assertBrokerTree([{ id: 'x', name, disabled: false }])).toThrow(/import alias/)
    expect(() => assertBrokerTree([{ id: 'x', name, disabled: true }])).not.toThrow()
  })

  it('rejects a forbidden child of a nested group, at any depth', () => {
    const child = { id: 'inc', name: '@deepseek-ai/cordis-plugin-include' }
    const inner = { id: 'inner', name: '@deepseek-ai/cordis-plugin-group', group: true, config: [child] }
    expect(() => assertBrokerTree([{ id: 'outer', name: 'cordis:group', group: true, config: [inner] }]))
      .toThrow(/outer\/inner\/inc/)
    const alias = { id: 'inner', name: 'cordis:group', group: true, config: [{ id: 'a', name: '#hmr' }] }
    expect(() => assertBrokerTree([{ id: 'outer', name: 'cordis:group', group: true, disabled: true, config: [alias] }]))
      .toThrow(/outer\/inner\/a/)
  })

  it('admits @deepseek-ai/dsh-client-hmr: a different package from dsh-hmr, documented as outside the forbidden set', () => {
    expect(() => assertBrokerTree([{ id: 'c', name: '@deepseek-ai/dsh-client-hmr' }])).not.toThrow()
    expect(() => assertBrokerTree([{ id: 'c', name: '@deepseek-ai/dsh-hmr-client' }])).not.toThrow()
  })

  describe('agent preset rows, which carry a second plugin list (S2 review A-1)', () => {
    const PRESET = '@deepseek-ai/dsh-agent-preset'
    const preset = (plugins: unknown, extra: object = {}) => ({ id: 'preset', name: PRESET, config: { id: 'p', plugins }, ...extra })

    it.each(forbidden)('rejects an active %s inside a preset, however the preset itself is flagged', (name) => {
      expect(() => assertBrokerTree([preset([{ id: 'x', name }])])).toThrow(new RegExp(`preset/x: .*${name.replaceAll('/', '\\/')}`))
      expect(() => assertBrokerTree([preset([{ id: 'x', name }], { disabled: true })])).toThrow(/preset\/x/)
      expect(() => assertBrokerTree([preset([{ id: 'x', name, disabled: dynamicOff }])])).toThrow(/dynamic expression/)
    })

    it('rejects the loader rows a preset could mount: Include by name or builtin, a # alias, a path', () => {
      for (const name of ['cordis:include', '@deepseek-ai/cordis-plugin-include', '#hmr', './plugin.js']) {
        expect(() => assertBrokerTree([preset([{ id: 'x', name }])])).toThrow(/preset\/x/)
      }
    })

    it('descends through groups and presets nested in presets', () => {
      const inner = preset([{ id: 'deep', name: '@deepseek-ai/dsh-hmr' }], { id: 'inner' })
      const group = { id: 'g', name: 'cordis:group', group: true, config: [inner] }
      expect(() => assertBrokerTree([preset([group])])).toThrow(/preset\/g\/inner\/deep/)
    })

    it('rejects a preset whose plugin list is not a literal list, and admits one that mounts nothing', () => {
      expect(() => assertBrokerTree([{ id: 'preset', name: PRESET, config: { id: 'p', plugins: dynamicOff } }]))
        .toThrow(/preset: agent preset plugins is not a literal entry list/)
      expect(() => assertBrokerTree([{ id: 'preset', name: PRESET, config: { id: 'p', plugins: 'nope' } }])).toThrow(/literal entry list/)
      expect(() => assertBrokerTree([{ id: 'preset', name: PRESET }])).not.toThrow()
      expect(() => assertBrokerTree([preset([])])).not.toThrow()
    })

    describe('a config that is not a literal object hides its plugin list (S5a review M-1)', () => {
      const jsExpr = { __jsExpr: "({ id: 'x', plugins: [{ id: 'hmr', name: '@deepseek-ai/dsh-hmr' }] })" }
      class Config { plugins = [{ id: 'x', name: '@deepseek-ai/dsh-hmr' }] }
      const hidden: Array<[string, unknown]> = [
        ['a !!js expression', jsExpr],
        ['a string', "({ plugins: [{ name: '@deepseek-ai/dsh-hmr' }] })"],
        ['an array', [{ id: 'x', name: '@deepseek-ai/dsh-hmr' }]],
        ['a number', 7],
        ['a boolean', true],
        ['a class instance', new Config()],
      ]

      it.each(hidden)('refuses a preset whose config is %s, flagged or not', (_name, config) => {
        const row = { id: 'preset', name: PRESET, config }
        expect(() => assertBrokerTree([row])).toThrow(/preset: agent preset config is not a literal object/)
        expect(() => assertBrokerTree([{ ...row, disabled: true }])).toThrow(/preset: agent preset config is not a literal object/)
        expect(() => assertBrokerTree([{ ...row, disabled: dynamicOff }])).toThrow(/agent preset config is not a literal object/)
      })

      it('refuses it inside a group and inside a preset nested in a preset, and names the row', () => {
        const nested = { id: 'inner', name: PRESET, config: jsExpr }
        expect(() => assertBrokerTree([{ id: 'g', name: 'cordis:group', config: [nested] }])).toThrow(/g\/inner: agent preset config is not a literal object/)
        expect(() => assertBrokerTree([preset([nested])])).toThrow(/preset\/inner: agent preset config is not a literal object/)
        expect(() => assertBrokerTree([{ ...nested, name: '@DeepSeek-AI/dsh-Agent-Preset' }])).toThrow(/inner: agent preset config is not a literal object/)
      })

      it('still admits a literal object, one with no prototype, an absent config and a null one', () => {
        expect(() => assertBrokerTree([preset([{ id: 'ok', name: '@deepseek-ai/dsh-persona' }])])).not.toThrow()
        expect(() => assertBrokerTree([{ id: 'preset', name: PRESET, config: Object.assign(Object.create(null) as object, { id: 'p', plugins: [] }) }])).not.toThrow()
        expect(() => assertBrokerTree([{ id: 'preset', name: PRESET }])).not.toThrow()
        expect(() => assertBrokerTree([{ id: 'preset', name: PRESET, config: null }])).not.toThrow()
        expect(() => assertBrokerTree([{ id: 'preset', name: PRESET, config: {} }])).not.toThrow()
      })

      it('does not treat a config that merely contains an expression elsewhere as a hidden list', () => {
        expect(() => assertBrokerTree([{ id: 'preset', name: PRESET, config: { id: 'p', model: jsExpr, plugins: [] } }])).not.toThrow()
      })
    })

    it('admits what the shipped presets carry: ordinary tools, and the plugin manager only literally disabled', () => {
      const rows = [
        { id: 'persona', name: '@deepseek-ai/dsh-persona' },
        { id: 'tool-bash', name: '@deepseek-ai/dsh-tool-bash', disabled: dynamicOff },
        { id: 'tool-plugin-manager', name: '@deepseek-ai/dsh-plugin-manager/tools', disabled: true },
        { id: 'tool-plugin-market', name: '@deepseek-ai/dsh-plugin-market/tools', disabled: true },
      ]
      expect(() => assertBrokerTree([preset(rows)])).not.toThrow()
      expect(() => assertBrokerTree([preset(rows.map(row => row.id === 'tool-plugin-manager' ? { ...row, disabled: false } : row))]))
        .toThrow(/tool-plugin-manager/)
    })

    it('matches the preset package name case-insensitively too', () => {
      expect(() => assertBrokerTree([{ ...preset([{ id: 'x', name: '@deepseek-ai/dsh-hmr' }]), name: '@DeepSeek-AI/dsh-Agent-Preset' }]))
        .toThrow(/preset\/x/)
    })
  })

  describe('names that differ only by case (S2 review A-2)', () => {
    const alternating = (name: string): string => name.replace(
      /[a-z]/gu, (letter, index: number) => index % 2 === 0 ? letter.toUpperCase() : letter,
    )
    it.each(forbidden.map(name => [name, alternating(name)] as const))(
      'rejects %s spelled %s', (_name, spelled) => {
        expect(() => assertBrokerTree([{ id: 'x', name: spelled }])).toThrow(spelled)
        expect(() => assertBrokerTree([{ id: 'x', name: spelled, disabled: false }])).toThrow(spelled)
        expect(() => assertBrokerTree([{ id: 'x', name: spelled, disabled: true }])).not.toThrow()
      })

    it('still admits an ordinary package, and dsh-client-hmr in any case', () => {
      expect(() => assertBrokerTree([{ id: 'a', name: '@DeepSeek-AI/dsh-Session' }, { id: 'c', name: '@DeepSeek-AI/dsh-Client-HMR' }])).not.toThrow()
    })
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

  it('rejects the Include plugin by package name', async () => {
    await expectRefused(fixture([...probeRows, { id: 'inc', name: '@deepseek-ai/cordis-plugin-include', config: { path: 'x.yml' } }]),
      /cordis-plugin-include is not allowed/)
  })

  it.each(['#hmr', '#internal/include'])('rejects the import alias %s', async (name) => {
    await expectRefused(fixture([...probeRows, { id: 'alias', name }]), /import alias/)
  })

  it('rejects the Include plugin or an alias introduced by a command-line overlay', async () => {
    const fx = fixture(probeRows)
    const overlay = fx.overlay('overlay.json', [{ insert: [{ id: 'inc', name: '@deepseek-ai/cordis-plugin-include' }] }])
    await expectRefused(fx, /cordis-plugin-include is not allowed/, brokerEnv, [overlay])
    const aliasOverlay = fx.overlay('alias.json', [{ insert: [{ id: 'alias', name: '#hmr' }] }])
    await expectRefused(fx, /#hmr is an import alias/, brokerEnv, [aliasOverlay])
  })

  it('rejects a forbidden child inside a nested group', async () => {
    const group = {
      id: 'outer', name: 'cordis:group', group: true,
      config: [{ id: 'inner', name: '@deepseek-ai/cordis-plugin-group', group: true, config: [{ id: 'm', name: '@deepseek-ai/dsh-plugin-manager' }] }],
    }
    await expectRefused(fixture([...probeRows, group as never]), /outer\/inner\/m: .*dsh-plugin-manager/)
  })

  it('admits dsh-client-hmr, which is outside the forbidden set', async () => {
    const seen = await useRealBoot()
    const fx = fixture([...probeRows, { id: 'client-hmr', name: '@deepseek-ai/dsh-client-hmr', disabled: true }])
    const { ctx } = await launch(fx)
    try {
      expect(seen.contexts).toHaveLength(1)
      expect(probe.constructed.map(row => row.id).sort()).toEqual(['a', 'b'])
    } finally {
      await ctx.fiber.dispose()
    }
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

  it('does not skip a forbidden row that has no running fiber unless it is literally disabled', async () => {
    const actual = await vi.importActual<typeof import('@deepseek-ai/dsh-app-boot')>('@deepseek-ai/dsh-app-boot')
    const root = mkdtempSync(join(tmpdir(), 'dsh-broker-audit-'))
    homes.push(root)
    writeFileSync(join(root, 'cordis.yml'), '[]\n')
    // dsh-hmr is not installed here, so its import fails and the row stays without a fiber that could start later.
    writePackage(root, 'broker-probe-a', 'a')
    const ctx = await actual.boot('audit', join(root, 'cordis.yml'), [
      { insert: [{ id: 'hmr', name: '@deepseek-ai/dsh-hmr' }, { id: 'ok', name: 'broker-probe-a' }] },
    ], (hostCtx) => { hostCtx.provide('subprocessBrokerConfig', { socketPath: SOCKET }) })
    try {
      const entry = ctx.get('loader')!.entries().find(row => row.options.id === 'hmr')!
      expect(entry.fiber).toBeUndefined()
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).toThrow(/hmr: .*no running fiber/)
      entry.options.disabled = true
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).not.toThrow()
    } finally {
      await ctx.fiber.dispose()
    }
  })

  it('reads the plugin list of a preset row in a booted tree, where the Loader does not list its rows', async () => {
    const actual = await vi.importActual<typeof import('@deepseek-ai/dsh-app-boot')>('@deepseek-ai/dsh-app-boot')
    const root = mkdtempSync(join(tmpdir(), 'dsh-broker-audit-'))
    homes.push(root)
    writeFileSync(join(root, 'cordis.yml'), '[]\n')
    const preset = (plugins: unknown) => ({ id: 'preset', name: '@deepseek-ai/dsh-agent-preset', config: { id: 'p', plugins } })
    const ctx = await actual.boot('audit', join(root, 'cordis.yml'), [{
      insert: [preset([{ id: 'hmr', name: '@DeepSeek-AI/dsh-hmr' }, { id: 'ok', name: '@deepseek-ai/dsh-persona' }])],
    }], (hostCtx) => { hostCtx.provide('subprocessBrokerConfig', { socketPath: SOCKET }) })
    try {
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).toThrow(/preset\/hmr: .*not allowed/)
      const rows = (ctx.get('loader') as unknown as { entries(): { options: { id?: string; config: unknown } }[] }).entries()
      const entry = rows.find(row => row.options.id === 'preset')!
      ;(entry.options.config as { plugins: { disabled?: boolean }[] }).plugins[0]!.disabled = true
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).not.toThrow()
    } finally {
      await ctx.fiber.dispose()
    }
  })

  it.each([
    ['a !!js expression', { __jsExpr: "({ id: 'p', plugins: [{ id: 'hmr', name: '@deepseek-ai/dsh-hmr' }] })" }],
    ['an array', [{ id: 'hmr', name: '@deepseek-ai/dsh-hmr' }]],
    ['a string', "({ plugins: [{ name: '@deepseek-ai/dsh-hmr' }] })"],
  ])('refuses a booted preset row whose declared config is %s (M-1, post-mount)', async (_name, config) => {
    const actual = await vi.importActual<typeof import('@deepseek-ai/dsh-app-boot')>('@deepseek-ai/dsh-app-boot')
    const root = mkdtempSync(join(tmpdir(), 'dsh-broker-audit-'))
    homes.push(root)
    writeFileSync(join(root, 'cordis.yml'), '[]\n')
    const ctx = await actual.boot('audit', join(root, 'cordis.yml'), [{
      insert: [{ id: 'preset', name: '@deepseek-ai/dsh-agent-preset', disabled: true, config }],
    }], (hostCtx) => { hostCtx.provide('subprocessBrokerConfig', { socketPath: SOCKET }) })
    try {
      expect(() => auditBrokerTree(ctx, { socketPath: SOCKET })).toThrow(/preset: agent preset config is not a literal object/)
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

function builtHost(): BindingHost {
  return vi.mocked(createBindingHost).mock.results[0]!.value as BindingHost
}

/** A package under the ROOM specifier whose `apply` reports what it was called with. */
function addRoomProbe(fx: Fixture): void {
  const runtime = join(fx.home, 'runtime')
  const dir = join(runtime, 'node_modules', ...ROOM_PLUGIN_SPECIFIER.split('/'))
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, 'package.json'), JSON.stringify({ name: ROOM_PLUGIN_SPECIFIER, version: '1.0.0', type: 'module', exports: './index.mjs' }))
  writeFileSync(join(dir, 'index.mjs'), `
export const name = 'room-probe'
export function apply(ctx, config, third) { globalThis.__brokerProbe.applied.push({ arity: arguments.length, third }) }
`)
  const manifest = JSON.parse(readFileSync(join(runtime, 'package.json'), 'utf8')) as { dependencies: Record<string, string> }
  manifest.dependencies[ROOM_PLUGIN_SPECIFIER] = '*'
  writeFileSync(join(runtime, 'package.json'), JSON.stringify(manifest))
}

describe('binding host in the launcher', () => {
  it('defines the lifecycle hook and hands ROOM its closed port, only under the declaration', async () => {
    await useRealBoot()
    const fx = fixture([...probeRows, { id: 'room', name: ROOM_PLUGIN_SPECIFIER }])
    addRoomProbe(fx)
    const { ctx } = await launch(fx)
    try {
      expect(probe.applied).toHaveLength(1)
      expect(probe.applied[0]!.arity).toBe(3)
      const port = probe.applied[0]!.third as { seat: unknown; revoke: unknown }
      expect(Object.keys(port).sort()).toEqual(['revoke', 'seat'])
      expect(Object.isFrozen(port)).toBe(true)
      expect(Object.getOwnPropertyDescriptor(ctx.root, LIFECYCLE_HOOK_KEY)).toMatchObject({ writable: false, configurable: false })
      // Nothing about either is a Context name.
      expect(Object.keys(ctx.reflect.props).filter(name => /hook|lifecycle|room|seat|port/iu.test(name))).toEqual([])
    } finally {
      await ctx.fiber.dispose()
    }

    probe.applied.length = 0
    const plain = fixture([...probeRows, { id: 'room', name: ROOM_PLUGIN_SPECIFIER }])
    addRoomProbe(plain)
    const unguarded = await launch(plain, launchEnv({}))
    try {
      expect(probe.applied).toEqual([{ arity: 2, third: undefined }])
      expect(Object.hasOwn(unguarded.ctx.root, LIFECYCLE_HOOK_KEY)).toBe(false)
    } finally {
      await unguarded.ctx.fiber.dispose()
    }
  })

  it('closes the include override, claims the resolver slot and holds the control claim, only under the declaration', async () => {
    await useRealBoot()
    const fx = fixture([...probeRows, { id: 'room', name: ROOM_PLUGIN_SPECIFIER }])
    addRoomProbe(fx)
    const { ctx } = await launch(fx)
    try {
      const builtins = (ctx as unknown as { loader: { builtins: Record<string, unknown> } }).loader.builtins
      const Evil = Object.freeze({ hostile: true })
      // C-1: no plugin can swap the include any more, nor the dictionary it lives in.
      expect(() => { builtins['include'] = Evil }).toThrow(/sealed/)
      expect(() => { (ctx as unknown as { loader: { builtins: unknown } }).loader.builtins = { include: Evil } }).toThrow(TypeError)
      expect(probe.applied).toHaveLength(1)
      expect(probe.applied[0]!.arity).toBe(3)
      // The assignment resolver is the launcher's: a locked symbol property that answers nobody.
      const descriptor = Object.getOwnPropertyDescriptor(ctx.root, ASSIGNMENT_RESOLVER_KEY)!
      expect(descriptor).toMatchObject({ writable: false, configurable: false, enumerable: false })
      expect((descriptor.value as { resolve(id: string): unknown }).resolve('any-session')).toBeUndefined()
      expect(Object.keys(ctx.reflect.props).filter(name => /resolver|assignment/iu.test(name))).toEqual([])
      // Limit 2: the launcher opened the control connection first, so a second opener in this process is refused.
      expect(() => claimBrokerControl()).toThrow(/already open in this process/)
    } finally {
      await ctx.fiber.dispose()
    }
    // Disposal releases the claim; the next launch of the same process may open again.
    const release = claimBrokerControl()
    release()

    const plain = fixture([...probeRows, { id: 'room', name: ROOM_PLUGIN_SPECIFIER }])
    addRoomProbe(plain)
    const unguarded = await launch(plain, launchEnv({}))
    try {
      expect(Object.hasOwn(unguarded.ctx.root, ASSIGNMENT_RESOLVER_KEY)).toBe(false)
      const free = claimBrokerControl()
      free()
    } finally {
      await unguarded.ctx.fiber.dispose()
    }
  })

  it('opens the control connection before anything else is built from the client, and refuses a second launch at once', async () => {
    await useRealBoot()
    const real = await vi.importActual<typeof import('../src/broker-binding.ts')>('../src/broker-binding.ts')
    const heldWhenBuilt: boolean[] = []
    vi.mocked(createBindingHost).mockClear()
    vi.mocked(createBindingHost).mockImplementation((options) => {
      // By the time anything is built from the client, the process's one control claim is already taken.
      try {
        claimBrokerControl()()
        heldWhenBuilt.push(false)
      } catch {
        heldWhenBuilt.push(true)
      }
      return real.createBindingHost(options)
    })
    try {
      const { ctx } = await launch(fixture(probeRows))
      try {
        expect(heldWhenBuilt).toEqual([true])
        // A second launch while the first is alive in this process cannot take the control connection.
        await expect(launch(fixture(probeRows))).rejects.toThrow(/already open in this process/)
      } finally {
        await ctx.fiber.dispose()
      }
    } finally {
      vi.mocked(createBindingHost).mockImplementation(real.createBindingHost)
    }
  })

  it('is built only under the broker declaration, with the denying client, and armed only after the audit', async () => {
    await useRealBoot()
    vi.mocked(createBindingHost).mockClear()
    const { ctx } = await launch(fixture(probeRows))
    try {
      expect(createBindingHost).toHaveBeenCalledTimes(1)
      const [options] = vi.mocked(createBindingHost).mock.calls[0]!
      expect(options).toMatchObject({ broker: { socketPath: SOCKET }, client: assignmentUnavailableClient })
      expect(options.ctx).toBe(ctx)
      expect(builtHost().gate.isActive()).toBe(true)
      // Held by the launcher alone: nothing about it is published on the Context.
      expect(Object.keys(ctx.reflect.props).filter(name => /binding|issuer|assign|resolver/iu.test(name))).toEqual([])
    } finally {
      await ctx.fiber.dispose()
    }
  })

  it('stays unarmed when the post-mount audit aborts startup', async () => {
    await useRealBoot()
    vi.mocked(createBindingHost).mockClear()
    probe.withdrawConfig = true
    await expect(launch(fixture(probeRows))).rejects.toThrow(/live tree audit failed after mount/)
    expect(createBindingHost).toHaveBeenCalledTimes(1)
    expect(builtHost().gate.isActive()).toBe(false)
  })

  it('is not built at all without the declaration', async () => {
    await useRealBoot()
    vi.mocked(createBindingHost).mockClear()
    const { ctx } = await launch(fixture(probeRows), launchEnv({}))
    try {
      expect(createBindingHost).not.toHaveBeenCalled()
    } finally {
      await ctx.fiber.dispose()
    }
  })
})
