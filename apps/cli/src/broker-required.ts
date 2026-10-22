/**
 * Opt-in, deny-only startup guard for a `dsh` launched with
 * `DSH_SUBPROCESS_BROKER_SOCKET` in its inherited environment.
 *
 * The variable is a declaration made by whoever starts this process, not an attestation: another process running
 * as the same user can start its own `dsh` without it. The guard therefore only tightens the process that carries
 * the variable and grants nothing. It does not cover profile edits after boot, direct spawn surfaces outside
 * `subprocess-local`, or the identity of the broker behind the socket.
 * @module @deepseek-ai/dsh/broker-required
 */

import { isAbsolute } from 'node:path'
import { FiberState, type Context } from '@deepseek-ai/cordis'
import type { EntryOptions } from '@deepseek-ai/cordis-plugin-loader'
import type { PatchOptions } from '@deepseek-ai/cordis-plugin-include'
import {
  composeEntries,
  readBundlePatches,
  resolveTelemetryPatch,
  type ProfileContext,
  type Profile,
  type RuntimeResolution,
} from '@deepseek-ai/dsh-app-boot'
import type { LaunchEnvironmentSnapshot } from '@deepseek-ai/dsh-launch-environment'

/** Inherited-environment variable that opts this process into broker-required startup. */
export const BROKER_SOCKET_ENV = 'DSH_SUBPROCESS_BROKER_SOCKET'

/**
 * The trusted broker config shape `subprocess-local` reads from `subprocessBrokerConfig`. Declared here, not
 * imported, so this guard adds no package or project-reference edge to the CLI.
 */
export interface BrokerConfig {
  readonly socketPath: string
}

const TELEMETRY_ROW_ID = 'session-telemetry-otel'

/**
 * Packages that can run or edit Host code in-process, or rewrite the composition after boot.
 *
 * Matching is by module specifier, never by resolved entry: the pre-mount check must not import or resolve
 * anything. A package that re-exports one of these under another name, or a `#` alias that resolves to one,
 * is therefore not recognised by name; `#` specifiers are refused outright for that reason (see
 * {@link ALLOWED_IMPORT_ALIASES}).
 *
 * Known and deliberately admitted (F2): `@deepseek-ai/dsh-client-hmr` is a different package from
 * `@deepseek-ai/dsh-hmr`. It only polls bundle files for the browser client and serves an SSE channel; it
 * does not reload Host plugins or edit the composition, so it is not in this set. Do not read "hmr" in a name
 * as membership.
 */
const FORBIDDEN_PACKAGES = [
  '@deepseek-ai/dsh-cordis-host-runner',
  '@deepseek-ai/dsh-plugin-manager',
  '@deepseek-ai/dsh-hmr',
  '@deepseek-ai/dsh-config-editor',
  '@deepseek-ai/dsh-settings',
  // The Loader imports a bare specifier as-is (no shorthand), so this name loads the real Include plugin, whose
  // `config.path` mounts rows the pre-mount check never saw. The `cordis:include` builtin is refused below.
  '@deepseek-ai/cordis-plugin-include',
] as const

/**
 * `#` subpath-import aliases admitted in broker mode. Empty on purpose: an alias is resolved through the
 * `imports` map of whichever package owns the Loader base URL, so its target cannot be known without resolving it,
 * and no shipped bundle needs one. Adding an entry is a deliberate act that must name a reviewed target.
 */
const ALLOWED_IMPORT_ALIASES: ReadonlySet<string> = new Set()

/** Tree carriers whose `config` list this guard descends into. */
const GROUP_NAMES = new Set(['cordis:group', '@deepseek-ai/cordis-plugin-group'])

function brokerError(message: string): Error {
  return new Error(`dsh: broker-required startup refused: ${message}`)
}

/**
 * Read the opt-in declaration from the inherited layer only.
 * @param environment The launch snapshot taken before any config layer was applied.
 * @returns The trusted broker config, or `undefined` when the variable is absent.
 * @throws when the variable is present but not an absolute path (an empty value counts as present).
 */
export function readBrokerRequirement(environment: LaunchEnvironmentSnapshot): BrokerConfig | undefined {
  const entry = environment.getFrom(BROKER_SOCKET_ENV, ['process'])
  if (entry === undefined) return undefined
  if (entry.value.length === 0 || entry.value.includes('\0') || !isAbsolute(entry.value)) {
    throw brokerError(`${BROKER_SOCKET_ENV} must be an absolute socket path`)
  }
  return Object.freeze({ socketPath: entry.value })
}

/**
 * Why a plugin row cannot be admitted, judged from its module specifier alone.
 * @param options Composed or live entry options.
 * @returns The reason, or `undefined` for a package that is not forbidden.
 */
function inadmissibleName(options: { name?: unknown; group?: unknown; config?: unknown }): string | undefined {
  const { name } = options
  if (typeof name !== 'string' || name.length === 0) return 'its module specifier is not a plain string'
  if (GROUP_NAMES.has(name)) return undefined
  for (const pkg of FORBIDDEN_PACKAGES) {
    if (name === pkg || name.startsWith(`${pkg}/`)) return `${name} is not allowed in broker mode`
  }
  if (name.startsWith('#') && !ALLOWED_IMPORT_ALIASES.has(name)) {
    return `${name} is an import alias this guard cannot resolve`
  }
  // Paths, URLs and builtins such as `cordis:include` load code or rows this guard cannot identify.
  if (name.startsWith('.') || name.startsWith('/') || name.startsWith('\\') || name.includes(':')) {
    return `${name} is not a package specifier this guard can identify`
  }
  return undefined
}

function walkComposed(rows: readonly EntryOptions[], trail: string, failures: string[]): void {
  for (const row of rows) {
    const label = `${trail}${typeof row.id === 'string' ? row.id : '?'}`
    const group = row.group === true || (typeof row.name === 'string' && GROUP_NAMES.has(row.name))
    // Only the literal `true` is a proven-off row; absent, false, null and `!!js` expressions may all run.
    if (row.disabled !== true) {
      const reason = inadmissibleName(row)
      if (reason !== undefined) {
        const dynamic = typeof row.disabled === 'object' && row.disabled !== null
          ? ' (its `disabled` is a dynamic expression; set it to the literal `true`)'
          : ''
        failures.push(`${label}: ${reason}${dynamic}`)
      }
    }
    // A disabled group stays inspected: refusing a forbidden child costs nothing and a skipped one could mount.
    if (!group) continue
    if (row.config === undefined || row.config === null) continue
    if (!Array.isArray(row.config)) {
      failures.push(`${label}: group config is not a literal entry list`)
      continue
    }
    walkComposed(row.config as EntryOptions[], `${label}/`, failures)
  }
}

/**
 * Check the composed entry list that will mount. Pure over the rows: a row is admitted only when its
 * module is a plain package outside {@link FORBIDDEN_PACKAGES}, or when its literal `disabled` is `true`.
 * @param rows Composed rows, as `composeEntries` returns them.
 * @throws when any row is forbidden, opaque or only conditionally disabled; the message lists each one.
 */
export function assertBrokerTree(rows: readonly EntryOptions[]): void {
  const failures: string[] = []
  walkComposed(rows, '', failures)
  if (failures.length > 0) throw brokerError(`composed tree is not admissible:\n  ${failures.join('\n  ')}`)
}

/**
 * Reject profile-level inputs that can change the composition outside the shipped bundles and the command line.
 * @param inputs The profile's own patch layer, the home patch layer, and the pre-import runtime resolution.
 * @throws when either patch layer has content, or any profile-local or linked package exists.
 */
export function assertBrokerBootInputs(inputs: {
  profilePatches: readonly PatchOptions[]
  homePatches: readonly PatchOptions[] | undefined
  resolution: Pick<RuntimeResolution, 'localPackageNames' | 'linkedRoots' | 'entries'>
}): void {
  const failures: string[] = []
  if (inputs.profilePatches.length > 0) failures.push('the profile patch layer is not empty')
  if ((inputs.homePatches?.length ?? 0) > 0) failures.push('the home patch layer is not empty')
  const { localPackageNames, linkedRoots, entries } = inputs.resolution
  if (localPackageNames.length > 0) failures.push(`profile-local packages: ${localPackageNames.join(', ')}`)
  if (linkedRoots.length > 0) failures.push(`linked profile packages: ${linkedRoots.map(root => root.name).join(', ')}`)
  const profileScoped = entries.filter(entry => entry.scope === 'profile')
  if (profileScoped.length > 0) failures.push(`profile-scoped packages: ${profileScoped.map(entry => entry.name).join(', ')}`)
  if (failures.length > 0) throw brokerError(failures.join('; '))
}

/**
 * The patch list to mount in broker mode: bundle layers, then command-line overlays, then the telemetry switch.
 * The profile and home layers were read once and proven empty by {@link assertBrokerBootInputs}, so they are
 * never read again here; the returned list is also the one the composed tree was checked from.
 * @param binName Diagnostic prefix.
 * @param context Profile context from the launcher.
 * @param profile Profile loaded before composition.
 * @returns The checked patch list for `boot`.
 * @throws when the composed rows fail {@link assertBrokerTree}.
 */
export function brokerBootPatches(binName: string, context: ProfileContext, profile: Profile): PatchOptions[] {
  const patches = structuredClone([...readBundlePatches(binName, context, profile), ...context.overlays])
  const telemetry = resolveTelemetryPatch(context.telemetryDisabledEnv,
    composeEntries([patches]).some(row => row.id === TELEMETRY_ROW_ID))
  if (telemetry !== undefined) patches.push(telemetry)
  assertBrokerTree(composeEntries([patches]))
  return patches
}

/**
 * Secondary, post-mount check before readiness is committed. Every effect of a mounted row has already
 * happened, so this can only abort startup; it admits nothing.
 * @param ctx Booted root context.
 * @param expected The config provided before the tree mounted.
 * @throws when the broker config is missing, pending, replaced, or a forbidden row is running or could still start.
 */
export function auditBrokerTree(ctx: Context, expected: BrokerConfig): void {
  const failures: string[] = []
  const active = ctx.get('subprocessBrokerConfig')
  const declared = ctx.reflect.get('subprocessBrokerConfig', false)
  if (active === undefined) failures.push('subprocessBrokerConfig is missing or pending')
  else if (active !== declared || active.socketPath !== expected.socketPath) {
    failures.push('subprocessBrokerConfig was replaced')
  }
  const loader = ctx.get('loader')
  if (loader === undefined) failures.push('the Loader is gone')
  else {
    for (const entry of loader.entries()) {
      const { options } = entry
      // The launcher's own bootstrap include is the root of the composed tree, not a configured row.
      if (options.id === 'include' && options.name === 'cordis:include' && entry.parent.ctx.fiber.entry === undefined) continue
      const reason = inadmissibleName(options)
      if (reason === undefined) continue
      const live = entry.fiber !== undefined && entry.fiber.state !== FiberState.DISPOSED
      // A row without a live fiber may still be pending or have a failed import, and `Entry.refresh()` or an
      // update can start it after readiness is committed. Only the literal `true` is a proven-off row.
      if (live) failures.push(`${entry.id}: ${reason}`)
      else if (options.disabled !== true) {
        failures.push(`${entry.id}: ${reason} (no running fiber, but not literally disabled, so it could still start)`)
      }
    }
  }
  if (failures.length > 0) throw brokerError(`live tree audit failed after mount:\n  ${failures.join('\n  ')}`)
}
