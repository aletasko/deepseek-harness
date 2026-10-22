/**
 * The broker control connection is opened once, by the launcher, before the plugin tree mounts. The broker
 * accepts one Core control connection at a time (`core_owner.py`), so a connection the launcher already holds is
 * what makes a second client refuse; this module is the in-process half of that rule. {@link claimBrokerControl}
 * lets exactly one opener hold the connection at a time in the process, and every path that creates a client goes
 * through {@link openBrokerControl}, so the launcher's own code cannot open a second one by accident, including
 * through a second bundled copy of this module (the claim lives on `globalThis` under a registry symbol).
 *
 * What this is not: a plugin that writes its own socket code never calls it. The refusal that matters there is the
 * broker's, and it only holds because the launcher connected first. The claim is a process-wide flag that Host
 * code can reach; it stops a second launcher-style opener, not a hostile one.
 * @module @deepseek-ai/dsh/broker-control
 */

import type { Context } from '@deepseek-ai/cordis'
import type { BindingBrokerClient } from './broker-binding.ts'
import type { BrokerConfig } from './broker-required.ts'

/** Registry-wide key of the process-wide claim, so duplicate copies of this module share one claim. */
export const CONTROL_CLAIM_KEY: unique symbol = Symbol.for('dsh.broker-control.claim')

interface ClaimBox {
  owner: object | undefined
}

function boxOf(holder: object): ClaimBox {
  if (!Object.hasOwn(holder, CONTROL_CLAIM_KEY)) {
    Object.defineProperty(holder, CONTROL_CLAIM_KEY, {
      value: { owner: undefined } satisfies ClaimBox, writable: false, configurable: false, enumerable: false,
    })
  }
  const box: unknown = (holder as Record<symbol, unknown>)[CONTROL_CLAIM_KEY]
  if (box === null || typeof box !== 'object' || !('owner' in box)) {
    throw new Error('dsh: the broker control claim is not in a usable state; refusing to open a control connection')
  }
  return box as ClaimBox
}

/**
 * Take the process's one broker control claim.
 * @param holder Where the claim lives; `globalThis` unless a test supplies its own.
 * @returns The release, idempotent; only the holder's own release frees the claim.
 * @throws when the claim is already held: a second control client in this process is refused.
 */
export function claimBrokerControl(holder: object = globalThis): () => void {
  const box = boxOf(holder)
  if (box.owner !== undefined) {
    throw new Error('dsh: a broker control connection is already open in this process; refusing a second control client')
  }
  const token = {}
  box.owner = token
  return () => { if (box.owner === token) box.owner = undefined }
}

/**
 * Open the control connection for a broker-required launch: claim first, then connect, so that a second opener
 * fails before it creates anything. The claim is released when the context is disposed, or at once if connecting
 * throws. Call it before the plugin tree mounts and before anything else is built from the client.
 * @param ctx The root context the launch owns.
 * @param broker The trusted broker config.
 * @param connect Builds the client. Today the launcher passes the client that refuses everything.
 * @param holder Test seam for the claim's location.
 * @returns The one client for this process.
 */
export function openBrokerControl(
  ctx: Context,
  broker: BrokerConfig,
  connect: (broker: BrokerConfig) => BindingBrokerClient,
  holder?: object,
): BindingBrokerClient {
  const release = claimBrokerControl(holder)
  try {
    const client = connect(broker)
    ctx.effect(() => release, 'broker control connection')
    return client
  } catch (error) {
    release()
    throw error
  }
}
