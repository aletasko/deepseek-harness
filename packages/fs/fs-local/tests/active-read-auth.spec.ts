import { afterEach, describe, expect, it } from 'vitest'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context, Service } from '@deepseek-ai/cordis'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime, { defineTool } from '@deepseek-ai/dsh-tools'
import { ToolCallId } from '@deepseek-ai/dsh-llm'
import { FsError, FsReadAuthorizer } from '@deepseek-ai/dsh-fs'
import type { FsReadAccessContext, FsTarget } from '@deepseek-ai/dsh-fs'
import { LocalFileSystem } from '@deepseek-ai/dsh-fs-local'

type TestAgent = { readonly id: string }

class TestAgents extends Service {
  private readonly agents: Map<string, TestAgent>

  constructor(ctx: Context, agents: Map<string, TestAgent>) {
    super(ctx, 'agents')
    this.agents = agents
  }

  get(id: string): TestAgent | undefined {
    return this.agents.get(id)
  }
}

class DelegatedReadAuthorizer extends FsReadAuthorizer {
  readonly checks: string[] = []
  readonly starts: string[] = []
  denySession = 'room-denied'
  allowSession = 'unrelated-allowed'
  barrier: Promise<void> | undefined
  private barrierArrivals = 0
  private releaseBarrier: (() => void) | undefined

  waitForTwoConcurrentReads(): void {
    this.barrier = new Promise<void>(resolve => { this.releaseBarrier = resolve })
  }

  authorizeOpenedTarget(_requested: FsTarget, _opened: FsTarget, _access: FsReadAccessContext): void {}

  override async authorizeAndStart<T>(opened: FsTarget, access: FsReadAccessContext, startRead: () => Promise<T>): Promise<T> {
    if (this.barrier !== undefined) {
      this.barrierArrivals++
      if (this.barrierArrivals === 2) this.releaseBarrier?.()
    }
    await this.barrier
    const actor = this.ctx.tools.attestActiveExecution(access.actor)
    const sessionId = actor.sessionId
    if (sessionId === undefined) throw new FsError('session identity is required', 'FS_PERMISSION_DENIED')
    this.checks.push(sessionId)
    // The delegated ROOM lease denies its own session. The trusted fallback
    // names the one unrelated session it allows; all other sessions deny.
    if (sessionId === this.denySession) throw new FsError(`ROOM read lease denied ${opened.displayPath}`, 'FS_PERMISSION_DENIED')
    if (sessionId !== this.allowSession) throw new FsError('session is outside the trusted read policy', 'FS_PERMISSION_DENIED')
    this.starts.push(sessionId)
    return startRead()
  }
}

let ctx: Context | undefined
let fibers: Array<Awaited<ReturnType<Context['plugin']>>> = []
let directory: string | undefined

afterEach(async () => {
  for (const fiber of fibers.reverse()) await fiber.dispose()
  fibers = []
  if (ctx !== undefined) await ctx.fiber.dispose()
  ctx = undefined
  if (directory !== undefined) await rm(directory, { recursive: true, force: true })
  directory = undefined
})

async function setup() {
  directory = await mkdtemp(join(tmpdir(), 'dsh-fs-active-read-'))
  await writeFile(join(directory, 'room.txt'), 'room content')
  const agents = new Map<string, TestAgent>([
    ['room-denied', { id: 'room-denied' }],
    ['unrelated-allowed', { id: 'unrelated-allowed' }],
  ])
  ctx = new Context()
  fibers.push(await ctx.plugin(SystemPrompt))
  fibers.push(await ctx.plugin(ToolRuntime))
  fibers.push(await ctx.plugin(TestAgents, agents))
  const authorizerFiber = await ctx.plugin(DelegatedReadAuthorizer)
  fibers.push(authorizerFiber)
  fibers.push(await ctx.plugin(LocalFileSystem, { cwd: directory, requireReadAuthorization: true }))
  return { ctx, authorizer: ctx.fsReadAuthorizer as DelegatedReadAuthorizer, agents }
}

describe('guarded reads use the exact active tool execution', () => {
  it('keeps two sessions isolated while the trusted policy can allow the unrelated session', async () => {
    const { ctx, authorizer, agents } = await setup()
    const target = await ctx.fs.resolve('room.txt')
    authorizer.waitForTwoConcurrentReads()
    let capturedRoomActor: object | undefined
    ctx.tools.register(defineTool({
      name: 'guarded-read', description: 'test read', parameters: {},
      output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] },
      async execute(_args, exec) {
        if (exec.agent?.id === 'room-denied') capturedRoomActor = exec
        await expect(ctx.fs.readText(target, exec.signal)).rejects.toMatchObject({ code: 'FS_PERMISSION_DENIED' })
        await expect(ctx.fs.readText(target, exec.signal, { actor: { ...exec } })).rejects.toMatchObject({ code: 'FS_PERMISSION_DENIED' })
        return ctx.fs.readText(target, exec.signal, { actor: exec })
      },
    }))
    const execute = (agent: TestAgent, callId: string) => ctx.tools.execute({
      signal: new AbortController().signal,
      callId: ToolCallId(callId),
      name: 'guarded-read',
      arguments: {},
      agent: agent as never,
    })
    const room = agents.get('room-denied')!
    const unrelated = agents.get('unrelated-allowed')!
    const [denied, allowed] = await Promise.all([
      execute(room, 'room-read'),
      execute(unrelated, 'unrelated-read'),
    ])
    expect(denied.isError).toBe(true)
    expect(JSON.stringify(denied.content)).toContain('ROOM read lease denied')
    expect(allowed.isError).toBe(false)
    if (!allowed.isError) expect(allowed.value).toBe('room content')
    expect(authorizer.checks.sort()).toEqual(['room-denied', 'unrelated-allowed'])
    expect(authorizer.starts).toEqual(['unrelated-allowed'])

    await expect(ctx.fs.readText(target)).rejects.toMatchObject({ code: 'FS_PERMISSION_DENIED' })
    await expect(ctx.fs.readText(target, undefined, { actor: {} })).rejects.toMatchObject({ code: 'FS_PERMISSION_DENIED' })
    await expect(ctx.fs.readText(target, undefined, { actor: capturedRoomActor! })).rejects.toMatchObject({ code: 'FS_PERMISSION_DENIED' })
  })
})
