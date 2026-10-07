import { describe, expect, test } from 'bun:test'
import { ClaudeClient } from '../src/claude-client'
import { createSdkQuery } from '../src/claude-client/sdk-runtime'
import { createSessionState } from '../src/claude-client/client/session-state'
import {
  parseClaudeSessionBindingPolicy,
  parseClaudeSessionOptionsPolicy,
} from '../src/claude-client/policy'
import type {
  ClaudePermissionIntent,
  ClaudeSessionOptions,
  SdkQueryFn,
} from '../src/claude-client/types'

const turnPermission: ClaudePermissionIntent = {
  kind: 'explicit',
  mode: 'plan',
}

const options: ClaudeSessionOptions = {
  cwd: '/tmp', model: 'claude-sonnet-5-5',
  settingSourcesIntent: { kind: 'isolated' }, systemPrompt: 'Caller prompt',
  harnessInstructions: 'Harness instructions', thinkingEffort: 'max',
  allowedTools: ['Read'], disallowedTools: ['Write'],
  ggMcpServer: { command: '/bin/true', serverName: 'gg' },
}

function sdk(messages: Record<string, unknown>[], captured: Record<string, unknown>[] = []): SdkQueryFn {
  return ({ options }) => {
    captured.push(options ?? {})
    const iterator = (async function* () { for (const message of messages) yield message })()
    return Object.assign(iterator, { interrupt: async () => {}, close: () => {} })
  }
}

describe('typed Claude session policy and advanced primitives', () => {
  test('SDK policy appends harness once, preserves caller and isolated sources', async () => {
    const captured: Record<string, unknown>[] = []
    for (const prompt of ['Caller prompt', 'Caller prompt\n\nHarness instructions']) {
      const state = createSessionState({ sessionId: 'bridge', providerSessionRef: 'bridge', sdkSessionRef: 'native',
        options: { ...options, systemPrompt: prompt } })
      await createSdkQuery({ session: state, prompt: 'input', permissionIntent: turnPermission,
        enableThinkingSummaries: true,
        canUseTool: async () => ({ behavior: 'deny', message: 'denied' }), sdkQueryOverride: sdk([], captured) })
    }
    expect(captured[0].systemPrompt).toEqual(captured[1].systemPrompt)
    expect(captured[0].systemPrompt).toEqual({ type: 'preset', preset: 'claude_code', append: 'Caller prompt\n\nHarness instructions' })
    expect(captured[0].settingSources).toEqual([])
    expect(captured[0].permissionMode).toBe('plan')
    expect(captured[0].thinking).toEqual({ type: 'adaptive', display: 'summarized' })
    expect(captured[0].settings).toEqual({ showThinkingSummaries: true })
    expect((captured[0].env as Record<string, string>).CLAUDE_CODE_EFFORT_LEVEL).toBe('max')
  })

  test('typed settings validate cwd, ordering and direct SDK fields fail closed', () => {
    expect(() => parseClaudeSessionOptionsPolicy({ permissionMode: 'plan' })).toThrow()
    expect(() => parseClaudeSessionOptionsPolicy({ permissionIntent: { kind: 'provider_default' }, settingSourcesIntent: { kind: 'isolated' } })).toThrow()
    expect(() => parseClaudeSessionOptionsPolicy({ permissionIntent: { kind: 'explicit', mode: 'impossible' }, settingSourcesIntent: { kind: 'isolated' } })).toThrow()
    expect(() => parseClaudeSessionOptionsPolicy({ permissionIntent: { kind: 'inherit_provider_configuration' }, settingSourcesIntent: { kind: 'explicit', sources: ['project'] } })).toThrow()
    expect(parseClaudeSessionOptionsPolicy({ permissionIntent: { kind: 'inherit_provider_configuration' }, settingSourcesIntent: { kind: 'standard' } }, '/tmp').settingSourcesIntent.kind).toBe('standard')
    expect(() => parseClaudeSessionBindingPolicy({ permissionIntent: turnPermission, settingSourcesIntent: { kind: 'isolated' } }, '/tmp')).toThrow('cannot retain permission authority')
    expect(parseClaudeSessionBindingPolicy({ settingSourcesIntent: { kind: 'standard' } }, '/tmp').settingSourcesIntent.kind).toBe('standard')
  })

  test('rebind verifies SDK initialization cwd/native identity and preserves policy', async () => {
    const captured: Record<string, unknown>[] = []
    const client = new ClaudeClient(() => {}, { mode: 'sdk', sdkQuery: sdk([{ type: 'system', subtype: 'init', cwd: '/tmp', session_id: 'native' }], captured) })
    client.resumeSession('bridge', options, 'provider', 'native')
    const rebound = await client.rebindSession('bridge', '/tmp')
    expect(rebound).toMatchObject({ effectiveCwd: '/tmp', bindingGeneration: 1, providerSessionRef: 'provider', claudeCanonicalSessionRef: 'native' })
    expect(captured[0].resume).toBe('native')
    expect(captured[0].tools).toEqual([])
    expect(captured[0].systemPrompt).toMatchObject({ append: 'Caller prompt\n\nHarness instructions' })
    const bad = new ClaudeClient(() => {}, { mode: 'sdk', sdkQuery: sdk([{ type: 'system', subtype: 'init', cwd: '/wrong', session_id: 'native' }]) })
    bad.resumeSession('bridge', options, 'provider', 'native')
    await expect(bad.rebindSession('bridge', '/tmp')).rejects.toThrow('cwd mismatch')
    const identity = new ClaudeClient(() => {}, { mode: 'sdk', sdkQuery: sdk([{ type: 'system', subtype: 'init', cwd: '/tmp', session_id: 'native-rebound' }]) })
    identity.resumeSession('bridge', options, 'provider', 'native')
    await expect(identity.rebindSession('bridge', '/tmp')).resolves.toMatchObject({
      providerSessionRef: 'provider',
      claudeCanonicalSessionRef: 'native-rebound',
    })
  })

  test('first real turn after rebind must prove the committed cwd again', async () => {
    let invocation = 0
    const client = new ClaudeClient(() => {}, {
      mode: 'sdk',
      sdkQuery: () => {
        invocation += 1
        if (invocation === 1) {
          return (async function* () {
            yield { type: 'system', subtype: 'init', cwd: '/tmp', session_id: 'native' }
          })()
        }
        return (async function* () {
          yield { type: 'system', subtype: 'init', cwd: '/wrong', session_id: 'native' }
          yield { type: 'result', subtype: 'success', session_id: 'native', result: 'done' }
        })()
      },
    })
    client.resumeSession('bridge', options, 'provider', 'native')
    await client.rebindSession('bridge', '/tmp')
    const ack = await client.sendInput(
      'bridge',
      [{ type: 'text', text: 'verify rebound cwd' }],
      undefined,
      turnPermission,
      'max'
    )
    await expect(client.waitForTurn('bridge', ack.turnId, 1000)).resolves.toMatchObject({
      status: 'failed',
    })
  })

  test('compact requires native identity, accepts observed boundary and otherwise does nothing', async () => {
    for (const boundary of [false, true]) {
      const events: unknown[] = []
      const captured: Record<string, unknown>[] = []
      const client = new ClaudeClient(event => events.push(event), { mode: 'sdk',
        sdkQuery: sdk(boundary ? [{ type: 'system', subtype: 'compact_boundary', compact_metadata: { trigger: 'manual', pre_tokens: 120, post_tokens: 30 } }] : [], captured) })
      client.resumeSession('bridge', options, 'provider', 'native')
      expect(await client.compactSession('bridge')).toEqual({ outcome: boundary ? 'accepted' : 'not_performed' })
      expect(captured[0].resume).toBe('native')
      const compactionEvents = events.filter((event: any) => event.event === 'context.compaction') as any[]
      expect(compactionEvents.length).toBe(boundary ? 1 : 0)
      if (boundary) {
        expect(compactionEvents[0]).toMatchObject({
          payload: { phase: 'completed', trigger: 'manual', preTokens: 120, postTokens: 30 },
        })
        expect(compactionEvents[0].payload.requestId).toBeUndefined()
      }
    }
    const fresh = new ClaudeClient(() => {})
    const created = fresh.createSession(options)
    await expect(fresh.compactSession(created.sessionId)).rejects.toThrow('canonical')
  })

  test('busy rejects rebind and compact', async () => {
    let release!: () => void
    const blocked: SdkQueryFn = () => (async function* () {
      await new Promise<void>(resolve => { release = resolve })
      yield { type: 'result', subtype: 'success', session_id: 'native', result: 'done' }
    })()
    const client = new ClaudeClient(() => {}, { mode: 'sdk', sdkQuery: blocked })
    client.resumeSession('bridge', options, 'provider', 'native')
    const ack = await client.sendInput('bridge', [{ type: 'text', text: 'hello' }], undefined, turnPermission, 'max')
    await expect(client.rebindSession('bridge', '/tmp')).rejects.toThrow('busy')
    await expect(client.compactSession('bridge')).rejects.toThrow('busy')
    while (!release) await Promise.resolve()
    release()
    await client.waitForTurn('bridge', ack.turnId, 1000)
  })

  test('SDK turn emits provider-observed permission mode and rejects explicit mismatch', async () => {
    const events: any[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: sdk([
        { type: 'system', subtype: 'init', session_id: 'native', permissionMode: 'plan' },
        { type: 'result', subtype: 'success', session_id: 'native', result: 'done' },
      ]),
    })
    client.resumeSession('bridge', options, 'provider', 'native')
    const ack = await client.sendInput('bridge', [{ type: 'text', text: 'hello' }], undefined, turnPermission, 'max')
    await client.waitForTurn('bridge', ack.turnId, 1000)
    expect(events.find(event => event.event === 'permission.observed')).toMatchObject({
      turnId: ack.turnId,
      payload: { permissionMode: 'plan', resolvedTurnSelection: 'plan' },
    })

    const mismatch = new ClaudeClient(() => {}, {
      mode: 'sdk',
      sdkQuery: sdk([
        { type: 'system', subtype: 'init', session_id: 'native', permissionMode: 'acceptEdits' },
      ]),
    })
    mismatch.resumeSession('bridge', options, 'provider', 'native')
    const mismatchAck = await mismatch.sendInput('bridge', [{ type: 'text', text: 'hello' }], undefined, turnPermission, 'max')
    const result = await mismatch.waitForTurn('bridge', mismatchAck.turnId, 1000)
    expect(result.status).toBe('failed')
  })

  test('protocol handles typed create/resume/rebind/compact and rejects raw policy', async () => {
    const policy = { settingSourcesIntent: options.settingSourcesIntent,
      harnessInstructions: options.harnessInstructions, systemPrompt: options.systemPrompt, thinkingEffort: 'max', cwd: '/tmp' }
    const requests = [
      { id: 'bad', method: 'session.create', params: { permissionMode: 'plan' } },
      { id: 'sticky', method: 'session.create', params: { ...policy, permissionIntent: turnPermission } },
      { id: 'create', method: 'session.create', params: policy },
      { id: 'resume', method: 'session.resume', params: { ...policy, providerSessionRef: 'provider', claudeCanonicalSessionRef: 'native' } },
    ]
    requests.push(
      { id: 'rebind', method: 'session.rebind', params: { sessionId: 'provider', destinationCwd: '/tmp' } } as any,
      { id: 'compact', method: 'session.compact', params: { sessionId: 'provider' } } as any,
    )
    const child = Bun.spawnSync(['bun', 'src/main.ts'], {
      cwd: import.meta.dir + '/..', env: { ...process.env, GG_CLAUDE_BRIDGE_MODE: 'fake' },
      stdin: new TextEncoder().encode(requests.map(request => JSON.stringify(request)).join('\n') + '\n'),
      stdout: 'pipe', stderr: 'pipe',
    })
    expect(child.exitCode).toBe(0)
    const responses = new TextDecoder().decode(child.stdout).trim().split('\n').map(line => JSON.parse(line))
    expect(responses.find(r => r.id === 'bad').error.code).toBe('BAD_REQUEST')
    expect(responses.find(r => r.id === 'sticky').error.code).toBe('BAD_REQUEST')
    expect(responses.find(r => r.id === 'rebind').result).toMatchObject({ bindingGeneration: 1, effectiveCwd: '/tmp', claudeCanonicalSessionRef: 'native' })
    expect(responses.find(r => r.id === 'compact').error.code).toBe('BAD_REQUEST')
  })
})
