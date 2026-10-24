import { describe, expect, test } from 'bun:test'
import type { ChildProcess } from 'node:child_process'
import type { SpawnOptions } from '@anthropic-ai/claude-agent-sdk'
import { ClaudeClient } from '../src/claude-client'
import { nativeProcessesFor } from '../src/claude-client/native-processes'
import type { ClaudeSessionOptions, SessionState } from '../src/claude-client/types'

const options: ClaudeSessionOptions = {
  cwd: '/tmp',
  model: 'claude-sonnet-5-5',
  settingSourcesIntent: { kind: 'isolated' },
  ggMcpServer: { command: '/bin/true', serverName: 'gg' },
}

function sessionState(client: ClaudeClient, sessionId: string): SessionState | undefined {
  return (client as unknown as { sessions: Map<string, SessionState> }).sessions.get(sessionId)
}

describe('retirement native close proof', () => {
  test('idle close returns exact attachment identity while preserving another session', async () => {
    const events: { event: string; sessionId: string }[] = []
    const client = new ClaudeClient(event => events.push(event), { mode: 'sdk' })
    client.resumeSession('bridge-one', options, 'provider-one', 'native-one')
    client.resumeSession('bridge-peer', options, 'provider-peer', 'native-peer')
    await expect(client.closeSessionForRetirement('bridge-one', 'retirement')).resolves.toEqual({
      ok: true, sessionId: 'bridge-one', providerSessionRef: 'provider-one',
    })
    expect(sessionState(client, 'bridge-one')).toBeUndefined()
    expect(sessionState(client, 'bridge-peer')?.providerSessionRef).toBe('provider-peer')
    expect(events.filter(event => event.event === 'session.closed')).toEqual([
      expect.objectContaining({ event: 'session.closed', sessionId: 'bridge-one' }),
    ])
    await expect(client.closeSessionForRetirement('bridge-one')).rejects.toThrow()
    await client.closeSessionForRetirement('bridge-peer')
  })

  test('an in-flight SDK query cannot be acknowledged through best-effort interrupt', async () => {
    let releaseQuery!: () => void
    let queryStarted!: () => void
    let interrupts = 0
    const gate = new Promise<void>(resolve => { releaseQuery = resolve })
    const started = new Promise<void>(resolve => { queryStarted = resolve })
    const client = new ClaudeClient(() => {}, {
      mode: 'sdk',
      sdkQuery: () => Object.assign((async function* () {
        queryStarted()
        yield { type: 'system', subtype: 'init', session_id: 'native' }
        await gate
        yield { type: 'result', subtype: 'success', session_id: 'native', result: 'complete' }
      })(), {
        interrupt: async () => { interrupts++; throw new Error('native interrupt failed') },
      }),
    })
    client.resumeSession('bridge', options, 'provider', 'native')
    const turn = await client.sendInput('bridge', [{ type: 'text', text: 'work' }], undefined,
      { kind: 'explicit', mode: 'plan' })
    await started
    await expect(client.closeSessionForRetirement('bridge')).rejects.toThrow('busy')
    expect(interrupts).toBe(0)
    expect(sessionState(client, 'bridge')).toBeDefined()
    releaseQuery()
    expect((await client.waitForTurn('bridge', turn.turnId, 2000)).status).toBe('completed')
    await expect(client.closeSessionForRetirement('bridge')).resolves.toMatchObject({ ok: true })
  })

  test('idle-looking sessions with native query, approvals or binding work fail closed', async () => {
    const client = new ClaudeClient(() => {}, { mode: 'sdk' })
    client.resumeSession('bridge', options, 'provider', 'native')
    const session = sessionState(client, 'bridge')!
    session.activeSdkQuery = Object.assign((async function* () {})(), {
      interrupt: async () => { throw new Error('unverified native work') },
    })
    await expect(client.closeSessionForRetirement('bridge')).rejects.toThrow('busy')
    expect(sessionState(client, 'bridge')).toBe(session)
    session.activeSdkQuery = null
    session.rebinding = true
    await expect(client.closeSessionForRetirement('bridge')).rejects.toThrow('busy')
    session.rebinding = false
    session.pendingApprovals.set('approval', {} as never)
    await expect(client.closeSessionForRetirement('bridge')).rejects.toThrow('busy')
    session.pendingApprovals.clear()
    await client.closeSessionForRetirement('bridge')
  })

  test('retirement reaps an SDK child after iterator cleanup while preserving the peer child', async () => {
    const children = new Map<string, ChildProcess>()
    const client = new ClaudeClient(() => {}, {
      mode: 'sdk',
      sdkQuery: ({ options: queryOptions }) => {
        const nativeId = queryOptions.resume as string
        const spawn = queryOptions.spawnClaudeCodeProcess as (options: SpawnOptions) => ChildProcess
        const child = spawn({
          command: process.execPath,
          args: ['-e', 'setInterval(() => {}, 1000)'],
          cwd: '/tmp',
          env: process.env,
          signal: new AbortController().signal,
        })
        children.set(nativeId, child)
        // Deliberately finish the SDK iterator without terminating its child.
        return (async function* () {
          yield { type: 'system', subtype: 'init', session_id: nativeId }
          yield { type: 'result', subtype: 'success', session_id: nativeId, result: 'complete' }
        })()
      },
    })
    client.resumeSession('bridge-one', options, 'provider-one', 'native-one')
    client.resumeSession('bridge-peer', options, 'provider-peer', 'native-peer')
    const states = [sessionState(client, 'bridge-one')!, sessionState(client, 'bridge-peer')!]
    try {
      for (const bridgeId of ['bridge-one', 'bridge-peer']) {
        const turn = await client.sendInput(bridgeId, [{ type: 'text', text: 'work' }], undefined,
          { kind: 'explicit', mode: 'plan' })
        expect((await client.waitForTurn(bridgeId, turn.turnId, 2000)).status).toBe('completed')
      }
      const child = children.get('native-one')!
      const peer = children.get('native-peer')!
      expect(states[0].activeSdkQuery).toBeNull()
      expect(child.signalCode).toBeNull()
      await expect(client.closeSessionForRetirement('bridge-one')).resolves.toEqual({
        ok: true, sessionId: 'bridge-one', providerSessionRef: 'provider-one',
      })
      expect(child.signalCode).toBe('SIGKILL')
      expect(() => process.kill(child.pid!, 0)).toThrow()
      expect(() => process.kill(peer.pid!, 0)).not.toThrow()
      expect(peer.signalCode).toBeNull()
      await client.closeSessionForRetirement('bridge-peer')
      expect(peer.signalCode).toBe('SIGKILL')
    } finally {
      await Promise.all(states.map(state => nativeProcessesFor(state).closeVerified()))
    }
  })

  test('a cwd verification query retains its native child owner for retirement', async () => {
    let child: ChildProcess | undefined
    const client = new ClaudeClient(() => {}, {
      mode: 'sdk',
      sdkQuery: ({ options: queryOptions }) => {
        const spawn = queryOptions.spawnClaudeCodeProcess as (options: SpawnOptions) => ChildProcess
        child = spawn({
          command: process.execPath, args: ['-e', 'setInterval(() => {}, 1000)'],
          cwd: '/tmp', env: process.env, signal: new AbortController().signal,
        })
        return (async function* () {
          yield { type: 'system', subtype: 'init', session_id: 'native', cwd: '/tmp' }
        })()
      },
    })
    client.resumeSession('bridge', options, 'provider', 'native')
    const session = sessionState(client, 'bridge')!
    try {
      await client.rebindSession('bridge', '/tmp')
      expect(child?.signalCode).toBeNull()
      await client.closeSessionForRetirement('bridge')
      expect(child?.signalCode).toBe('SIGKILL')
      expect(() => process.kill(child!.pid!, 0)).toThrow()
    } finally {
      await nativeProcessesFor(session).closeVerified()
    }
  })
})
