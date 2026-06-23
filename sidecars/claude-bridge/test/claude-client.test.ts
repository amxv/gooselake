import { describe, expect, test } from 'bun:test'

import { ClaudeClient } from '../src/claude-client'

type BridgeEvent = {
  event: string
  sessionId: string
  turnId?: string | null
  payload: Record<string, unknown>
}

type SessionOptions = Parameters<ClaudeClient['createSession']>[0]

function sdkSessionOptions(overrides: SessionOptions = {}): SessionOptions {
  return {
    ggMcpServer: {
      serverName: 'gg',
      command: '/tmp/gg-mcp-server',
    },
    ...overrides,
  }
}

function waitForEvent(
  events: BridgeEvent[],
  predicate: (event: BridgeEvent) => boolean,
  timeoutMs = 2000
): Promise<BridgeEvent> {
  const startedAt = Date.now()

  return new Promise((resolve, reject) => {
    const poll = () => {
      const found = events.find(predicate)
      if (found) {
        resolve(found)
        return
      }

      if (Date.now() - startedAt > timeoutMs) {
        reject(new Error('timed out waiting for event'))
        return
      }

      setTimeout(poll, 10)
    }

    poll()
  })
}

function resolveInternalSessionState(
  client: ClaudeClient,
  sessionId: string
): Record<string, unknown> | undefined {
  const clientState = client as unknown as {
    sessions?: Map<string, Record<string, unknown>>
  }
  return clientState.sessions?.get(sessionId)
}

describe('claude client bridge behavior', () => {
  test('create/send/wait emits streaming events', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event))

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'hello sidecar test' },
    ])

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 2000)

    expect(result.status).toBe('completed')
    expect(
      events.some(
        event => event.event === 'message.delta' && event.turnId === ack.turnId
      )
    ).toBe(true)
  })

  test('approval flow accepts and completes turn', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event))

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'needs approval [needs-approval]' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')

    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 2000)

    expect(result.status).toBe('completed')
  })

  test('interrupt marks slow turn interrupted', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event))

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'slow turn [slow]' },
    ])

    await client.interruptTurn(session.sessionId, ack.turnId)
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 2000)
    expect(result.status).toBe('interrupted')
  })

  test('wait timeout can be retried for the same turn', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event))

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'slow turn [slow]' },
    ])

    await expect(
      client.waitForTurn(session.sessionId, ack.turnId, 5)
    ).rejects.toMatchObject({
      code: 'TIMEOUT',
    })

    const retried = await client.waitForTurn(
      session.sessionId,
      ack.turnId,
      3000
    )
    expect(retried.status).toBe('completed')
  })

  test('sdk mode rejects session creation without external gg MCP config', () => {
    const client = new ClaudeClient(() => undefined, {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub(),
    })

    expect(() => client.createSession({})).toThrow(
      'Missing ggMcpServer config for SDK mode session'
    )
  })

  test('sdk mode rejects session resume without external gg MCP config', () => {
    const client = new ClaudeClient(() => undefined, {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub(),
    })

    expect(() => client.resumeSession('claude_sess_resume_1', {})).toThrow(
      'Missing ggMcpServer config for SDK mode session'
    )
  })

  test('supportedModels returns deterministic fake catalog outside sdk mode', async () => {
    const client = new ClaudeClient(() => undefined, {
      mode: 'fake',
    })

    const models = await client.supportedModels()
    expect(models).toEqual([
      {
        value: 'claude-sonnet-4-6',
        displayName: 'Claude Sonnet 4.6',
        supportsEffort: true,
        supportedEffortLevels: ['low', 'medium', 'high'],
        supportsVision: true,
        supportsToolCalling: true,
      },
      {
        value: 'claude-opus-4-8',
        displayName: 'Claude Opus 4.8',
        supportsEffort: true,
        supportedEffortLevels: ['low', 'medium', 'high'],
        supportsVision: true,
        supportsToolCalling: true,
      },
      {
        value: 'claude-haiku-4-5',
        displayName: 'Claude Haiku 4.5',
        supportsEffort: true,
        supportedEffortLevels: ['low', 'medium', 'high'],
        supportsVision: true,
        supportsToolCalling: true,
      },
    ])
  })

  test('supportedModels normalizes sdk payload shape', async () => {
    const client = new ClaudeClient(() => undefined, {
      mode: 'sdk',
      sdkSupportedModels: async () => [
        {
          value: ' claude-opus-4-8 ',
          displayName: ' Claude Opus 4.8 ',
          supportsEffort: true,
          supportedEffortLevels: ['HIGH', ' medium ', ''],
          supportsVision: true,
        },
        {
          value: '   ',
          displayName: '',
          supportsEffort: false,
          supportedEffortLevels: [],
        },
      ],
    })

    const models = await client.supportedModels()
    expect(models).toEqual([
      {
        value: 'claude-opus-4-8',
        displayName: 'Claude Opus 4.8',
        supportsEffort: true,
        supportedEffortLevels: ['high', 'medium'],
        supportsVision: true,
        supportsToolCalling: undefined,
      },
    ])
  })

  test('sdk mode emits approval + tool item lifecycle and completes on accept', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'run sdk flow with approval' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')

    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('completed')

    expect(
      events.some(
        event =>
          event.event === 'item.started' &&
          event.turnId === ack.turnId &&
          (event.payload.item as Record<string, unknown>)?.id === 'tool_1' &&
          (event.payload.item as Record<string, unknown>)?.type ===
            'commandExecution'
      )
    ).toBe(true)
    expect(
      events.some(
        event =>
          event.event === 'item.completed' &&
          event.turnId === ack.turnId &&
          (event.payload.item as Record<string, unknown>)?.id === 'tool_1' &&
          (event.payload.item as Record<string, unknown>)?.type ===
            'commandExecution'
      )
    ).toBe(true)
    expect(
      events.some(
        event => event.event === 'message.delta' && event.turnId === ack.turnId
      )
    ).toBe(true)
  })

  test('sdk mode streams reasoning deltas from thinking stream events', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryThinkingStreamStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'show your thinking stream' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')

    const reasoningEvents = events.filter(
      event => event.event === 'reasoning.delta' && event.turnId === ack.turnId
    )
    expect(reasoningEvents).toHaveLength(2)
    expect(reasoningEvents.map(event => event.payload.summaryIndex)).toEqual([
      0, 0,
    ])
    expect(reasoningEvents.map(event => event.payload.delta)).toEqual([
      'Inspecting prompt. ',
      'Forming answer.',
    ])
    expect(reasoningEvents.map(event => event.payload.itemId)).toEqual([
      `item_msg_${ack.turnId}`,
      `item_msg_${ack.turnId}`,
    ])
  })

  test('sdk mode falls back to assistant thinking blocks when stream deltas are unavailable', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryThinkingFallbackStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'fallback thinking path' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')

    const reasoningEvents = events.filter(
      event => event.event === 'reasoning.delta' && event.turnId === ack.turnId
    )
    expect(reasoningEvents).toHaveLength(1)
    expect(reasoningEvents[0]?.payload.summaryIndex).toBe(1)
    expect(reasoningEvents[0]?.payload.delta).toBe('Fallback reasoning block')
    expect(reasoningEvents[0]?.payload.itemId).toBe(`item_msg_${ack.turnId}`)
  })

  test('sdk mode falls back to tool_use_summary when no thinking stream or assistant thinking blocks are present', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryToolUseSummaryFallbackStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'show reasoning summary fallback' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')

    const reasoningEvents = events.filter(
      event => event.event === 'reasoning.delta' && event.turnId === ack.turnId
    )
    expect(reasoningEvents).toHaveLength(2)
    expect(reasoningEvents.map(event => event.payload.summaryIndex)).toEqual([
      0, 1,
    ])
    expect(reasoningEvents.map(event => event.payload.delta)).toEqual([
      'Checked repository context.',
      'Prepared concise final response.',
    ])
    expect(reasoningEvents.map(event => event.payload.itemId)).toEqual([
      `item_msg_${ack.turnId}`,
      `item_msg_${ack.turnId}`,
    ])
  })

  test('sdk mode emits CAPACITY_EXHAUSTED for terminal limit text and preserves emitted output', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryTerminalResultStub({
        subtype: 'error_during_execution',
        result:
          "You've hit your 5-hour limit. Please try again later in this window.",
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'trigger terminal limit text' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('failed')

    const capacityError = events.find(
      event =>
        event.event === 'error' &&
        event.turnId === ack.turnId &&
        event.payload.code === 'CAPACITY_EXHAUSTED'
    )
    expect(capacityError).toBeDefined()
    expect(capacityError?.payload.details).toEqual(
      expect.objectContaining({
        providerAuthCapacityClassification: expect.objectContaining({
          provider: 'claude',
          source: 'claude_terminal_assistant_text',
          reason: 'session_window_limit',
          matchedRule: '5_hour_limit_window',
          resetWindowHint: '5h',
        }),
      })
    )

    const messageDelta = events.find(
      event =>
        event.event === 'message.delta' &&
        event.turnId === ack.turnId &&
        event.payload.delta ===
          "You've hit your 5-hour limit. Please try again later in this window."
    )
    expect(messageDelta).toBeDefined()

    const completedAgentItem = events.find(
      event =>
        event.event === 'item.completed' &&
        event.turnId === ack.turnId &&
        (event.payload.item as Record<string, unknown>)?.type === 'agentMessage'
    )
    expect(
      (completedAgentItem?.payload.item as Record<string, unknown>)?.text
    ).toBe(
      "You've hit your 5-hour limit. Please try again later in this window."
    )
  })

  test('sdk mode emits CAPACITY_EXHAUSTED for terminal limit text even when result subtype is success', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryTerminalResultStub({
        subtype: 'success',
        result:
          'I reached the usage limit for this session. The limit resets in 3 hours.',
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'trigger success-subtype terminal limit text' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(
      events.some(
        event =>
          event.event === 'error' &&
          event.turnId === ack.turnId &&
          event.payload.code === 'CAPACITY_EXHAUSTED'
      )
    ).toBe(true)
  })

  test('sdk mode does not emit CAPACITY_EXHAUSTED for normal terminal text', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryTerminalResultStub({
        subtype: 'success',
        result: 'Completed normally. Here is your summary and next step.',
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'normal terminal result' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(
      events.some(
        event =>
          event.event === 'error' &&
          event.turnId === ack.turnId &&
          event.payload.code === 'CAPACITY_EXHAUSTED'
      )
    ).toBe(false)
  })

  test('sdk mode does not emit CAPACITY_EXHAUSTED for generic explanatory usage-limit success text', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryTerminalResultStub({
        subtype: 'success',
        result: 'The usage limit resets in 3 hours for this workspace.',
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'generic explanatory limit text' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(
      events.some(
        event =>
          event.event === 'error' &&
          event.turnId === ack.turnId &&
          event.payload.code === 'CAPACITY_EXHAUSTED'
      )
    ).toBe(false)
  })

  test('sdk mode does not emit CAPACITY_EXHAUSTED for conditional usage-limit success text', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryTerminalResultStub({
        subtype: 'success',
        result: "If you've hit your usage limit, it resets in 3 hours.",
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'conditional explanatory limit text' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(
      events.some(
        event =>
          event.event === 'error' &&
          event.turnId === ack.turnId &&
          event.payload.code === 'CAPACITY_EXHAUSTED'
      )
    ).toBe(false)
  })

  test('sdk mode emits session.updated with canonical sdk session id without rewriting provider identity', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'collect sdk session id' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')

    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )

    await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    const sessionUpdatedEvent = events.find(
      event => event.event === 'session.updated'
    )
    expect(sessionUpdatedEvent).toBeDefined()
    expect(sessionUpdatedEvent?.payload.providerSessionRef).toBe(
      session.providerSessionRef
    )
    expect(sessionUpdatedEvent?.payload.claudeCanonicalSessionRef).toBe(
      'sdk_session_1'
    )
  })

  test('sdk mode keeps first-turn canonicalization metadata-only and resumes second send without stale turn state', async () => {
    const events: BridgeEvent[] = []
    const queryCalls: Array<Record<string, unknown> | undefined> = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub({
        onOptions: options => {
          queryCalls.push(options)
        },
      }),
    })

    const session = client.createSession(sdkSessionOptions())

    const firstAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'first turn canonicalization sequence' },
    ])
    const firstApprovalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === firstAck.turnId
    )
    const firstApprovalId = firstApprovalEvent.payload.approvalId
    expect(typeof firstApprovalId).toBe('string')
    await client.respondApproval(
      session.sessionId,
      firstAck.turnId,
      firstApprovalId as string,
      'accept'
    )
    const firstResult = await client.waitForTurn(
      session.sessionId,
      firstAck.turnId,
      4000
    )
    expect(firstResult.status).toBe('completed')

    const metadataUpdatesAfterFirstTurn = events.filter(
      event => event.event === 'session.updated'
    )
    expect(metadataUpdatesAfterFirstTurn).toHaveLength(1)
    expect(metadataUpdatesAfterFirstTurn[0]?.payload.providerSessionRef).toBe(
      session.providerSessionRef
    )
    expect(
      metadataUpdatesAfterFirstTurn[0]?.payload.claudeCanonicalSessionRef
    ).toBe('sdk_session_1')

    const secondAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'second turn should keep history' },
    ])
    const secondApprovalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' &&
        event.turnId === secondAck.turnId
    )
    const secondApprovalId = secondApprovalEvent.payload.approvalId
    expect(typeof secondApprovalId).toBe('string')
    await client.respondApproval(
      session.sessionId,
      secondAck.turnId,
      secondApprovalId as string,
      'accept'
    )
    const secondResult = await client.waitForTurn(
      session.sessionId,
      secondAck.turnId,
      4000
    )
    expect(secondResult.status).toBe('completed')

    expect(
      events.some(
        event =>
          event.event === 'message.delta' && event.turnId === firstAck.turnId
      )
    ).toBe(true)
    expect(
      events.some(
        event =>
          event.event === 'message.delta' && event.turnId === secondAck.turnId
      )
    ).toBe(true)
    expect(queryCalls).toHaveLength(2)
    expect(queryCalls[0]?.resume).toBeUndefined()
    expect(queryCalls[1]?.resume).toBe('sdk_session_1')
    expect(
      events.filter(event => event.event === 'session.updated')
    ).toHaveLength(1)

    const runtimeState = resolveInternalSessionState(
      client,
      session.sessionId
    ) as
      | {
          turnOrder?: string[]
          turnResults?: Map<string, unknown>
          turnWaiters?: Map<string, unknown>
        }
      | undefined
    expect(runtimeState).toBeDefined()
    expect(runtimeState?.turnOrder).toEqual([firstAck.turnId, secondAck.turnId])
    expect(runtimeState?.turnResults?.has(firstAck.turnId)).toBe(true)
    expect(runtimeState?.turnResults?.has(secondAck.turnId)).toBe(true)
    expect(runtimeState?.turnWaiters?.size ?? 0).toBe(0)
  })

  test('sdk mode resume prefers canonical session ref for sdk query resume options', async () => {
    const queryCalls: Array<Record<string, unknown> | undefined> = []
    const client = new ClaudeClient(() => undefined, {
      mode: 'sdk',
      sdkQuery: ({ options }: { options?: Record<string, unknown> }) => {
        queryCalls.push(options)
        const iterator = (async function* () {
          yield {
            type: 'result',
            subtype: 'success',
            session_id: 'claude_sdk_resume_canonical',
            result: 'resumed with canonical ref',
            usage: {
              input_tokens: 3,
              output_tokens: 2,
            },
          }
        })()

        return Object.assign(iterator, {
          interrupt: async () => {},
        })
      },
    })

    const resumed = client.resumeSession(
      'claude_runtime_resume_ref',
      sdkSessionOptions(),
      'claude_runtime_resume_ref',
      'claude_sdk_resume_canonical'
    )
    expect(resumed.providerSessionRef).toBe('claude_runtime_resume_ref')
    expect(resumed.claudeCanonicalSessionRef).toBe(
      'claude_sdk_resume_canonical'
    )

    const ack = await client.sendInput(resumed.sessionId, [
      { type: 'text', text: 'resume with canonical session id' },
    ])
    const result = await client.waitForTurn(resumed.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('completed')

    expect(queryCalls).toHaveLength(1)
    expect(queryCalls[0]?.resume).toBe('claude_sdk_resume_canonical')
  })

  test('sdk mode emits Claude compaction lifecycle events and resumes /compact turns', async () => {
    const events: BridgeEvent[] = []
    const queryCalls: Array<{
      prompt: unknown
      options: Record<string, unknown> | undefined
    }> = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryCompactionLifecycleStub(call => {
        queryCalls.push(call)
      }),
    })

    const session = client.createSession(sdkSessionOptions())

    const firstAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'establish sdk session' },
    ])
    const firstResult = await client.waitForTurn(
      session.sessionId,
      firstAck.turnId,
      4000
    )
    expect(firstResult.status).toBe('completed')

    const compactAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: '/compact' },
    ])
    const compactResult = await client.waitForTurn(
      session.sessionId,
      compactAck.turnId,
      4000
    )
    expect(compactResult.status).toBe('completed')

    expect(queryCalls).toHaveLength(2)
    expect(queryCalls[1]?.prompt).toBe('/compact')
    expect(queryCalls[1]?.options?.resume).toBe('sdk_session_compaction')

    const compactionEvents = events.filter(
      event =>
        event.event === 'context.compaction' &&
        event.turnId === compactAck.turnId
    )
    expect(compactionEvents).toHaveLength(2)
    expect(compactionEvents[0]?.payload).toEqual({
      phase: 'started',
    })
    expect(compactionEvents[1]?.payload).toEqual({
      phase: 'completed',
      trigger: 'manual',
      preTokens: 123_456,
    })
  })

  test('sdk mode fails turn on declined approval', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'decline sdk approval flow' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')

    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'decline'
    )

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('failed')
  })

  test('sdk mode sends image attachments as structured user message blocks', async () => {
    const events: BridgeEvent[] = []
    let capturedPrompt: unknown
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub({
        onPrompt: prompt => {
          capturedPrompt = prompt
        },
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'describe this screenshot' },
      { type: 'image', mediaType: 'image/png', data: 'AQID' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')
    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(capturedPrompt).toBeDefined()
    expect(typeof capturedPrompt).not.toBe('string')

    const streamPrompt = capturedPrompt as AsyncIterable<unknown>
    const iterator = streamPrompt[Symbol.asyncIterator]()
    const firstMessage = await iterator.next()
    expect(firstMessage.done).toBe(false)

    const userMessage = firstMessage.value as Record<string, unknown>
    expect(userMessage.type).toBe('user')
    expect(userMessage.parent_tool_use_id).toBeNull()

    const payload = userMessage.message as Record<string, unknown>
    const content = payload.content as Array<Record<string, unknown>>
    expect(Array.isArray(content)).toBe(true)
    expect(content[0]).toEqual({
      type: 'text',
      text: 'describe this screenshot',
    })
    expect(content[1]).toEqual({
      type: 'image',
      source: {
        type: 'base64',
        media_type: 'image/png',
        data: 'AQID',
      },
    })
  })

  test('sdk mode propagates in-progress task output status before terminal output', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryTaskOutputStatusStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'wait for background task output' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('completed')

    const taskOutputCompletions = events.filter(
      event =>
        event.event === 'item.completed' &&
        event.turnId === ack.turnId &&
        (event.payload.item as Record<string, unknown>)?.id ===
          'tool_task_output_1'
    )
    expect(taskOutputCompletions).toHaveLength(2)
    expect(
      (taskOutputCompletions[0]?.payload.item as Record<string, unknown>)
        ?.status
    ).toBe('in_progress')
    expect(
      (taskOutputCompletions[1]?.payload.item as Record<string, unknown>)
        ?.status
    ).toBe('completed')
  })

  test('sdk mode passes user config scope to sdk query options', async () => {
    const events: BridgeEvent[] = []
    let capturedOptions: Record<string, unknown> | undefined
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub({
        onOptions: options => {
          capturedOptions = options
        },
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'check options' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')
    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('completed')
    expect(capturedOptions?.configScope).toBe('user')
  })

  test('sdk mode wires external gg MCP server and rewrites gg tool prefixes', async () => {
    const events: BridgeEvent[] = []
    let capturedOptions: Record<string, unknown> | undefined
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub({
        onOptions: options => {
          capturedOptions = options
        },
      }),
    })

    const session = client.createSession({
      ggMcpServer: {
        serverName: 'gg_runtime',
        callerAgentId: 'sess_runtime_cfg',
        command: '/tmp/gg-mcp-server',
      },
      allowedTools: [
        'mcp__gg_team__gg_team_message',
        'mcp__gg_process__gg_process_run',
        'mcp__gg__gg_team_status',
        'mcp__gg_old__gg_process_status',
        'Read',
      ],
      disallowedTools: [
        'mcp__gg__gg_process_kill',
        'mcp__gg_old__gg_team_manage',
        'Bash',
      ],
    })
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'check external mcp wiring and tool rewrites' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')
    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('completed')

    const mcpServers = capturedOptions?.mcpServers as
      | Record<string, unknown>
      | undefined
    expect(mcpServers).toBeDefined()
    expect(Object.keys(mcpServers ?? {})).toEqual(['gg_runtime'])
    expect((mcpServers?.gg_runtime as Record<string, unknown>)?.command).toBe(
      '/tmp/gg-mcp-server'
    )
    expect((mcpServers?.gg_runtime as Record<string, unknown>)?.env).toEqual(
      expect.objectContaining({
        GG_MCP_CALLER_AGENT_ID: 'sess_runtime_cfg',
      })
    )

    expect(capturedOptions?.allowedTools).toEqual([
      'mcp__gg_runtime__gg_team_message',
      'mcp__gg_runtime__gg_process_run',
      'mcp__gg_runtime__gg_team_status',
      'mcp__gg_runtime__gg_process_status',
      'Read',
    ])
    expect(capturedOptions?.disallowedTools).toEqual([
      'mcp__gg_runtime__gg_process_kill',
      'mcp__gg_runtime__gg_team_manage',
      'Bash',
    ])
  })

  test('sdk mode injects caller id for namespaced gg MCP tools', async () => {
    const events: BridgeEvent[] = []
    let capturedPermission: Record<string, unknown> | undefined
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQuerySinglePermissionCaptureStub({
        toolName: 'mcp__gg__goldengoose.gg_team_status',
        onPermission: permission => {
          capturedPermission = permission
        },
      }),
    })

    const session = client.createSession({
      ggMcpServer: {
        serverName: 'gg',
        callerAgentId: 'sess_dynamic_caller',
        command: '/tmp/gg-mcp-server',
      },
    })
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'check namespaced gg caller metadata injection' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')
    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('completed')
    expect(capturedPermission).toBeDefined()
    expect(capturedPermission?.behavior).toBe('allow')
    expect(capturedPermission?.updatedInput).toEqual(
      expect.objectContaining({
        team_id: 'team_1',
        __gg_caller_agent_id: 'sess_dynamic_caller',
      })
    )
  })

  test('sdk mode fails turn when external gg MCP config command is empty', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub(),
    })

    const session = client.createSession({
      ggMcpServer: {
        serverName: 'gg',
        command: '   ',
      },
    })
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'this should fail before sdk query starts' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('failed')
    const errorEvent = events.find(
      event => event.event === 'error' && event.turnId === ack.turnId
    )
    expect(errorEvent).toBeDefined()
    expect(errorEvent?.payload.message).toContain(
      'Invalid ggMcpServer config: command is empty'
    )
  })

  test('sdk mode prefers assistant usage and result model context window', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryModelWindowUsageStub(),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'collect usage data' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(result.usage).toEqual({
      inputTokens: 12_000,
      outputTokens: 400,
      cacheCreationInputTokens: 8_000,
      cacheReadInputTokens: 16_000,
      contextWindowSize: 1_000_000,
    })

    const completedEvent = events.find(
      event =>
        event.event === 'turn.completed' &&
        event.turnId === ack.turnId &&
        event.sessionId === session.sessionId
    )
    expect(completedEvent).toBeDefined()
    expect(completedEvent?.payload.usage).toEqual(result.usage)
  })

  test('sdk mode reuses last known usage when a turn omits usage payloads', async () => {
    const client = new ClaudeClient(() => undefined, {
      mode: 'sdk',
      sdkQuery: createSdkQueryMissingUsageSecondTurnStub(),
    })

    const session = client.createSession(sdkSessionOptions())

    const firstAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'first run' },
    ])
    const firstResult = await client.waitForTurn(
      session.sessionId,
      firstAck.turnId,
      4000
    )
    expect(firstResult.status).toBe('completed')
    expect(firstResult.usage).toEqual({
      inputTokens: 9_000,
      outputTokens: 200,
      cacheCreationInputTokens: 2_000,
      cacheReadInputTokens: 5_000,
      contextWindowSize: 200_000,
    })

    const secondAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'second run' },
    ])
    const secondResult = await client.waitForTurn(
      session.sessionId,
      secondAck.turnId,
      4000
    )
    expect(secondResult.status).toBe('completed')
    expect(secondResult.usage).toEqual(firstResult.usage)
  })

  test('sdk mode reconnects gg MCP server after Stream closed tool_result', async () => {
    const events: BridgeEvent[] = []
    const reconnectCalls: string[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryGgTeamStreamClosedStub({
        onReconnect: serverName => reconnectCalls.push(serverName),
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'trigger gg_team stream closed recovery' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(reconnectCalls).toEqual(['gg'])
    expect(
      events.some(
        event =>
          event.event === 'item.completed' &&
          event.turnId === ack.turnId &&
          (event.payload.item as Record<string, unknown>)?.id ===
            'gg_team_tool_1' &&
          (event.payload.item as Record<string, unknown>)?.output ===
            'Stream closed'
      )
    ).toBe(true)
  })

  test('sdk mode reconnects unified gg MCP server after Stream closed tool_result', async () => {
    const events: BridgeEvent[] = []
    const reconnectCalls: string[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryGgTeamStreamClosedStub({
        toolName: 'mcp__gg__gg_team_status',
        onReconnect: serverName => reconnectCalls.push(serverName),
      }),
    })

    const session = client.createSession({
      ggMcpServer: {
        serverName: 'gg',
        command: '/tmp/gg-mcp-server',
      },
    })
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'trigger unified gg stream closed recovery' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(reconnectCalls).toEqual(['gg'])
    expect(
      events.some(
        event =>
          event.event === 'item.completed' &&
          event.turnId === ack.turnId &&
          (event.payload.item as Record<string, unknown>)?.id ===
            'gg_team_tool_1' &&
          (event.payload.item as Record<string, unknown>)?.output ===
            'Stream closed'
      )
    ).toBe(true)
  })

  test('sdk mode reconnects custom-named gg MCP server after Stream closed tool_result', async () => {
    const events: BridgeEvent[] = []
    const reconnectCalls: string[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryGgTeamStreamClosedStub({
        toolName: 'mcp__gg_runtime__gg_team_status',
        onReconnect: serverName => reconnectCalls.push(serverName),
      }),
    })

    const session = client.createSession({
      ggMcpServer: {
        serverName: 'gg_runtime',
        command: '/tmp/gg-mcp-server',
      },
    })
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'trigger custom gg stream closed recovery' },
    ])
    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)

    expect(result.status).toBe('completed')
    expect(reconnectCalls).toEqual(['gg_runtime'])
    expect(
      events.some(
        event =>
          event.event === 'item.completed' &&
          event.turnId === ack.turnId &&
          (event.payload.item as Record<string, unknown>)?.id ===
            'gg_team_tool_1' &&
          (event.payload.item as Record<string, unknown>)?.output ===
            'Stream closed'
      )
    ).toBe(true)
  })

  test('sdk mode deflects concurrent gg_team tool calls to one in-flight approval', async () => {
    const events: BridgeEvent[] = []
    let secondPermission: Record<string, unknown> | undefined
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryConcurrentGgTeamToolStub({
        onPermissions: permissions => {
          secondPermission = permissions.second
        },
      }),
    })

    const session = client.createSession(sdkSessionOptions())
    const ack = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'serialize gg_team tool calls' },
    ])

    const approvalEvent = await waitForEvent(
      events,
      event =>
        event.event === 'approval.requested' && event.turnId === ack.turnId
    )
    const approvalId = approvalEvent.payload.approvalId
    expect(typeof approvalId).toBe('string')

    await client.respondApproval(
      session.sessionId,
      ack.turnId,
      approvalId as string,
      'accept'
    )

    const result = await client.waitForTurn(session.sessionId, ack.turnId, 4000)
    expect(result.status).toBe('completed')
    expect(
      events.filter(
        event =>
          event.event === 'approval.requested' && event.turnId === ack.turnId
      )
    ).toHaveLength(1)
    expect(secondPermission).toMatchObject({
      behavior: 'deny',
      message: expect.stringContaining('already in flight'),
    })
    expect(secondPermission?.interrupt).toBeUndefined()
  })

  test('sdk mode hard fork uses captured streamed user UUIDs when available', async () => {
    const events: BridgeEvent[] = []
    const queryCalls: Array<Record<string, unknown> | undefined> = []
    let queryInvocation = 0
    let historyLookupCount = 0
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: ({
        prompt,
        options,
      }: {
        prompt: unknown
        options?: Record<string, unknown>
      }) => {
        queryInvocation += 1
        queryCalls.push(options)
        const invocation = queryInvocation

        const iterator = (async function* () {
          if (invocation === 1) {
            yield {
              type: 'user',
              session_id: 'sdk_session_root',
              uuid: 'captured_user_turn_1',
              message: {
                content: 'first streamed turn',
              },
              parent_tool_use_id: null,
            }
            yield {
              type: 'assistant',
              session_id: 'sdk_session_root',
              uuid: 'captured_assistant_turn_1',
              message: {
                content: [{ type: 'text', text: 'first response' }],
              },
            }
            yield {
              type: 'result',
              subtype: 'success',
              session_id: 'sdk_session_root',
              result: 'first response',
              usage: {
                input_tokens: 4,
                output_tokens: 2,
              },
            }
            return
          }

          if (invocation === 2) {
            yield {
              type: 'user',
              session_id: 'sdk_session_root',
              uuid: 'captured_user_turn_2',
              message: {
                content: 'second streamed turn',
              },
              parent_tool_use_id: null,
            }
            yield {
              type: 'assistant',
              session_id: 'sdk_session_root',
              uuid: 'captured_assistant_turn_2',
              message: {
                content: [{ type: 'text', text: 'second response' }],
              },
            }
            yield {
              type: 'result',
              subtype: 'success',
              session_id: 'sdk_session_root',
              result: 'second response',
              usage: {
                input_tokens: 5,
                output_tokens: 2,
              },
            }
            return
          }

          expect(typeof prompt).not.toBe('string')
          yield {
            type: 'result',
            subtype: 'success',
            session_id: 'sdk_session_child_from_capture',
            result: 'forked',
            usage: {
              input_tokens: 0,
              output_tokens: 0,
            },
          }
        })()

        return Object.assign(iterator, {
          interrupt: async () => {},
        })
      },
      sdkGetSessionMessages: async () => {
        historyLookupCount += 1
        throw new Error(
          'history lookup should not be needed when capture succeeds'
        )
      },
    })

    const session = client.createSession(sdkSessionOptions())

    const firstAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'first streamed turn' },
    ])
    await client.waitForTurn(session.sessionId, firstAck.turnId, 4000)

    const secondAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'second streamed turn' },
    ])
    await client.waitForTurn(session.sessionId, secondAck.turnId, 4000)

    const hardForkResult = await client.hardForkSession(
      session.sessionId,
      secondAck.turnId
    )

    expect(historyLookupCount).toBe(0)
    expect(hardForkResult.childProviderSessionRef).toBe(
      'sdk_session_child_from_capture'
    )
    expect(queryCalls).toHaveLength(3)
    expect(queryCalls[2]?.resume).toBe('sdk_session_root')
    expect(queryCalls[2]?.forkSession).toBe(true)
    expect(queryCalls[2]?.resumeSessionAt).toBe('captured_assistant_turn_1')
    expect(typeof queryCalls[2]?.sessionId).toBe('string')

    const sessionUpdatedEvents = events.filter(
      event => event.event === 'session.updated'
    )
    expect(sessionUpdatedEvents.at(-1)?.payload.providerSessionRef).toBe(
      'sdk_session_child_from_capture'
    )
    expect(sessionUpdatedEvents.at(-1)?.payload.claudeCanonicalSessionRef).toBe(
      'sdk_session_child_from_capture'
    )
  })

  test('sdk mode hard fork falls back to getSessionMessages when streamed UUIDs are unavailable', async () => {
    const events: BridgeEvent[] = []
    const queryCalls: Array<Record<string, unknown> | undefined> = []
    let queryInvocation = 0
    const historyCalls: Array<{
      sessionId: string
      options: Record<string, unknown> | undefined
    }> = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: ({
        options,
      }: {
        prompt: unknown
        options?: Record<string, unknown>
      }) => {
        queryInvocation += 1
        queryCalls.push(options)
        const invocation = queryInvocation

        const iterator = (async function* () {
          if (invocation <= 2) {
            yield {
              type: 'assistant',
              session_id: 'sdk_session_history',
              message: {
                content: [{ type: 'text', text: `response ${invocation}` }],
              },
            }
            yield {
              type: 'result',
              subtype: 'success',
              session_id: 'sdk_session_history',
              result: `response ${invocation}`,
              usage: {
                input_tokens: 4,
                output_tokens: 2,
              },
            }
            return
          }

          yield {
            type: 'result',
            subtype: 'success',
            session_id: 'sdk_session_history_child',
            result: 'forked from history',
            usage: {
              input_tokens: 0,
              output_tokens: 0,
            },
          }
        })()

        return Object.assign(iterator, {
          interrupt: async () => {},
        })
      },
      sdkGetSessionMessages: async (sessionId, options) => {
        historyCalls.push({ sessionId, options })
        return [
          {
            type: 'user',
            uuid: 'history_user_turn_1',
            session_id: sessionId,
            message: {
              content: 'history turn 1',
            },
            parent_tool_use_id: null,
          },
          {
            type: 'assistant',
            uuid: 'history_assistant_turn_1',
            session_id: sessionId,
            message: {
              content: [{ type: 'text', text: 'history response 1' }],
            },
            parent_tool_use_id: null,
          },
          {
            type: 'user',
            uuid: 'history_user_turn_2',
            session_id: sessionId,
            message: {
              content: 'history turn 2',
            },
            parent_tool_use_id: null,
          },
          {
            type: 'assistant',
            uuid: 'history_assistant_turn_2',
            session_id: sessionId,
            message: {
              content: [{ type: 'text', text: 'history response 2' }],
            },
            parent_tool_use_id: null,
          },
        ]
      },
    })

    const session = client.createSession(
      sdkSessionOptions({
        cwd: '/tmp/hard-fork-history',
      })
    )

    const firstAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'history turn 1' },
    ])
    await client.waitForTurn(session.sessionId, firstAck.turnId, 4000)

    const secondAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'history turn 2' },
    ])
    await client.waitForTurn(session.sessionId, secondAck.turnId, 4000)

    const hardForkResult = await client.hardForkSession(
      session.sessionId,
      'history_user_turn_2'
    )

    expect(hardForkResult.childProviderSessionRef).toBe(
      'sdk_session_history_child'
    )
    expect(historyCalls).toEqual([
      {
        sessionId: 'sdk_session_history',
        options: {
          dir: '/tmp/hard-fork-history',
        },
      },
    ])
    expect(queryCalls[2]?.resume).toBe('sdk_session_history')
    expect(queryCalls[2]?.forkSession).toBe(true)
    expect(queryCalls[2]?.resumeSessionAt).toBe('history_assistant_turn_1')

    const internalSessionState = resolveInternalSessionState(
      client,
      session.sessionId
    ) as
      | {
          turnOrder?: string[]
          turnResults?: Map<string, unknown>
          turnWaiters?: Map<string, unknown>
          turnToolItems?: Map<string, unknown>
        }
      | undefined
    expect(internalSessionState).toBeDefined()
    expect(internalSessionState?.turnOrder).toEqual([firstAck.turnId])
    expect(internalSessionState?.turnResults?.has(secondAck.turnId)).toBe(false)
    expect(internalSessionState?.turnWaiters?.has(secondAck.turnId)).toBe(false)
    expect(internalSessionState?.turnToolItems?.has(secondAck.turnId)).toBe(
      false
    )

    const sessionUpdatedEvents = events.filter(
      event => event.event === 'session.updated'
    )
    expect(sessionUpdatedEvents.at(-1)?.payload.providerSessionRef).toBe(
      'sdk_session_history_child'
    )
    expect(sessionUpdatedEvents.at(-1)?.payload.claudeCanonicalSessionRef).toBe(
      'sdk_session_history_child'
    )
  })

  test('sdk mode hard fork normalizes to the earliest user boundary within the edited turn', async () => {
    const queryCalls: Array<Record<string, unknown> | undefined> = []
    let queryInvocation = 0
    let historyLookupCount = 0
    const client = new ClaudeClient(() => undefined, {
      mode: 'sdk',
      sdkQuery: ({
        options,
      }: {
        prompt: unknown
        options?: Record<string, unknown>
      }) => {
        queryInvocation += 1
        queryCalls.push(options)
        const invocation = queryInvocation

        const iterator = (async function* () {
          if (invocation === 1) {
            yield {
              type: 'user',
              session_id: 'sdk_session_inclusive',
              uuid: 'turn_1_user_boundary',
              message: {
                content: 'turn one',
              },
              parent_tool_use_id: null,
            }
            yield {
              type: 'assistant',
              session_id: 'sdk_session_inclusive',
              uuid: 'turn_1_assistant_boundary',
              message: {
                content: [{ type: 'text', text: 'turn one response' }],
              },
            }
            yield {
              type: 'result',
              subtype: 'success',
              session_id: 'sdk_session_inclusive',
              result: 'turn one response',
              usage: {
                input_tokens: 4,
                output_tokens: 2,
              },
            }
            return
          }

          if (invocation === 2) {
            yield {
              type: 'user',
              session_id: 'sdk_session_inclusive',
              uuid: 'turn_2_user_boundary_earliest',
              message: {
                content: 'turn two first chunk',
              },
              parent_tool_use_id: null,
            }
            yield {
              type: 'user',
              session_id: 'sdk_session_inclusive',
              uuid: 'turn_2_user_boundary_later',
              message: {
                content: 'turn two appended chunk',
              },
              parent_tool_use_id: null,
            }
            yield {
              type: 'assistant',
              session_id: 'sdk_session_inclusive',
              uuid: 'turn_2_assistant_boundary',
              message: {
                content: [{ type: 'text', text: 'turn two response' }],
              },
            }
            yield {
              type: 'result',
              subtype: 'success',
              session_id: 'sdk_session_inclusive',
              result: 'turn two response',
              usage: {
                input_tokens: 6,
                output_tokens: 2,
              },
            }
            return
          }

          yield {
            type: 'result',
            subtype: 'success',
            session_id: 'sdk_session_inclusive_child',
            result: 'forked inclusive',
            usage: {
              input_tokens: 0,
              output_tokens: 0,
            },
          }
        })()

        return Object.assign(iterator, {
          interrupt: async () => {},
        })
      },
      sdkGetSessionMessages: async () => {
        historyLookupCount += 1
        throw new Error('captured turn boundaries should be sufficient')
      },
    })

    const session = client.createSession(sdkSessionOptions())

    const firstAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'turn one' },
    ])
    await client.waitForTurn(session.sessionId, firstAck.turnId, 4000)

    const secondAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'turn two' },
    ])
    await client.waitForTurn(session.sessionId, secondAck.turnId, 4000)

    await client.hardForkSession(
      session.sessionId,
      'turn_2_user_boundary_later'
    )

    expect(historyLookupCount).toBe(0)
    expect(queryCalls[2]?.resume).toBe('sdk_session_inclusive')
    expect(queryCalls[2]?.resumeSessionAt).toBe('turn_1_assistant_boundary')
  })

  test('sdk mode hard fork creates a fresh child session when editing the first user turn', async () => {
    const queryCalls: Array<Record<string, unknown> | undefined> = []
    let queryInvocation = 0
    const client = new ClaudeClient(() => undefined, {
      mode: 'sdk',
      sdkQuery: ({
        options,
      }: {
        prompt: unknown
        options?: Record<string, unknown>
      }) => {
        queryInvocation += 1
        queryCalls.push(options)
        const invocation = queryInvocation

        const iterator = (async function* () {
          if (invocation === 1) {
            yield {
              type: 'user',
              session_id: 'sdk_session_first_turn',
              uuid: 'first_turn_user_boundary',
              message: {
                content: 'first turn only',
              },
              parent_tool_use_id: null,
            }
            yield {
              type: 'assistant',
              session_id: 'sdk_session_first_turn',
              uuid: 'first_turn_assistant_boundary',
              message: {
                content: [{ type: 'text', text: 'first turn response' }],
              },
            }
            yield {
              type: 'result',
              subtype: 'success',
              session_id: 'sdk_session_first_turn',
              result: 'first turn response',
              usage: {
                input_tokens: 4,
                output_tokens: 2,
              },
            }
            return
          }

          yield {
            type: 'result',
            subtype: 'success',
            session_id: 'sdk_session_fresh_child',
            result: 'fresh fork',
            usage: {
              input_tokens: 0,
              output_tokens: 0,
            },
          }
        })()

        return Object.assign(iterator, {
          interrupt: async () => {},
        })
      },
      sdkGetSessionMessages: async () => {
        throw new Error('first-turn capture should not require history lookup')
      },
    })

    const session = client.createSession(sdkSessionOptions())

    const firstAck = await client.sendInput(session.sessionId, [
      { type: 'text', text: 'first turn only' },
    ])
    await client.waitForTurn(session.sessionId, firstAck.turnId, 4000)

    const hardForkResult = await client.hardForkSession(
      session.sessionId,
      'first_turn_user_boundary'
    )

    expect(hardForkResult.childProviderSessionRef).toBe(
      'sdk_session_fresh_child'
    )
    expect(queryCalls).toHaveLength(2)
    expect(queryCalls[1]?.resume).toBeUndefined()
    expect(queryCalls[1]?.forkSession).toBeUndefined()
    expect(queryCalls[1]?.resumeSessionAt).toBeUndefined()
    expect(typeof queryCalls[1]?.sessionId).toBe('string')
  })

  test('sdk mode sustains parallel session load with approvals', async () => {
    const events: BridgeEvent[] = []
    const client = new ClaudeClient(event => events.push(event), {
      mode: 'sdk',
      sdkQuery: createSdkQueryStub(),
    })

    const sessionCount = 24
    const sessions = Array.from({ length: sessionCount }, () =>
      client.createSession(sdkSessionOptions())
    )
    const turnAcks = await Promise.all(
      sessions.map((session, index) =>
        client.sendInput(session.sessionId, [
          { type: 'text', text: `parallel sdk turn ${index}` },
        ])
      )
    )

    await Promise.all(
      turnAcks.map(async (ack, index) => {
        const session = sessions[index]
        const approvalEvent = await waitForEvent(
          events,
          event =>
            event.event === 'approval.requested' &&
            event.sessionId === session.sessionId &&
            event.turnId === ack.turnId
        )
        const approvalId = approvalEvent.payload.approvalId as string
        await client.respondApproval(
          session.sessionId,
          ack.turnId,
          approvalId,
          'accept'
        )
      })
    )

    const results = await Promise.all(
      turnAcks.map((ack, index) =>
        client.waitForTurn(sessions[index].sessionId, ack.turnId, 5000)
      )
    )
    expect(results.every(result => result.status === 'completed')).toBe(true)
    expect(
      events.some(
        event =>
          event.event === 'error' || event.payload.code === 'INTERNAL_ERROR'
      )
    ).toBe(false)
  })
})

function createSdkQueryCompactionLifecycleStub(
  onCall: (call: {
    prompt: unknown
    options: Record<string, unknown> | undefined
  }) => void
) {
  let callCount = 0
  return ({
    prompt,
    options,
  }: {
    prompt: unknown
    options?: Record<string, unknown>
  }) => {
    callCount += 1
    const currentCall = callCount
    onCall({ prompt, options })

    const iterator = (async function* () {
      if (currentCall === 1) {
        yield {
          type: 'result',
          subtype: 'success',
          session_id: 'sdk_session_compaction',
          result: 'session established',
          usage: {
            input_tokens: 4,
            output_tokens: 2,
          },
        }
        return
      }

      yield {
        type: 'system',
        subtype: 'status',
        session_id: 'sdk_session_compaction',
        status: 'compacting',
      }

      yield {
        type: 'system',
        subtype: 'compact_boundary',
        session_id: 'sdk_session_compaction',
        compact_metadata: {
          trigger: 'manual',
          pre_tokens: 123_456,
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_compaction',
        usage: {
          input_tokens: 2,
          output_tokens: 1,
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryStub(config?: {
  onPrompt?: (prompt: unknown) => void
  onOptions?: (options: Record<string, unknown> | undefined) => void
}) {
  return ({
    prompt,
    options,
  }: {
    prompt: unknown
    options?: Record<string, unknown>
  }) => {
    config?.onPrompt?.(prompt)
    config?.onOptions?.(options)
    const canUseTool = options?.canUseTool as
      | ((
          toolName: string,
          input: unknown,
          runtimeOptions: Record<string, unknown>
        ) => Promise<Record<string, unknown>>)
      | undefined

    if (!canUseTool) {
      throw new Error('sdk query stub requires options.canUseTool')
    }

    const iterator = (async function* () {
      const permissionPromise = canUseTool(
        'Bash',
        { command: 'echo from sdk stub' },
        { suggestions: [{ tool: 'Bash' }] }
      )

      yield {
        type: 'assistant',
        session_id: 'sdk_session_1',
        message: {
          content: [
            {
              type: 'tool_use',
              id: 'tool_1',
              name: 'Bash',
              input: { command: 'echo from sdk stub' },
            },
          ],
        },
      }

      const permission = await permissionPromise
      if (permission.behavior !== 'allow') {
        yield {
          type: 'result',
          subtype: 'error_during_execution',
          session_id: 'sdk_session_1',
          errors: ['tool denied'],
          usage: {
            input_tokens: 8,
            output_tokens: 0,
          },
        }
        return
      }

      yield {
        type: 'user',
        session_id: 'sdk_session_1',
        message: {
          content: [
            {
              type: 'tool_result',
              tool_use_id: 'tool_1',
              content: [{ type: 'text', text: 'command completed' }],
            },
          ],
        },
      }

      yield {
        type: 'stream_event',
        session_id: 'sdk_session_1',
        event: {
          content_block_delta: {
            delta: { text: 'all done' },
          },
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_1',
        result: 'all done',
        usage: {
          input_tokens: 8,
          output_tokens: 3,
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryThinkingStreamStub() {
  return () => {
    const iterator = (async function* () {
      yield {
        type: 'stream_event',
        session_id: 'sdk_session_thinking_stream',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: 'Inspecting prompt. ' },
        },
      }

      yield {
        type: 'stream_event',
        session_id: 'sdk_session_thinking_stream',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: 'Forming answer.' },
        },
      }

      yield {
        type: 'stream_event',
        session_id: 'sdk_session_thinking_stream',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: {
            type: 'signature_delta',
            signature: 'signed-thinking-block',
          },
        },
      }

      yield {
        type: 'stream_event',
        session_id: 'sdk_session_thinking_stream',
        event: {
          type: 'content_block_delta',
          index: 1,
          delta: { type: 'text_delta', text: 'streamed text answer' },
        },
      }

      yield {
        type: 'assistant',
        session_id: 'sdk_session_thinking_stream',
        message: {
          content: [
            {
              type: 'thinking',
              thinking: 'Should not emit this fallback copy',
            },
            { type: 'text', text: 'streamed text answer' },
          ],
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_thinking_stream',
        result: 'streamed text answer',
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryThinkingFallbackStub() {
  return () => {
    const iterator = (async function* () {
      yield {
        type: 'assistant',
        session_id: 'sdk_session_thinking_fallback',
        message: {
          content: [
            { type: 'text', text: 'assistant text' },
            { type: 'thinking', thinking: 'Fallback reasoning block' },
          ],
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_thinking_fallback',
        result: 'assistant text',
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryToolUseSummaryFallbackStub() {
  return () => {
    const iterator = (async function* () {
      yield {
        type: 'assistant',
        session_id: 'sdk_session_tool_use_summary_fallback',
        message: {
          content: [{ type: 'text', text: 'assistant text only' }],
        },
      }

      yield {
        type: 'tool_use_summary',
        session_id: 'sdk_session_tool_use_summary_fallback',
        summary: 'Checked repository context.',
      }

      yield {
        type: 'tool_use_summary',
        session_id: 'sdk_session_tool_use_summary_fallback',
        summary: 'Prepared concise final response.',
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_tool_use_summary_fallback',
        result: 'assistant text only',
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryTerminalResultStub(config: {
  subtype: string
  result: string
  sessionId?: string
}) {
  return () => {
    const iterator = (async function* () {
      yield {
        type: 'result',
        subtype: config.subtype,
        session_id: config.sessionId ?? 'sdk_session_terminal_result',
        result: config.result,
        usage: {
          input_tokens: 5,
          output_tokens: 3,
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQuerySinglePermissionCaptureStub(config: {
  toolName: string
  onPermission: (permission: Record<string, unknown>) => void
}) {
  return ({ options }: { options?: Record<string, unknown> }) => {
    const canUseTool = options?.canUseTool as
      | ((
          toolName: string,
          input: unknown,
          runtimeOptions: Record<string, unknown>
        ) => Promise<Record<string, unknown>>)
      | undefined

    if (!canUseTool) {
      throw new Error('sdk query stub requires options.canUseTool')
    }

    const iterator = (async function* () {
      const permission = await canUseTool(
        config.toolName,
        { team_id: 'team_1' },
        {}
      )
      config.onPermission(permission)

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_permission_capture',
        result: 'permission captured',
        usage: {
          input_tokens: 5,
          output_tokens: 2,
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryModelWindowUsageStub() {
  return () => {
    const iterator = (async function* () {
      yield {
        type: 'assistant',
        session_id: 'sdk_session_usage',
        message: {
          usage: {
            input_tokens: 12_000,
            output_tokens: 400,
            cache_creation_input_tokens: 8_000,
            cache_read_input_tokens: 16_000,
          },
          content: [{ type: 'text', text: 'usage response' }],
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_usage',
        result: 'usage response',
        usage: {
          input_tokens: 67,
          output_tokens: 6_164,
          cache_creation_input_tokens: 83_050,
          cache_read_input_tokens: 2_779_492,
        },
        modelUsage: {
          'claude-sonnet-4-6': {
            inputTokens: 12_000,
            outputTokens: 400,
            cacheReadInputTokens: 16_000,
            cacheCreationInputTokens: 8_000,
            webSearchRequests: 0,
            costUSD: 0.01,
            contextWindow: 1_000_000,
          },
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryMissingUsageSecondTurnStub() {
  let callCount = 0
  return () => {
    callCount += 1
    const turnIndex = callCount
    const iterator = (async function* () {
      if (turnIndex === 1) {
        yield {
          type: 'assistant',
          session_id: 'sdk_session_cache',
          message: {
            usage: {
              input_tokens: 9_000,
              output_tokens: 200,
              cache_creation_input_tokens: 2_000,
              cache_read_input_tokens: 5_000,
            },
            content: [{ type: 'text', text: 'first turn' }],
          },
        }
        yield {
          type: 'result',
          subtype: 'success',
          session_id: 'sdk_session_cache',
          result: 'first turn',
          modelUsage: {
            'claude-sonnet-4-6': {
              inputTokens: 9_000,
              outputTokens: 200,
              cacheReadInputTokens: 5_000,
              cacheCreationInputTokens: 2_000,
              webSearchRequests: 0,
              costUSD: 0.01,
              contextWindow: 200_000,
            },
          },
        }
        return
      }

      yield {
        type: 'assistant',
        session_id: 'sdk_session_cache',
        message: {
          content: [{ type: 'text', text: 'second turn' }],
        },
      }
      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_cache',
        result: 'second turn',
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryTaskOutputStatusStub() {
  return () => {
    const iterator = (async function* () {
      yield {
        type: 'assistant',
        session_id: 'sdk_session_task_output',
        message: {
          content: [
            {
              type: 'tool_use',
              id: 'tool_task_output_1',
              name: 'TaskOutput',
              input: {
                task_id: 'task_background_1',
                block: true,
                timeout: 60_000,
              },
            },
          ],
        },
      }

      yield {
        type: 'user',
        session_id: 'sdk_session_task_output',
        message: {
          content: [
            {
              type: 'tool_result',
              tool_use_id: 'tool_task_output_1',
              content: [
                {
                  type: 'text',
                  text: '{"status":"running","task_id":"task_background_1"}',
                },
              ],
            },
          ],
        },
      }

      yield {
        type: 'user',
        session_id: 'sdk_session_task_output',
        message: {
          content: [
            {
              type: 'tool_result',
              tool_use_id: 'tool_task_output_1',
              content: [
                {
                  type: 'text',
                  text: '{"status":"completed","output":"vitest complete"}',
                },
              ],
            },
          ],
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_task_output',
        result: 'vitest complete',
        usage: {
          input_tokens: 11,
          output_tokens: 4,
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}

function createSdkQueryGgTeamStreamClosedStub(config?: {
  toolName?: string
  onReconnect?: (serverName: string) => void
}) {
  return () => {
    const iterator = (async function* () {
      yield {
        type: 'assistant',
        session_id: 'sdk_session_gg_team_stream_closed',
        message: {
          content: [
            {
              type: 'tool_use',
              id: 'gg_team_tool_1',
              name: config?.toolName ?? 'mcp__gg_team__gg_team_status',
              input: { team_id: 'team_1' },
            },
          ],
        },
      }

      yield {
        type: 'user',
        session_id: 'sdk_session_gg_team_stream_closed',
        message: {
          content: [
            {
              type: 'tool_result',
              tool_use_id: 'gg_team_tool_1',
              content: [{ type: 'text', text: 'Stream closed' }],
            },
          ],
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_gg_team_stream_closed',
        result: 'retry suggested',
        usage: {
          input_tokens: 11,
          output_tokens: 5,
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
      reconnectMcpServer: async (serverName: string) => {
        config?.onReconnect?.(serverName)
      },
    })
  }
}

function createSdkQueryConcurrentGgTeamToolStub(config?: {
  onPermissions?: (permissions: {
    first: Record<string, unknown>
    second: Record<string, unknown>
  }) => void
}) {
  return ({ options }: { options?: Record<string, unknown> }) => {
    const canUseTool = options?.canUseTool as
      | ((
          toolName: string,
          input: unknown,
          runtimeOptions: Record<string, unknown>
        ) => Promise<Record<string, unknown>>)
      | undefined

    if (!canUseTool) {
      throw new Error('sdk query stub requires options.canUseTool')
    }

    const iterator = (async function* () {
      const firstPermissionPromise = canUseTool(
        'mcp__gg_team__gg_team_manage',
        { team_id: 'team_1' },
        {}
      )
      const secondPermissionPromise = canUseTool(
        'mcp__gg_team__gg_team_status',
        { team_id: 'team_1' },
        {}
      )

      yield {
        type: 'assistant',
        session_id: 'sdk_session_gg_team_serialized',
        message: {
          content: [
            {
              type: 'tool_use',
              id: 'gg_team_tool_1',
              name: 'mcp__gg_team__gg_team_manage',
              input: { team_id: 'team_1' },
            },
            {
              type: 'tool_use',
              id: 'gg_team_tool_2',
              name: 'mcp__gg_team__gg_team_status',
              input: { team_id: 'team_1' },
            },
          ],
        },
      }

      const [firstPermission, secondPermission] = await Promise.all([
        firstPermissionPromise,
        secondPermissionPromise,
      ])
      config?.onPermissions?.({
        first: firstPermission,
        second: secondPermission,
      })

      yield {
        type: 'user',
        session_id: 'sdk_session_gg_team_serialized',
        message: {
          content: [
            {
              type: 'tool_result',
              tool_use_id: 'gg_team_tool_1',
              content: [
                {
                  type: 'text',
                  text:
                    firstPermission.behavior === 'allow'
                      ? 'members listed'
                      : String(firstPermission.message ?? 'first denied'),
                },
              ],
            },
            {
              type: 'tool_result',
              tool_use_id: 'gg_team_tool_2',
              content: [
                {
                  type: 'text',
                  text:
                    secondPermission.behavior === 'allow'
                      ? 'status listed'
                      : String(secondPermission.message ?? 'second denied'),
                },
              ],
            },
          ],
        },
      }

      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'sdk_session_gg_team_serialized',
        result: 'serialized gg_team flow complete',
        usage: {
          input_tokens: 20,
          output_tokens: 9,
        },
      }
    })()

    return Object.assign(iterator, {
      interrupt: async () => {},
    })
  }
}
