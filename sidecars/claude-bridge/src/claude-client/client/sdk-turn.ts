import { getOptionalString } from '../guards'
import { buildSdkPrompt } from '../prompt'
import {
  contextWindowFromResultMessage,
  extractCompactBoundaryMetadata,
  extractAssistantDeltas,
  extractSdkMessageUuid,
  extractAssistantReasoningDeltas,
  extractAssistantToolUses,
  extractSdkResultText,
  extractStreamDelta,
  extractStreamReasoningDelta,
  extractToolUseSummary,
  extractUserToolResults,
  hasCompactionInProgressStatus,
  isSdkReplayUserMessage,
  isSdkUserPromptMessage,
  resolveSdkResultStatus,
  updateSessionSdkRef,
  usageFromAssistantMessage,
  usageFromResultMessage,
} from '../sdk-parsing'
import { createSdkQuery } from '../sdk-runtime'
import type {
  ApprovalDecision,
  ClaudeBridgeEventCallback,
  ClaudeInputItem,
  ClaudeTurnResult,
  ClaudeTurnUsage,
  SessionState,
  SdkPermissionResult,
  SdkQueryFn,
  SdkQueryHandle,
  SdkToolItemState,
} from '../types'

export interface SdkTurnDependencies {
  emit: ClaudeBridgeEventCallback
  sdkPrewarmPromise: Promise<void> | null
  sdkQueryOverride?: SdkQueryFn
  handleSdkToolApproval: (
    session: SessionState,
    turnId: string,
    toolName: string,
    input: unknown,
    options: Record<string, unknown>
  ) => Promise<SdkPermissionResult>
  isGgTeamMcpToolName: (toolName: string) => boolean
  isStreamClosedToolResult: (output: unknown) => boolean
  resolveGgTeamMcpServerName: (session: SessionState) => string
  reconnectGgTeamMcpServer: (
    query: SdkQueryHandle,
    sessionId: string,
    turnId: string,
    toolName: string,
    serverName: string
  ) => Promise<void>
  resolveSdkTurnUsage: (
    session: SessionState,
    options: {
      assistantUsage?: ClaudeTurnUsage
      resultUsage?: ClaudeTurnUsage
      contextWindowSize?: number
    }
  ) => ClaudeTurnUsage | undefined
  completeTurn: (session: SessionState, result: ClaudeTurnResult) => void
  recordTurnAssistantMessageUuid: (
    session: SessionState,
    turnId: string,
    uuid: string
  ) => void
  recordTurnUserMessageUuid: (
    session: SessionState,
    turnId: string,
    uuid: string
  ) => void
  resolvePendingApprovalsForTurn: (
    session: SessionState,
    turnId: string,
    decision: ApprovalDecision
  ) => void
}

function isCompactSlashCommandPrompt(prompt: unknown): prompt is string {
  if (typeof prompt !== 'string') {
    return false
  }

  const trimmed = prompt.trim()
  return /^\/compact(?:\s|$)/.test(trimmed)
}

interface TerminalCapacityMatch {
  reason: 'session_window_limit' | 'usage_limit'
  matchedRule: string
  resetWindowHint: string | null
}

function classifyTerminalCapacityMessage(
  text: string
): TerminalCapacityMatch | null {
  const normalized = text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
  const squashed = text.toLowerCase().replace(/[^a-z0-9]+/g, '')
  const hasPhrase = (phrase: string) => {
    const normalizedPhrase = phrase
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, ' ')
      .trim()
    const squashedPhrase = phrase.toLowerCase().replace(/[^a-z0-9]+/g, '')
    return (
      normalized.includes(normalizedPhrase) || squashed.includes(squashedPhrase)
    )
  }

  const startsWithPhrase = (textValue: string, phrase: string) => {
    const normalizedPhrase = phrase
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, ' ')
      .trim()
    return (
      textValue === normalizedPhrase ||
      textValue.startsWith(`${normalizedPhrase} `)
    )
  }

  const stripLeadingPhrase = (textValue: string, phrase: string) => {
    const normalizedPhrase = phrase
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, ' ')
      .trim()
    if (textValue === normalizedPhrase) {
      return ''
    }
    if (textValue.startsWith(`${normalizedPhrase} `)) {
      return textValue.slice(normalizedPhrase.length + 1)
    }
    return null
  }

  const stripOptionalPrefix = (textValue: string) => {
    for (const phrase of [
      'i m sorry but',
      'i am sorry but',
      'sorry but',
      'i m sorry',
      'i am sorry',
      'sorry',
      'unfortunately',
      'apologies',
    ]) {
      const stripped = stripLeadingPhrase(textValue, phrase)
      if (stripped !== null) {
        return stripped
      }
    }
    return textValue
  }

  const hasExhaustionMarker = () => {
    const assertedClause = stripOptionalPrefix(normalized)
    if (
      ['if', 'when', 'whether'].some(phrase =>
        startsWithPhrase(assertedClause, phrase)
      )
    ) {
      return false
    }

    return [
      'you hit',
      'you ve hit',
      'you have hit',
      'you reached',
      'you ve reached',
      'you have reached',
      'i hit',
      'i ve hit',
      'i have hit',
      'i reached',
      'i ve reached',
      'i have reached',
      'i can t continue because',
      'i cannot continue because',
    ].some(phrase => startsWithPhrase(assertedClause, phrase))
  }

  const extractHourHint = (): string | null => {
    const words = normalized.split(/\s+/)
    for (let index = 0; index < words.length - 1; index += 1) {
      const hours = Number.parseInt(words[index] ?? '', 10)
      if (!Number.isFinite(hours) || hours < 1 || hours > 48) {
        continue
      }
      const unit = words[index + 1] ?? ''
      if (unit.startsWith('hour') || unit === 'hr' || unit === 'hrs') {
        return `${hours}h`
      }
    }
    return null
  }

  if (!hasExhaustionMarker()) {
    return null
  }

  if (
    hasPhrase('5 hour limit') ||
    hasPhrase('5 hour window') ||
    hasPhrase('five hour limit')
  ) {
    return {
      reason: 'session_window_limit',
      matchedRule: '5_hour_limit_window',
      resetWindowHint: extractHourHint(),
    }
  }

  if (
    hasPhrase('session limit') &&
    (hasPhrase('try again') ||
      hasPhrase('resets at') ||
      hasPhrase('reset at') ||
      hasPhrase('reset in'))
  ) {
    return {
      reason: 'session_window_limit',
      matchedRule: 'session_limit_with_reset_hint',
      resetWindowHint: extractHourHint(),
    }
  }

  if (
    hasPhrase('usage limit') &&
    (hasPhrase('try again') || hasPhrase('resets') || hasPhrase('reset'))
  ) {
    return {
      reason: 'usage_limit',
      matchedRule: 'usage_limit_with_reset_hint',
      resetWindowHint: extractHourHint(),
    }
  }

  return null
}

export async function runTurnWithSdk(
  deps: SdkTurnDependencies,
  session: SessionState,
  turnId: string,
  input: ClaudeInputItem[]
): Promise<void> {
  const messageItemId = `item_msg_${turnId}`
  let emittedMessageDelta = false
  let emittedStreamReasoningDelta = false
  let emittedAssistantReasoningFallback = false
  let emittedToolUseSummaryFallback = false
  const assistantReasoningFallback = new Map<number, string[]>()
  const toolUseSummaryFallback: string[] = []
  let latestAssistantUsage: ClaudeTurnUsage | undefined
  session.turnToolItems.set(turnId, new Map<string, SdkToolItemState>())

  const emitReasoningDelta = (summaryIndex: number, delta: string) => {
    deps.emit({
      event: 'reasoning.delta',
      sessionId: session.sessionId,
      turnId,
      payload: {
        itemId: messageItemId,
        summaryIndex,
        delta,
      },
    })
  }

  const cacheAssistantReasoningFallback = (
    summaryIndex: number,
    delta: string
  ) => {
    const cached = assistantReasoningFallback.get(summaryIndex) ?? []
    cached.push(delta)
    assistantReasoningFallback.set(summaryIndex, cached)
  }

  const emitAssistantReasoningFallbackIfNeeded = () => {
    if (emittedStreamReasoningDelta || emittedAssistantReasoningFallback) {
      return
    }

    const sortedSummaries = Array.from(
      assistantReasoningFallback.entries()
    ).sort(([left], [right]) => left - right)
    for (const [summaryIndex, deltas] of sortedSummaries) {
      for (const delta of deltas) {
        emitReasoningDelta(summaryIndex, delta)
      }
    }

    if (sortedSummaries.length > 0) {
      emittedAssistantReasoningFallback = true
    }
  }

  const emitToolUseSummaryFallbackIfNeeded = () => {
    if (
      emittedStreamReasoningDelta ||
      emittedAssistantReasoningFallback ||
      emittedToolUseSummaryFallback ||
      assistantReasoningFallback.size > 0
    ) {
      return
    }

    for (const [summaryIndex, summary] of toolUseSummaryFallback.entries()) {
      emitReasoningDelta(summaryIndex, summary)
    }

    if (toolUseSummaryFallback.length > 0) {
      emittedToolUseSummaryFallback = true
    }
  }

  deps.emit({
    event: 'item.started',
    sessionId: session.sessionId,
    turnId,
    payload: {
      item: {
        type: 'agentMessage',
        id: messageItemId,
      },
    },
  })

  try {
    if (deps.sdkPrewarmPromise) {
      await deps.sdkPrewarmPromise
    }

    const promptStream = session.turnPromptStreams.get(turnId)
    const builtPrompt = buildSdkPrompt(session, input)
    const prompt = isCompactSlashCommandPrompt(builtPrompt)
      ? builtPrompt
      : (promptStream?.prompt ?? builtPrompt)
    const query = await createSdkQuery({
      session,
      prompt,
      sdkQueryOverride: deps.sdkQueryOverride,
      canUseTool: (toolName, input, options) =>
        deps.handleSdkToolApproval(session, turnId, toolName, input, options),
    })
    session.activeSdkQuery = query

    for await (const message of query) {
      const updatedClaudeCanonicalSessionRef = updateSessionSdkRef(
        session,
        message
      )
      if (updatedClaudeCanonicalSessionRef) {
        deps.emit({
          event: 'session.updated',
          sessionId: session.sessionId,
          payload: {
            providerSessionRef: session.providerSessionRef,
            claudeCanonicalSessionRef: updatedClaudeCanonicalSessionRef,
          },
        })
      }

      const messageType = getOptionalString(
        (message as Record<string, unknown>).type
      )

      if (messageType === 'assistant') {
        const assistantMessageUuid = extractSdkMessageUuid(message)
        if (assistantMessageUuid) {
          deps.recordTurnAssistantMessageUuid(
            session,
            turnId,
            assistantMessageUuid
          )
        }

        const assistantUsage = usageFromAssistantMessage(message)
        if (assistantUsage) {
          latestAssistantUsage = assistantUsage
        }

        const assistantReasoning = extractAssistantReasoningDeltas(message)
        for (const reasoningDelta of assistantReasoning) {
          cacheAssistantReasoningFallback(
            reasoningDelta.summaryIndex,
            reasoningDelta.delta
          )
        }

        const assistantText = extractAssistantDeltas(message)
        if (!emittedMessageDelta) {
          for (const delta of assistantText) {
            deps.emit({
              event: 'message.delta',
              sessionId: session.sessionId,
              turnId,
              payload: {
                itemId: messageItemId,
                delta,
              },
            })
            emittedMessageDelta = true
          }
        }

        const toolUses = extractAssistantToolUses(message)
        const toolItems = session.turnToolItems.get(turnId)
        if (toolItems) {
          for (const toolUse of toolUses) {
            const itemType =
              toolUse.name === 'Bash' ? 'commandExecution' : 'toolUse'
            const item: SdkToolItemState = {
              itemId: toolUse.id,
              itemType,
              toolName: toolUse.name,
              input: toolUse.input,
            }
            toolItems.set(toolUse.id, item)
            deps.emit({
              event: 'item.started',
              sessionId: session.sessionId,
              turnId,
              payload: {
                item: {
                  type: item.itemType,
                  id: item.itemId,
                  toolName: item.toolName,
                  input: item.input,
                },
              },
            })
          }
        }
        continue
      }

      if (messageType === 'user') {
        const userMessageUuid = extractSdkMessageUuid(message)
        if (
          userMessageUuid &&
          !isSdkReplayUserMessage(message) &&
          isSdkUserPromptMessage(message)
        ) {
          deps.recordTurnUserMessageUuid(session, turnId, userMessageUuid)
        }

        const toolResults = extractUserToolResults(message)
        if (toolResults.length > 0) {
          const toolItems = session.turnToolItems.get(turnId)
          for (const toolResult of toolResults) {
            const existing = toolItems?.get(toolResult.toolUseId)
            const itemType = existing?.itemType ?? 'toolUse'
            const toolName = existing?.toolName ?? 'tool'
            const itemId = existing?.itemId ?? toolResult.toolUseId
            const itemPayload: Record<string, unknown> = {
              type: itemType,
              id: itemId,
              toolName,
              output: toolResult.output,
            }
            if (toolResult.status) {
              itemPayload.status = toolResult.status
            }

            deps.emit({
              event: 'item.completed',
              sessionId: session.sessionId,
              turnId,
              payload: {
                item: itemPayload,
              },
            })

            if (deps.isGgTeamMcpToolName(toolName)) {
              const isStreamClosedResult = deps.isStreamClosedToolResult(
                toolResult.output
              )
              if (isStreamClosedResult) {
                const ggTeamServerName =
                  deps.resolveGgTeamMcpServerName(session)
                await deps.reconnectGgTeamMcpServer(
                  query,
                  session.sessionId,
                  turnId,
                  toolName,
                  ggTeamServerName
                )
              } else if (toolResult.status !== 'in_progress') {
                session.ggTeamToolInFlight = false
                session.ggTeamToolInvocationId = null
              }
            }
          }
        }
        continue
      }

      if (messageType === 'system') {
        if (hasCompactionInProgressStatus(message)) {
          deps.emit({
            event: 'context.compaction',
            sessionId: session.sessionId,
            turnId,
            payload: {
              phase: 'started',
            },
          })
        }

        const compactBoundary = extractCompactBoundaryMetadata(message)
        if (compactBoundary) {
          deps.emit({
            event: 'context.compaction',
            sessionId: session.sessionId,
            turnId,
            payload: {
              phase: 'completed',
              trigger: compactBoundary.trigger,
              preTokens: compactBoundary.preTokens,
            },
          })
        }
        continue
      }

      if (messageType === 'stream_event') {
        const streamReasoningDelta = extractStreamReasoningDelta(message)
        if (streamReasoningDelta) {
          emitReasoningDelta(
            streamReasoningDelta.summaryIndex,
            streamReasoningDelta.delta
          )
          emittedStreamReasoningDelta = true
        }

        const streamDelta = extractStreamDelta(message)
        if (streamDelta) {
          deps.emit({
            event: 'message.delta',
            sessionId: session.sessionId,
            turnId,
            payload: {
              itemId: messageItemId,
              delta: streamDelta,
            },
          })
          emittedMessageDelta = true
        }
        continue
      }

      if (messageType === 'tool_use_summary') {
        const summary = extractToolUseSummary(message)
        if (summary) {
          toolUseSummaryFallback.push(summary)
        }
        continue
      }

      if (messageType === 'result') {
        emitAssistantReasoningFallbackIfNeeded()
        emitToolUseSummaryFallbackIfNeeded()

        const status = resolveSdkResultStatus(message, session, turnId)
        const resultText = extractSdkResultText(message)
        const contextWindowSize = contextWindowFromResultMessage(message)
        const resultUsage = usageFromResultMessage(message)
        const terminalCapacityMatch = resultText
          ? classifyTerminalCapacityMessage(resultText)
          : null
        if (!emittedMessageDelta && resultText) {
          deps.emit({
            event: 'message.delta',
            sessionId: session.sessionId,
            turnId,
            payload: {
              itemId: messageItemId,
              delta: resultText,
            },
          })
          emittedMessageDelta = true
        }

        if (terminalCapacityMatch && resultText) {
          deps.emit({
            event: 'error',
            sessionId: session.sessionId,
            turnId,
            payload: {
              code: 'CAPACITY_EXHAUSTED',
              message: resultText,
              details: {
                providerAuthCapacityClassification: {
                  provider: 'claude',
                  source: 'claude_terminal_assistant_text',
                  reason: terminalCapacityMatch.reason,
                  matchedRule: terminalCapacityMatch.matchedRule,
                  resetWindowHint: terminalCapacityMatch.resetWindowHint,
                },
              },
            },
          })
        }

        deps.emit({
          event: 'item.completed',
          sessionId: session.sessionId,
          turnId,
          payload: {
            item: {
              type: 'agentMessage',
              id: messageItemId,
              text: resultText,
            },
          },
        })

        deps.completeTurn(session, {
          turnId,
          status,
          usage: deps.resolveSdkTurnUsage(session, {
            assistantUsage: latestAssistantUsage,
            resultUsage,
            contextWindowSize,
          }),
        })
        return
      }
    }

    if (session.interruptedTurns.has(turnId)) {
      emitAssistantReasoningFallbackIfNeeded()
      emitToolUseSummaryFallbackIfNeeded()
      session.interruptedTurns.delete(turnId)
      deps.emit({
        event: 'item.completed',
        sessionId: session.sessionId,
        turnId,
        payload: {
          item: {
            type: 'agentMessage',
            id: messageItemId,
          },
        },
      })
      deps.completeTurn(session, {
        turnId,
        status: 'interrupted',
        usage: deps.resolveSdkTurnUsage(session, {
          assistantUsage: latestAssistantUsage,
        }),
      })
    } else {
      emitAssistantReasoningFallbackIfNeeded()
      emitToolUseSummaryFallbackIfNeeded()
      deps.emit({
        event: 'item.completed',
        sessionId: session.sessionId,
        turnId,
        payload: {
          item: {
            type: 'agentMessage',
            id: messageItemId,
          },
        },
      })
      deps.completeTurn(session, {
        turnId,
        status: 'completed',
        usage: deps.resolveSdkTurnUsage(session, {
          assistantUsage: latestAssistantUsage,
        }),
      })
    }
  } catch (error) {
    if (session.interruptedTurns.has(turnId)) {
      emitAssistantReasoningFallbackIfNeeded()
      emitToolUseSummaryFallbackIfNeeded()
      session.interruptedTurns.delete(turnId)
      deps.emit({
        event: 'item.completed',
        sessionId: session.sessionId,
        turnId,
        payload: {
          item: {
            type: 'agentMessage',
            id: messageItemId,
          },
        },
      })
      deps.completeTurn(session, {
        turnId,
        status: 'interrupted',
        usage: deps.resolveSdkTurnUsage(session, {
          assistantUsage: latestAssistantUsage,
        }),
      })
      return
    }

    const errorMessage =
      error instanceof Error ? error.message : 'SDK turn execution failed'
    emitAssistantReasoningFallbackIfNeeded()
    deps.emit({
      event: 'error',
      sessionId: session.sessionId,
      turnId,
      payload: {
        code: 'INTERNAL_ERROR',
        message: errorMessage,
        details: null,
      },
    })
    deps.emit({
      event: 'item.completed',
      sessionId: session.sessionId,
      turnId,
      payload: {
        item: {
          type: 'agentMessage',
          id: messageItemId,
        },
      },
    })
    deps.completeTurn(session, {
      turnId,
      status: 'failed',
      usage: deps.resolveSdkTurnUsage(session, {
        assistantUsage: latestAssistantUsage,
      }),
    })
  } finally {
    session.activeSdkQuery = null
    session.turnToolItems.delete(turnId)
    session.ggTeamToolApprovalPending = false
    session.ggTeamToolInFlight = false
    session.ggTeamToolInvocationId = null
    deps.resolvePendingApprovalsForTurn(session, turnId, 'decline')
  }
}
