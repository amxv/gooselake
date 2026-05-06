import { BridgeError } from '../../errors'
import type {
  BridgeMode,
  ClaudeSessionOptions,
  ClaudeTurnResult,
  PendingApproval,
  SessionState,
  SdkToolItemState,
  TurnPromptStreamHandle,
} from '../types'

export function createSessionState(params: {
  sessionId: string
  providerSessionRef: string
  sdkSessionRef: string | null
  options: ClaudeSessionOptions
}): SessionState {
  return {
    sessionId: params.sessionId,
    providerSessionRef: params.providerSessionRef,
    sdkSessionRef: params.sdkSessionRef,
    options: params.options,
    activeTurnId: null,
    ggTeamToolApprovalPending: false,
    ggTeamToolInFlight: false,
    ggTeamToolInvocationId: null,
    interruptedTurns: new Set<string>(),
    turnResults: new Map<string, ClaudeTurnResult>(),
    turnOrder: [],
    turnWaiters: new Map<string, Array<(result: ClaudeTurnResult) => void>>(),
    pendingApprovals: new Map<string, PendingApproval>(),
    activeSdkQuery: null,
    turnToolItems: new Map<string, Map<string, SdkToolItemState>>(),
    turnPromptTexts: new Map<string, string>(),
    turnPromptStreams: new Map<string, TurnPromptStreamHandle>(),
    turnUserMessageIds: new Map<string, string[]>(),
    turnAssistantMessageIds: new Map<string, string[]>(),
    turnRollbackBoundaryIds: new Map<string, string>(),
    userMessageTurnIds: new Map<string, string>(),
    lastKnownUsage: null,
  }
}

export function ensureSdkExternalMcpSessionOptions(
  mode: BridgeMode,
  options: ClaudeSessionOptions
): ClaudeSessionOptions {
  if (mode !== 'sdk') {
    return options
  }

  if (!options.ggMcpServer) {
    throw new BridgeError(
      'BAD_REQUEST',
      'Missing ggMcpServer config for SDK mode session'
    )
  }

  return options
}
