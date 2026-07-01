export type TurnTerminalStatus = 'completed' | 'interrupted' | 'failed'
export type BridgeMode = 'fake' | 'sdk'
export type ApprovalDecision = 'accept' | 'decline'

export type SdkPermissionAllow = {
  behavior: 'allow'
  updatedInput: unknown
  updatedPermissions?: unknown
}

export type SdkPermissionDeny = {
  behavior: 'deny'
  message: string
  interrupt?: boolean
}

export type SdkPermissionResult = SdkPermissionAllow | SdkPermissionDeny

export type SdkQueryHandle = AsyncIterable<unknown> & {
  interrupt?: () => Promise<void>
  reconnectMcpServer?: (serverName: string) => Promise<void>
  supportedCommands?: () => Promise<SdkSlashCommand[]>
}

export interface SdkSlashCommand {
  name: string
  description: string
  argumentHint: string
}

export type ClaudeVisionMediaType =
  | 'image/jpeg'
  | 'image/png'
  | 'image/gif'
  | 'image/webp'

export type SdkUserContentBlock =
  | {
      type: 'text'
      text: string
    }
  | {
      type: 'image'
      source: {
        type: 'base64'
        media_type: ClaudeVisionMediaType
        data: string
      }
    }

export type SdkUserMessage = {
  type: 'user'
  session_id: string
  message: {
    role: 'user'
    content: string | SdkUserContentBlock[]
  }
  parent_tool_use_id: null
}

export type SdkQueryFn = (args: {
  prompt: string | AsyncIterable<SdkUserMessage>
  options?: Record<string, unknown>
}) => SdkQueryHandle

export interface SdkGetSessionMessagesOptions {
  dir?: string
  limit?: number
  offset?: number
}

export interface SdkSessionMessage {
  type: 'user' | 'assistant'
  uuid: string
  session_id: string
  message: unknown
  parent_tool_use_id: string | null
}

export type SdkGetSessionMessagesFn = (
  sessionId: string,
  options?: SdkGetSessionMessagesOptions
) => Promise<SdkSessionMessage[]>

export interface SdkSupportedModel {
  value: string
  displayName: string
  supportsEffort: boolean
  supportedEffortLevels: string[]
  supportsVision?: boolean
  supportsToolCalling?: boolean
}

export type SdkSupportedModelsFn = () => Promise<SdkSupportedModel[]>

export interface GgMcpServerConfig {
  serverName?: string
  callerAgentId?: string
  command: string
  args?: string[]
  env?: Record<string, string>
}

export type SdkCanUseToolFn = (
  toolName: string,
  input: unknown,
  options: Record<string, unknown>
) => Promise<SdkPermissionResult>

export interface ClaudeInputItem {
  type: string
  text?: string
  data?: string
  mediaType?: string
}

export interface ClaudeSessionOptions {
  cwd?: string
  model?: string
  permissionMode?: string
  settingSources?: string[]
  systemPrompt?: string | null
  allowedTools?: string[]
  disallowedTools?: string[]
  thinkingEffort?: 'low' | 'medium' | 'high' | 'xhigh' | 'max'
  ggMcpServer?: GgMcpServerConfig
}

export interface ClaudeTurnUsage {
  inputTokens: number
  outputTokens: number
  cacheCreationInputTokens?: number
  cacheReadInputTokens?: number
  contextWindowSize?: number
  last_message?: string
  lastMessage?: string
}

export interface ClaudeTurnResult {
  turnId: string
  status: TurnTerminalStatus
  usage?: ClaudeTurnUsage
  assistantText?: string
}

export type ClaudeBridgeEventCallback = (event: {
  event: string
  sessionId: string
  turnId?: string | null
  payload: Record<string, unknown>
}) => void

export interface ClaudeClientOptions {
  mode?: BridgeMode
  sdkQuery?: SdkQueryFn
  sdkGetSessionMessages?: SdkGetSessionMessagesFn
  sdkSupportedModels?: SdkSupportedModelsFn
}

export interface PendingApproval {
  turnId: string
  resolve: (response: PendingApprovalResponse) => void
}

export interface PendingApprovalResponse {
  decision: ApprovalDecision
  updatedInput?: unknown
}

export interface SdkToolItemState {
  itemId: string
  itemType: string
  toolName: string
  input: unknown
}

export interface TurnPromptStreamHandle {
  prompt: AsyncIterable<SdkUserMessage>
  pushInput: (input: ClaudeInputItem[]) => void
  close: () => void
}

export interface SessionState {
  sessionId: string
  providerSessionRef: string
  sdkSessionRef: string | null
  options: ClaudeSessionOptions
  activeTurnId: string | null
  ggTeamToolApprovalPending: boolean
  ggTeamToolInFlight: boolean
  ggTeamToolInvocationId: string | null
  interruptedTurns: Set<string>
  turnResults: Map<string, ClaudeTurnResult>
  turnOrder: string[]
  turnWaiters: Map<string, Array<(result: ClaudeTurnResult) => void>>
  pendingApprovals: Map<string, PendingApproval>
  activeSdkQuery: SdkQueryHandle | null
  turnToolItems: Map<string, Map<string, SdkToolItemState>>
  turnPromptTexts: Map<string, string>
  turnPromptStreams: Map<string, TurnPromptStreamHandle>
  turnUserMessageIds: Map<string, string[]>
  turnAssistantMessageIds: Map<string, string[]>
  turnRollbackBoundaryIds: Map<string, string>
  userMessageTurnIds: Map<string, string>
  lastKnownUsage: ClaudeTurnUsage | null
}
