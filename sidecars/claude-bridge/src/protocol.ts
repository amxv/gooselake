export const PROTOCOL_VERSION = '0.1.0'

export type BridgeMethod =
  | 'bridge.ping'
  | 'bridge.capabilities'
  | 'session.create'
  | 'session.resume'
  | 'session.hard_fork'
  | 'session.send'
  | 'session.interrupt'
  | 'session.approval.respond'
  | 'session.wait'
  | 'session.supported_commands'
  | 'session.supported_models'
  | 'session.close'
  | 'bridge.shutdown'

export type BridgeErrorCode =
  | 'BAD_REQUEST'
  | 'UNAUTHORIZED'
  | 'SESSION_NOT_FOUND'
  | 'TURN_NOT_FOUND'
  | 'TURN_IN_PROGRESS'
  | 'TURN_NOT_IN_PROGRESS'
  | 'APPROVAL_NOT_FOUND'
  | 'PROVIDER_PROCESS_EXITED'
  | 'PROTOCOL_VIOLATION'
  | 'TIMEOUT'
  | 'INTERNAL_ERROR'

export interface BridgeRequest {
  id: string
  method: BridgeMethod
  params: Record<string, unknown>
}

export interface BridgeErrorShape {
  code: BridgeErrorCode
  message: string
  details: unknown
}

export interface BridgeSuccessResponse {
  id: string
  result: Record<string, unknown>
}

export interface BridgeErrorResponse {
  id: string
  error: BridgeErrorShape
}

export type BridgeResponse = BridgeSuccessResponse | BridgeErrorResponse

export interface BridgeEvent {
  event: string
  seq: number
  sessionId: string
  turnId?: string | null
  payload: Record<string, unknown>
}

export interface SessionCreateParams {
  cwd?: string
  model?: string
  permissionMode?: string
  settingSources?: string[]
  systemPrompt?: string | null
  allowedTools?: string[]
  disallowedTools?: string[]
  thinkingEffort?: 'low' | 'medium' | 'high' | 'max'
  ggMcpServer?: {
    serverName?: string
    callerAgentId?: string
    command: string
    args?: string[]
    env?: Record<string, string>
  }
}

export interface SessionResumeParams {
  sessionId: string
  providerSessionRef: string
  claudeCanonicalSessionRef?: string
  cwd?: string
  model?: string
  permissionMode?: string
  settingSources?: string[]
  systemPrompt?: string | null
  allowedTools?: string[]
  disallowedTools?: string[]
  ggMcpServer?: {
    serverName?: string
    callerAgentId?: string
    command: string
    args?: string[]
    env?: Record<string, string>
  }
}

export interface SessionSendParams {
  sessionId: string
  input: Array<{
    type: string
    text?: string
    data?: string
    mediaType?: string
  }>
  expectedTurnId?: string | null
}

export interface SessionHardForkParams {
  sessionId: string
  rollbackBoundaryId: string
}

export interface SessionInterruptParams {
  sessionId: string
  turnId: string
}

export interface SessionApprovalRespondParams {
  sessionId: string
  turnId: string
  approvalId: string
  decision: 'accept' | 'decline'
  updatedInput?: unknown
}

export interface SessionWaitParams {
  sessionId: string
  turnId: string
  timeoutMs?: number
}

export interface SessionSupportedCommandsParams {
  cwd?: string
  model?: string
  permissionMode?: string
  settingSources?: string[]
  systemPrompt?: string | null
  allowedTools?: string[]
  disallowedTools?: string[]
  thinkingEffort?: 'low' | 'medium' | 'high' | 'max'
  forceRefresh?: boolean
  ggMcpServer?: {
    serverName?: string
    callerAgentId?: string
    command: string
    args?: string[]
    env?: Record<string, string>
  }
}

export interface SessionSupportedModelsParams {}

export interface SessionCloseParams {
  sessionId: string
  reason?: string
}

export function parseBridgeRequest(line: string): BridgeRequest | null {
  let raw: unknown
  try {
    raw = JSON.parse(line)
  } catch {
    return null
  }

  if (!isRecord(raw)) {
    return null
  }

  const id = raw.id
  const method = raw.method
  const params = raw.params

  if (
    typeof id !== 'string' ||
    typeof method !== 'string' ||
    !isRecord(params)
  ) {
    return null
  }

  return {
    id,
    method: method as BridgeMethod,
    params,
  }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}
