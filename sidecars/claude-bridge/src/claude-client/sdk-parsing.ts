import {
  getOptionalNumber,
  getOptionalRecord,
  getOptionalString,
} from './guards'
import type { ClaudeTurnUsage, SessionState, TurnTerminalStatus } from './types'

export function usageForPrompt(prompt: string): ClaudeTurnUsage {
  return {
    inputTokens: Math.max(1, prompt.length),
    outputTokens: 24,
  }
}

function normalizeTokenCount(value: unknown): number | undefined {
  const parsed = getOptionalNumber(value)
  if (parsed === undefined) {
    return undefined
  }
  return Math.max(0, Math.floor(parsed))
}

function extractContextWindowSize(
  usageRaw: Record<string, unknown> | undefined
): number | undefined {
  if (!usageRaw) {
    return undefined
  }

  const candidate =
    normalizeTokenCount(usageRaw.contextWindowSize) ??
    normalizeTokenCount(usageRaw.context_window_size) ??
    normalizeTokenCount(usageRaw.contextWindow) ??
    normalizeTokenCount(usageRaw.context_window)

  if (candidate === undefined || candidate <= 0) {
    return undefined
  }
  return candidate
}

function parseUsageRecord(
  usageRaw: Record<string, unknown> | undefined
): ClaudeTurnUsage | undefined {
  if (!usageRaw) {
    return undefined
  }

  const inputTokens =
    normalizeTokenCount(usageRaw.input_tokens) ??
    normalizeTokenCount(usageRaw.inputTokens)
  const outputTokens =
    normalizeTokenCount(usageRaw.output_tokens) ??
    normalizeTokenCount(usageRaw.outputTokens)
  const cacheCreationInputTokens =
    normalizeTokenCount(usageRaw.cache_creation_input_tokens) ??
    normalizeTokenCount(usageRaw.cacheCreationInputTokens)
  const cacheReadInputTokens =
    normalizeTokenCount(usageRaw.cache_read_input_tokens) ??
    normalizeTokenCount(usageRaw.cacheReadInputTokens)

  if (
    inputTokens === undefined &&
    outputTokens === undefined &&
    cacheCreationInputTokens === undefined &&
    cacheReadInputTokens === undefined
  ) {
    return undefined
  }

  return {
    inputTokens: inputTokens ?? 0,
    outputTokens: outputTokens ?? 0,
    cacheCreationInputTokens: cacheCreationInputTokens ?? 0,
    cacheReadInputTokens: cacheReadInputTokens ?? 0,
    contextWindowSize: extractContextWindowSize(usageRaw),
  }
}

function usageScore(usage: ClaudeTurnUsage): number {
  return (
    usage.inputTokens +
    usage.outputTokens +
    (usage.cacheCreationInputTokens ?? 0) +
    (usage.cacheReadInputTokens ?? 0)
  )
}

function selectPrimaryModelUsage(
  messageRoot: Record<string, unknown> | undefined
): Record<string, unknown> | undefined {
  if (!messageRoot) {
    return undefined
  }

  const modelUsageRaw = getOptionalRecord(messageRoot.modelUsage)
  if (!modelUsageRaw) {
    return undefined
  }

  let selected: Record<string, unknown> | undefined
  let selectedScore = -1
  for (const candidate of Object.values(modelUsageRaw)) {
    const candidateRecord = getOptionalRecord(candidate)
    if (!candidateRecord) {
      continue
    }
    const parsedUsage = parseUsageRecord(candidateRecord)
    const candidateScore = parsedUsage ? usageScore(parsedUsage) : -1
    if (!selected || candidateScore > selectedScore) {
      selected = candidateRecord
      selectedScore = candidateScore
    }
  }

  return selected
}

export function applyContextWindowToUsage(
  usage: ClaudeTurnUsage | undefined,
  contextWindowSize: number | undefined
): ClaudeTurnUsage | undefined {
  if (!usage) {
    return undefined
  }
  if (!contextWindowSize || contextWindowSize <= 0) {
    return usage
  }
  if (usage.contextWindowSize === contextWindowSize) {
    return usage
  }
  return { ...usage, contextWindowSize }
}

export function usageFromAssistantMessage(
  message: unknown
): ClaudeTurnUsage | undefined {
  const messageRoot = getOptionalRecord(message)
  const assistantMessage = getOptionalRecord(messageRoot?.message)
  const assistantUsageRaw = getOptionalRecord(assistantMessage?.usage)
  return parseUsageRecord(assistantUsageRaw)
}

export function contextWindowFromResultMessage(
  message: unknown
): number | undefined {
  const messageRoot = getOptionalRecord(message)
  const topLevelUsageRaw = getOptionalRecord(messageRoot?.usage)
  const topLevelWindow = extractContextWindowSize(topLevelUsageRaw)
  if (topLevelWindow) {
    return topLevelWindow
  }

  const primaryModelUsage = selectPrimaryModelUsage(messageRoot)
  return extractContextWindowSize(primaryModelUsage)
}

export function usageFromResultMessage(
  message: unknown
): ClaudeTurnUsage | undefined {
  const messageRoot = getOptionalRecord(message)
  const topLevelUsage = parseUsageRecord(getOptionalRecord(messageRoot?.usage))
  const modelUsage = parseUsageRecord(selectPrimaryModelUsage(messageRoot))
  const contextWindowSize = contextWindowFromResultMessage(message)

  return applyContextWindowToUsage(
    modelUsage ?? topLevelUsage,
    contextWindowSize
  )
}

export function resolveSdkResultStatus(
  message: unknown,
  session: SessionState,
  turnId: string
): TurnTerminalStatus {
  if (session.interruptedTurns.has(turnId)) {
    session.interruptedTurns.delete(turnId)
    return 'interrupted'
  }

  const subtype = getOptionalString(
    (message as Record<string, unknown>).subtype
  )
  if (subtype === 'success') {
    return 'completed'
  }
  return 'failed'
}

export interface SdkReasoningDelta {
  summaryIndex: number
  delta: string
}

function normalizeSummaryIndex(value: unknown): number {
  const parsed = getOptionalNumber(value)
  if (parsed === undefined || parsed < 0) {
    return 0
  }
  return Math.floor(parsed)
}

export function extractAssistantDeltas(message: unknown): string[] {
  const root = getOptionalRecord(message)
  const assistantMessage = getOptionalRecord(root?.message)
  const content = assistantMessage?.content
  if (!Array.isArray(content)) {
    return []
  }

  return content
    .filter(item => getOptionalString(getOptionalRecord(item)?.type) === 'text')
    .map(item => getOptionalString(getOptionalRecord(item)?.text))
    .filter((value): value is string => Boolean(value))
}

export function extractAssistantReasoningDeltas(
  message: unknown
): SdkReasoningDelta[] {
  const root = getOptionalRecord(message)
  const assistantMessage = getOptionalRecord(root?.message)
  const content = assistantMessage?.content
  if (!Array.isArray(content)) {
    return []
  }

  const deltas: SdkReasoningDelta[] = []
  for (const [summaryIndex, block] of content.entries()) {
    const blockRecord = getOptionalRecord(block)
    if (!blockRecord) {
      continue
    }
    if (getOptionalString(blockRecord.type) !== 'thinking') {
      continue
    }

    const delta = getOptionalString(blockRecord.thinking)
    if (!delta) {
      continue
    }

    deltas.push({
      summaryIndex,
      delta,
    })
  }

  return deltas
}

export function extractAssistantToolUses(message: unknown): Array<{
  id: string
  name: string
  input: unknown
}> {
  const root = getOptionalRecord(message)
  const assistantMessage = getOptionalRecord(root?.message)
  const content = assistantMessage?.content
  if (!Array.isArray(content)) {
    return []
  }

  const result: Array<{ id: string; name: string; input: unknown }> = []

  for (const block of content) {
    const blockRecord = getOptionalRecord(block)
    if (!blockRecord) {
      continue
    }
    if (getOptionalString(blockRecord.type) !== 'tool_use') {
      continue
    }

    const id =
      getOptionalString(blockRecord.id) ??
      `tool_use_${Math.floor(Math.random() * 1_000_000)}`
    const name = getOptionalString(blockRecord.name) ?? 'Tool'

    result.push({
      id,
      name,
      input: blockRecord.input ?? null,
    })
  }

  return result
}

export function extractUserToolResults(message: unknown): Array<{
  toolUseId: string
  output: unknown
  status: string | null
}> {
  const root = getOptionalRecord(message)
  const userMessage = getOptionalRecord(root?.message)
  const content = userMessage?.content
  if (!Array.isArray(content)) {
    return []
  }

  const result: Array<{ toolUseId: string; output: unknown }> = []

  for (const block of content) {
    const blockRecord = getOptionalRecord(block)
    if (!blockRecord) {
      continue
    }
    if (getOptionalString(blockRecord.type) !== 'tool_result') {
      continue
    }

    const toolUseId = getOptionalString(blockRecord.tool_use_id)
    if (!toolUseId) {
      continue
    }

    const structuredToolResult = extractStructuredToolResultPayload(
      root,
      blockRecord
    )
    const output =
      structuredToolResult !== undefined
        ? structuredToolResult
        : normalizeToolResultContent(blockRecord.content)
    result.push({
      toolUseId,
      output,
      status: inferToolResultStatus(output),
    })
  }

  return result
}

export function extractSdkMessageUuid(message: unknown): string | undefined {
  const root = getOptionalRecord(message)
  return getOptionalString(root?.uuid)
}

export function isSdkReplayUserMessage(message: unknown): boolean {
  const root = getOptionalRecord(message)
  return (
    getOptionalString(root?.type) === 'user' && root?.isReplay === true
  )
}

export function isSdkUserPromptMessage(message: unknown): boolean {
  const root = getOptionalRecord(message)
  if (getOptionalString(root?.type) !== 'user') {
    return false
  }

  const userMessage = getOptionalRecord(root?.message)
  const content = userMessage?.content
  if (typeof content === 'string') {
    return content.trim().length > 0
  }
  if (!Array.isArray(content)) {
    return false
  }

  return content.some(block => {
    const blockRecord = getOptionalRecord(block)
    const blockType = getOptionalString(blockRecord?.type)
    return !blockType || blockType !== 'tool_result'
  })
}

function extractStructuredToolResultPayload(
  root: Record<string, unknown> | undefined,
  blockRecord: Record<string, unknown>
): unknown | undefined {
  const rootCandidate =
    root?.toolUseResult ??
    root?.tool_use_result ??
    root?.toolResult ??
    root?.tool_result
  if (rootCandidate !== undefined) {
    return rootCandidate
  }

  const blockCandidate =
    blockRecord.toolUseResult ??
    blockRecord.tool_use_result ??
    blockRecord.toolResult ??
    blockRecord.tool_result
  if (blockCandidate !== undefined) {
    return blockCandidate
  }

  return undefined
}

export function extractSdkResultText(message: unknown): string | undefined {
  const root = getOptionalRecord(message)
  return getOptionalString(root?.result)
}

export function extractToolUseSummary(message: unknown): string | undefined {
  const root = getOptionalRecord(message)
  const summary =
    getOptionalString(root?.summary) ??
    getOptionalString(root?.toolUseSummary) ??
    getOptionalString(root?.tool_use_summary)

  if (!summary || summary.trim().length === 0) {
    return undefined
  }

  return summary
}

export function hasCompactionInProgressStatus(message: unknown): boolean {
  const root = getOptionalRecord(message)
  if (!root) {
    return false
  }

  return (
    getOptionalString(root.type) === 'system' &&
    getOptionalString(root.subtype) === 'status' &&
    getOptionalString(root.status) === 'compacting'
  )
}

export interface SdkCompactBoundaryMetadata {
  trigger: 'manual' | 'auto' | null
  preTokens: number | null
}

function normalizeCompactBoundaryTrigger(
  value: unknown
): SdkCompactBoundaryMetadata['trigger'] {
  const trigger = getOptionalString(value)
  if (trigger === 'manual' || trigger === 'auto') {
    return trigger
  }
  return null
}

export function extractCompactBoundaryMetadata(
  message: unknown
): SdkCompactBoundaryMetadata | undefined {
  const root = getOptionalRecord(message)
  if (!root) {
    return undefined
  }

  if (
    getOptionalString(root.type) !== 'system' ||
    getOptionalString(root.subtype) !== 'compact_boundary'
  ) {
    return undefined
  }

  const metadata =
    getOptionalRecord(root.compact_metadata) ??
    getOptionalRecord(root.compactMetadata)
  return {
    trigger: normalizeCompactBoundaryTrigger(metadata?.trigger),
    preTokens:
      normalizeTokenCount(metadata?.pre_tokens) ??
      normalizeTokenCount(metadata?.preTokens) ??
      null,
  }
}

function normalizeToolResultContent(content: unknown): unknown {
  if (typeof content === 'string') {
    return content
  }

  if (!Array.isArray(content)) {
    return content
  }

  const asText = content
    .map(item => {
      const record = getOptionalRecord(item)
      if (!record) {
        return undefined
      }
      if (getOptionalString(record.type) !== 'text') {
        return undefined
      }
      return getOptionalString(record.text)
    })
    .filter((value): value is string => Boolean(value))

  if (asText.length === 0) {
    return content
  }

  return asText.join('')
}

const TOOL_STATUS_IN_PROGRESS_TOKENS = new Set([
  'inprogress',
  'progress',
  'running',
  'pending',
  'queued',
  'starting',
  'processing',
  'working',
  'waiting',
  'polling',
  'active',
  'ongoing',
])

const TOOL_STATUS_COMPLETED_TOKENS = new Set([
  'completed',
  'complete',
  'success',
  'succeeded',
  'ok',
  'done',
  'finished',
  'resolved',
])

const TOOL_STATUS_FAILED_TOKENS = new Set([
  'failed',
  'fail',
  'error',
  'errored',
  'timeout',
  'timedout',
])

const TOOL_STATUS_INTERRUPTED_TOKENS = new Set([
  'interrupted',
  'cancelled',
  'canceled',
  'aborted',
  'killed',
])

const TOOL_STATUS_DECLINED_TOKENS = new Set(['declined', 'rejected', 'denied'])

function normalizeToolStatusToken(value: string): string | null {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '')
  if (!normalized) {
    return null
  }

  if (TOOL_STATUS_IN_PROGRESS_TOKENS.has(normalized)) {
    return 'in_progress'
  }
  if (TOOL_STATUS_COMPLETED_TOKENS.has(normalized)) {
    return 'completed'
  }
  if (TOOL_STATUS_FAILED_TOKENS.has(normalized)) {
    return 'failed'
  }
  if (TOOL_STATUS_INTERRUPTED_TOKENS.has(normalized)) {
    return 'interrupted'
  }
  if (TOOL_STATUS_DECLINED_TOKENS.has(normalized)) {
    return 'declined'
  }

  return null
}

function inferStatusFromScalar(value: unknown): string | null {
  if (typeof value === 'string') {
    const directStatus = normalizeToolStatusToken(value)
    if (directStatus) {
      return directStatus
    }

    const trimmed = value.trim()
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        return inferToolResultStatus(JSON.parse(trimmed))
      } catch {
        return null
      }
    }

    return null
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return value === 0 ? 'completed' : 'failed'
  }

  return null
}

function inferStatusFromRecord(
  record: Record<string, unknown>,
  depth: number
): string | null {
  for (const key of ['status', 'state', 'phase', 'task_status', 'taskStatus']) {
    const candidate = inferStatusFromScalar(record[key])
    if (candidate) {
      return candidate
    }
  }

  for (const key of ['running', 'isRunning', 'is_running', 'active']) {
    const candidate = record[key]
    if (candidate === true) {
      return 'in_progress'
    }
  }

  for (const key of ['done', 'isDone', 'is_done', 'completed']) {
    const candidate = record[key]
    if (candidate === true) {
      return 'completed'
    }
    if (candidate === false) {
      return 'in_progress'
    }
  }

  for (const key of ['success', 'ok']) {
    const candidate = record[key]
    if (candidate === true) {
      return 'completed'
    }
    if (candidate === false) {
      return 'failed'
    }
  }

  for (const key of ['exitCode', 'exit_code', 'code']) {
    const candidate = inferStatusFromScalar(record[key])
    if (candidate) {
      return candidate
    }
  }

  if (depth < 3) {
    for (const key of [
      'result',
      'output',
      'data',
      'task',
      'task_output',
      'taskOutput',
      'details',
    ]) {
      const nested = inferToolResultStatus(record[key], depth + 1)
      if (nested) {
        return nested
      }
    }
  }

  return null
}

export function inferToolResultStatus(
  value: unknown,
  depth = 0
): string | null {
  if (value === null || value === undefined || depth > 4) {
    return null
  }

  const scalarStatus = inferStatusFromScalar(value)
  if (scalarStatus) {
    return scalarStatus
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const status = inferToolResultStatus(item, depth + 1)
      if (status) {
        return status
      }
    }
    return null
  }

  const record = getOptionalRecord(value)
  if (!record) {
    return null
  }
  return inferStatusFromRecord(record, depth)
}

function extractContentBlockDeltaPayload(event: Record<string, unknown>): {
  summaryIndex: number
  delta: Record<string, unknown> | undefined
} | null {
  const eventType = getOptionalString(event.type)
  if (eventType === 'content_block_delta') {
    return {
      summaryIndex: normalizeSummaryIndex(event.index),
      delta: getOptionalRecord(event.delta),
    }
  }

  const contentBlockDelta = getOptionalRecord(event.content_block_delta)
  if (!contentBlockDelta) {
    return null
  }

  return {
    summaryIndex: normalizeSummaryIndex(contentBlockDelta.index ?? event.index),
    delta: getOptionalRecord(contentBlockDelta.delta) ?? contentBlockDelta,
  }
}

export function extractStreamReasoningDelta(
  message: unknown
): SdkReasoningDelta | undefined {
  const root = getOptionalRecord(message)
  const event = getOptionalRecord(root?.event)
  if (!event) {
    return undefined
  }

  const contentBlockDelta = extractContentBlockDeltaPayload(event)
  if (!contentBlockDelta) {
    return undefined
  }

  const deltaType = getOptionalString(contentBlockDelta.delta?.type)
  if (deltaType === 'signature_delta') {
    return undefined
  }

  const thinking = getOptionalString(contentBlockDelta.delta?.thinking)
  if (!thinking) {
    return undefined
  }
  if (deltaType && deltaType !== 'thinking_delta') {
    return undefined
  }

  return {
    summaryIndex: contentBlockDelta.summaryIndex,
    delta: thinking,
  }
}

export function extractStreamDelta(message: unknown): string | undefined {
  const root = getOptionalRecord(message)
  const event = getOptionalRecord(root?.event)
  if (!event) {
    return undefined
  }

  const directDelta = getOptionalRecord(event.delta)
  const directDeltaText = getOptionalString(directDelta?.text)
  const directDeltaType = getOptionalString(directDelta?.type)
  if (
    directDeltaText &&
    (!directDeltaType || directDeltaType === 'text_delta')
  ) {
    return directDeltaText
  }

  const contentBlockDelta = extractContentBlockDeltaPayload(event)
  const contentBlockDeltaText = getOptionalString(
    contentBlockDelta?.delta?.text
  )
  const contentBlockDeltaType = getOptionalString(
    contentBlockDelta?.delta?.type
  )
  if (
    contentBlockDeltaText &&
    (!contentBlockDeltaType || contentBlockDeltaType === 'text_delta')
  ) {
    return contentBlockDeltaText
  }

  const candidates = [
    getOptionalString(getOptionalRecord(event.message_delta)?.text),
  ]

  return candidates.find((candidate): candidate is string => Boolean(candidate))
}

export function updateSessionSdkRef(
  session: SessionState,
  message: unknown
): string | null {
  const sdkSessionId = getOptionalString(getOptionalRecord(message)?.session_id)
  if (!sdkSessionId) {
    return null
  }

  if (session.sdkSessionRef === sdkSessionId) {
    return null
  }

  session.sdkSessionRef = sdkSessionId
  return sdkSessionId
}

export function coerceToolInput(input: unknown): unknown {
  const record = getOptionalRecord(input)
  if (record) {
    return record
  }
  return input
}
