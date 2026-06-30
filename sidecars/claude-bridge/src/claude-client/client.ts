import { randomUUID } from 'node:crypto'

import { BridgeError } from '../errors'
import { runTurnDeterministic as runDeterministicTurn } from './deterministic-turn'
import { getOptionalRecord, getOptionalString } from './guards'
import {
  appendTurnPromptText,
  createTurnPromptStream,
  extractPromptText,
  setTurnPromptText,
} from './prompt'
import { applyContextWindowToUsage, coerceToolInput } from './sdk-parsing'
import {
  createSdkQuery,
  getSdkSessionMessages,
  getSdkSupportedModels,
  prewarmSdkDependencies,
} from './sdk-runtime'
import {
  createSessionState,
  ensureSdkExternalMcpSessionOptions,
} from './client/session-state'
import { runTurnWithSdk } from './client/sdk-turn'
import type {
  ApprovalDecision,
  BridgeMode,
  ClaudeBridgeEventCallback,
  ClaudeClientOptions,
  ClaudeInputItem,
  ClaudeSessionOptions,
  ClaudeTurnResult,
  ClaudeTurnUsage,
  PendingApprovalResponse,
  SdkGetSessionMessagesFn,
  SdkSessionMessage,
  SdkPermissionResult,
  SdkQueryHandle,
  SdkQueryFn,
  SdkSlashCommand,
  SdkSupportedModel,
  SdkSupportedModelsFn,
  SessionState,
} from './types'

const GG_CALLER_AGENT_ID_TOOL_INPUT_KEY = '__gg_caller_agent_id'
const GG_TOOL_INVOCATION_ID_TOOL_INPUT_KEY = '__gg_tool_invocation_id'
const GG_TEAM_TOOL_PREFIX = 'gg_team_'
const GG_PROCESS_TOOL_PREFIX = 'gg_process_'
const GG_MARKDOWN_TOOL_PREFIX = 'gg_markdown_'
const MCP_TOOL_PREFIX = 'mcp__'
const STREAM_CLOSED_TOOL_RESULT = 'Stream closed'
const GG_TEAM_TOOL_IN_FLIGHT_DENY_MESSAGE =
  'Another gg_team tool call is already in flight for this session. Retry this tool call after the current call completes.'

export class ClaudeClient {
  private readonly sessions = new Map<string, SessionState>()
  private nextSession = 1
  private nextTurn = 1
  private nextApproval = 1
  private nextGgToolInvocation = 1
  private readonly emit: ClaudeBridgeEventCallback
  private readonly mode: BridgeMode
  private readonly sdkQueryOverride?: SdkQueryFn
  private readonly sdkGetSessionMessagesOverride?: SdkGetSessionMessagesFn
  private readonly sdkSupportedModelsOverride?: SdkSupportedModelsFn
  private readonly sdkPrewarmPromise: Promise<void> | null

  constructor(
    emit: ClaudeBridgeEventCallback,
    options: ClaudeClientOptions = {}
  ) {
    this.emit = emit
    this.mode = options.mode ?? 'fake'
    this.sdkQueryOverride = options.sdkQuery
    this.sdkGetSessionMessagesOverride = options.sdkGetSessionMessages
    this.sdkSupportedModelsOverride = options.sdkSupportedModels
    this.sdkPrewarmPromise =
      this.mode === 'sdk'
        ? prewarmSdkDependencies().catch(error => {
            const message =
              error instanceof Error ? error.message : String(error)
            process.stderr.write(
              `Claude SDK prewarm failed; continuing with lazy init: ${message}\\n`
            )
          })
        : null
  }

  createSession(options: ClaudeSessionOptions): {
    sessionId: string
    providerSessionRef: string
    claudeCanonicalSessionRef?: string
    createdAt: number
  } {
    const sessionOptions = ensureSdkExternalMcpSessionOptions(
      this.mode,
      options
    )
    const sessionId = `claude_sess_${this.nextSession++}`
    this.sessions.set(
      sessionId,
      createSessionState({
        sessionId,
        providerSessionRef: sessionId,
        sdkSessionRef: null,
        options: sessionOptions,
      })
    )

    this.emit({
      event: 'session.started',
      sessionId,
      payload: {
        providerSessionRef: sessionId,
        claudeCanonicalSessionRef: null,
        createdAt: unixTimeSeconds(),
      },
    })

    return {
      sessionId,
      providerSessionRef: sessionId,
      claudeCanonicalSessionRef: undefined,
      createdAt: unixTimeSeconds(),
    }
  }

  resumeSession(
    sessionId: string,
    options: ClaudeSessionOptions,
    providerSessionRef?: string,
    claudeCanonicalSessionRef?: string
  ): {
    sessionId: string
    providerSessionRef: string
    claudeCanonicalSessionRef?: string
    createdAt: number
  } {
    const sessionOptions = ensureSdkExternalMcpSessionOptions(
      this.mode,
      options
    )
    const sessionRef = providerSessionRef ?? sessionId
    const canonicalSessionRef = claudeCanonicalSessionRef ?? sessionRef
    const state = this.sessions.get(sessionId)
    if (!state) {
      this.sessions.set(
        sessionId,
        createSessionState({
          sessionId,
          providerSessionRef: sessionRef,
          sdkSessionRef: canonicalSessionRef,
          options: sessionOptions,
        })
      )
    } else {
      this.resolveAllPendingApprovals(state, 'decline')
      for (const stream of state.turnPromptStreams.values()) {
        stream.close()
      }
      state.options = sessionOptions
      state.providerSessionRef = sessionRef
      state.sdkSessionRef = canonicalSessionRef
      state.activeTurnId = null
      state.ggTeamToolApprovalPending = false
      state.ggTeamToolInFlight = false
      state.ggTeamToolInvocationId = null
      state.interruptedTurns.clear()
      state.turnResults.clear()
      state.turnOrder = []
      state.turnWaiters.clear()
      state.pendingApprovals.clear()
      state.activeSdkQuery = null
      state.turnToolItems.clear()
      state.turnPromptTexts.clear()
      state.turnPromptStreams.clear()
      state.turnUserMessageIds.clear()
      state.turnAssistantMessageIds.clear()
      state.turnRollbackBoundaryIds.clear()
      state.userMessageTurnIds.clear()
      state.lastKnownUsage = null
    }

    this.emit({
      event: 'session.resumed',
      sessionId,
      payload: {
        providerSessionRef: sessionRef,
        claudeCanonicalSessionRef: canonicalSessionRef,
        createdAt: unixTimeSeconds(),
      },
    })

    return {
      sessionId,
      providerSessionRef: sessionRef,
      claudeCanonicalSessionRef: canonicalSessionRef,
      createdAt: unixTimeSeconds(),
    }
  }

  async sendInput(
    sessionId: string,
    input: ClaudeInputItem[],
    expectedTurnId?: string | null
  ): Promise<{ turnId: string; status: 'inProgress' }> {
    const session = this.requireSession(sessionId)
    if (session.activeTurnId) {
      if (!expectedTurnId || expectedTurnId !== session.activeTurnId) {
        throw new BridgeError(
          'TURN_IN_PROGRESS',
          `Turn already active in ${sessionId}`,
          {
            sessionId,
            turnId: session.activeTurnId,
          }
        )
      }

      appendTurnPromptText(session, session.activeTurnId, input)
      if (this.mode === 'sdk') {
        const promptStream = session.turnPromptStreams.get(session.activeTurnId)
        if (!promptStream) {
          throw new BridgeError(
            'PROTOCOL_VIOLATION',
            `Active streaming prompt is unavailable for ${sessionId}`,
            {
              sessionId,
              turnId: session.activeTurnId,
            }
          )
        }
        promptStream.pushInput(input)
      }

      return { turnId: session.activeTurnId, status: 'inProgress' }
    }

    const turnId = expectedTurnId ?? `turn_${this.nextTurn++}`
    session.activeTurnId = turnId
    setTurnPromptText(session, turnId, input)
    if (this.mode === 'sdk') {
      session.turnPromptStreams.set(
        turnId,
        createTurnPromptStream(session, input)
      )
    }

    this.emit({
      event: 'turn.started',
      sessionId,
      turnId,
      payload: { turnId },
    })

    void this.runTurn(session, turnId, input)

    return { turnId, status: 'inProgress' }
  }

  async interruptTurn(sessionId: string, turnId: string): Promise<void> {
    const session = this.requireSession(sessionId)
    if (session.activeTurnId !== turnId) {
      throw new BridgeError(
        'TURN_NOT_IN_PROGRESS',
        `Turn ${turnId} is not in progress`,
        {
          sessionId,
          turnId,
        }
      )
    }

    session.interruptedTurns.add(turnId)
    this.resolvePendingApprovalsForTurn(session, turnId, 'decline')

    if (this.mode === 'sdk') {
      const activeQuery = session.activeSdkQuery
      if (activeQuery && typeof activeQuery.interrupt === 'function') {
        try {
          await activeQuery.interrupt()
        } catch {
          // Best effort interruption; wait path resolves terminal status.
        }
      }
    }
  }

  async respondApproval(
    sessionId: string,
    turnId: string,
    approvalId: string,
    decision: ApprovalDecision,
    updatedInput?: unknown
  ): Promise<void> {
    const session = this.requireSession(sessionId)
    const pending = session.pendingApprovals.get(approvalId)
    if (!pending || pending.turnId !== turnId) {
      throw new BridgeError(
        'APPROVAL_NOT_FOUND',
        `Approval ${approvalId} not found`,
        {
          sessionId,
          turnId,
          approvalId,
        }
      )
    }

    session.pendingApprovals.delete(approvalId)
    pending.resolve({
      decision,
      updatedInput,
    })
  }

  async waitForTurn(
    sessionId: string,
    turnId: string,
    timeoutMs: number
  ): Promise<ClaudeTurnResult> {
    const session = this.requireSession(sessionId)
    const completed = session.turnResults.get(turnId)
    if (completed) {
      return completed
    }

    return new Promise((resolve, reject) => {
      let settled = false
      const resolveWrapped = (result: ClaudeTurnResult) => {
        if (settled) {
          return
        }
        settled = true
        clearTimeout(timeout)
        this.removeTurnWaiter(session, turnId, resolveWrapped)
        resolve(result)
      }

      const timeout = setTimeout(
        () => {
          if (settled) {
            return
          }
          settled = true
          this.removeTurnWaiter(session, turnId, resolveWrapped)
          reject(
            new BridgeError(
              'TIMEOUT',
              `Timed out waiting for turn ${turnId} in ${sessionId}`,
              {
                timeoutMs,
              }
            )
          )
        },
        Math.max(1, timeoutMs)
      )

      const waiters = session.turnWaiters.get(turnId) ?? []
      waiters.push(resolveWrapped)
      session.turnWaiters.set(turnId, waiters)
    })
  }

  async hardForkSession(
    sessionId: string,
    rollbackBoundaryId: string
  ): Promise<{ childProviderSessionRef: string }> {
    const session = this.requireSession(sessionId)
    const normalizedRollbackBoundaryId = rollbackBoundaryId.trim()
    if (!normalizedRollbackBoundaryId) {
      throw new BridgeError(
        'BAD_REQUEST',
        'session.hard_fork requires rollbackBoundaryId',
        {
          sessionId,
          rollbackBoundaryId,
        }
      )
    }
    if (session.activeTurnId) {
      throw new BridgeError(
        'TURN_IN_PROGRESS',
        `Cannot hard-fork while turn ${session.activeTurnId} is active`,
        {
          sessionId,
          turnId: session.activeTurnId,
        }
      )
    }
    if (this.mode !== 'sdk') {
      throw new BridgeError(
        'BAD_REQUEST',
        'session.hard_fork requires Claude SDK mode',
        {
          sessionId,
        }
      )
    }
    if (this.sdkPrewarmPromise) {
      await this.sdkPrewarmPromise
    }

    const sourceSessionRef =
      session.sdkSessionRef ?? session.providerSessionRef ?? session.sessionId
    if (!sourceSessionRef.trim()) {
      throw new BridgeError(
        'PROTOCOL_VIOLATION',
        `Cannot hard-fork session ${sessionId} without a Claude provider session ref`,
        {
          sessionId,
        }
      )
    }

    const effectiveBoundaryId = this.resolveRollbackBoundaryIdForTurn(
      session,
      normalizedRollbackBoundaryId
    )
    const capturedBoundary = this.resolveHardForkBoundaryFromCapturedTurns(
      session,
      effectiveBoundaryId
    )
    const historyBoundary =
      capturedBoundary ??
      (await this.resolveHardForkBoundaryFromHistory(
        session,
        sourceSessionRef,
        effectiveBoundaryId
      ))
    const requestedChildSessionRef = randomUUID()
    const childProviderSessionRef = await this.executeSdkHardFork(session, {
      sourceSessionRef,
      requestedChildSessionRef,
      predecessorAssistantUuid: historyBoundary.predecessorAssistantUuid,
    })

    session.sdkSessionRef = childProviderSessionRef
    session.providerSessionRef = childProviderSessionRef
    session.activeTurnId = null
    session.ggTeamToolApprovalPending = false
    session.ggTeamToolInFlight = false
    session.ggTeamToolInvocationId = null
    this.resolveAllPendingApprovals(session, 'decline')
    this.pruneTurnStateForHardFork(session, historyBoundary.rolledBackTurnIds)

    this.emit({
      event: 'session.updated',
      sessionId,
      payload: {
        providerSessionRef: childProviderSessionRef,
        claudeCanonicalSessionRef: childProviderSessionRef,
      },
    })

    return {
      childProviderSessionRef,
    }
  }

  async closeSession(sessionId: string, reason?: string): Promise<void> {
    const session = this.requireSession(sessionId)
    if (
      session.activeSdkQuery &&
      typeof session.activeSdkQuery.interrupt === 'function'
    ) {
      try {
        await session.activeSdkQuery.interrupt()
      } catch {
        // Ignore close-time interrupt errors.
      }
    }

    this.resolveAllPendingApprovals(session, 'decline')
    for (const stream of session.turnPromptStreams.values()) {
      stream.close()
    }
    session.turnPromptStreams.clear()
    session.turnPromptTexts.clear()
    session.turnToolItems.clear()
    session.turnUserMessageIds.clear()
    session.turnAssistantMessageIds.clear()
    session.turnRollbackBoundaryIds.clear()
    session.userMessageTurnIds.clear()
    session.turnResults.clear()
    session.turnWaiters.clear()
    session.turnOrder = []
    session.activeTurnId = null
    this.sessions.delete(sessionId)

    this.emit({
      event: 'session.closed',
      sessionId,
      payload: {
        reason: reason ?? 'closed_by_request',
      },
    })
  }

  async supportedCommands(
    options: ClaudeSessionOptions
  ): Promise<SdkSlashCommand[]> {
    if (this.mode !== 'sdk') {
      return []
    }

    if (this.sdkPrewarmPromise) {
      await this.sdkPrewarmPromise
    }

    const sessionOptions = ensureSdkExternalMcpSessionOptions(
      this.mode,
      options
    )
    const discoverySessionId = `claude_skills_${this.nextSession++}`
    const discoverySession: SessionState = createSessionState({
      sessionId: discoverySessionId,
      providerSessionRef: discoverySessionId,
      sdkSessionRef: null,
      options: sessionOptions,
    })

    let query: SdkQueryHandle | null = null
    try {
      query = await createSdkQuery({
        session: discoverySession,
        prompt: '',
        sdkQueryOverride: this.sdkQueryOverride,
        canUseTool: async () => ({
          behavior: 'deny',
          message:
            'Tool use is disabled while discovering supported slash commands.',
        }),
      })

      if (typeof query.supportedCommands !== 'function') {
        return []
      }

      const supportedCommands = await query.supportedCommands()
      return supportedCommands
        .map(command => ({
          name: typeof command?.name === 'string' ? command.name.trim() : '',
          description:
            typeof command?.description === 'string'
              ? command.description.trim()
              : '',
          argumentHint:
            typeof command?.argumentHint === 'string'
              ? command.argumentHint.trim()
              : '',
        }))
        .filter(command => command.name.length > 0)
    } finally {
      if (query && typeof query.interrupt === 'function') {
        try {
          await query.interrupt()
        } catch {
          // Discovery cleanup is best effort.
        }
      }
    }
  }

  async supportedModels(): Promise<SdkSupportedModel[]> {
    if (this.mode !== 'sdk') {
      return [
        {
          value: 'claude-sonnet-5',
          displayName: 'Claude Sonnet 5',
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
      ]
    }

    if (this.sdkPrewarmPromise) {
      await this.sdkPrewarmPromise
    }

    const supportedModels = await getSdkSupportedModels(
      this.sdkSupportedModelsOverride
    )
    return supportedModels
      .map(model => ({
        value: typeof model?.value === 'string' ? model.value.trim() : '',
        displayName:
          typeof model?.displayName === 'string'
            ? model.displayName.trim()
            : '',
        supportsEffort: Boolean(model?.supportsEffort),
        supportedEffortLevels: Array.isArray(model?.supportedEffortLevels)
          ? model.supportedEffortLevels
              .filter(
                level => typeof level === 'string' && level.trim().length > 0
              )
              .map(level => level.trim().toLowerCase())
          : [],
        supportsVision:
          typeof model?.supportsVision === 'boolean'
            ? model.supportsVision
            : undefined,
        supportsToolCalling:
          typeof model?.supportsToolCalling === 'boolean'
            ? model.supportsToolCalling
            : undefined,
      }))
      .filter(model => model.value.length > 0)
      .map(model => ({
        ...model,
        displayName:
          model.displayName.length > 0 ? model.displayName : model.value,
      }))
  }

  private async runTurn(
    session: SessionState,
    turnId: string,
    input: ClaudeInputItem[]
  ): Promise<void> {
    const promptText = extractPromptText(input)
    if (this.mode === 'sdk') {
      await this.runTurnWithSdk(session, turnId, input)
      return
    }

    await this.runTurnDeterministic(session, turnId, promptText)
  }

  private async runTurnDeterministic(
    session: SessionState,
    turnId: string,
    prompt: string
  ): Promise<void> {
    await runDeterministicTurn(
      {
        emit: this.emit,
        createApprovalId: () => `approval_${this.nextApproval++}`,
        completeTurn: (targetSession, result) =>
          this.completeTurn(targetSession, result),
        sleep,
      },
      session,
      turnId,
      prompt
    )
  }

  private async runTurnWithSdk(
    session: SessionState,
    turnId: string,
    input: ClaudeInputItem[]
  ): Promise<void> {
    await runTurnWithSdk(
      {
        emit: this.emit,
        sdkPrewarmPromise: this.sdkPrewarmPromise,
        sdkQueryOverride: this.sdkQueryOverride,
        handleSdkToolApproval: this.handleSdkToolApproval.bind(this),
        isGgTeamMcpToolName,
        isStreamClosedToolResult,
        resolveGgTeamMcpServerName,
        reconnectGgTeamMcpServer: this.reconnectGgTeamMcpServer.bind(this),
        resolveSdkTurnUsage: this.resolveSdkTurnUsage.bind(this),
        completeTurn: this.completeTurn.bind(this),
        recordTurnAssistantMessageUuid:
          this.recordTurnAssistantMessageUuid.bind(this),
        recordTurnUserMessageUuid: this.recordTurnUserMessageUuid.bind(this),
        resolvePendingApprovalsForTurn:
          this.resolvePendingApprovalsForTurn.bind(this),
      },
      session,
      turnId,
      input
    )
  }

  private async handleSdkToolApproval(
    session: SessionState,
    turnId: string,
    toolName: string,
    input: unknown,
    options: Record<string, unknown>
  ): Promise<SdkPermissionResult> {
    if (session.interruptedTurns.has(turnId)) {
      return {
        behavior: 'deny',
        message: 'Turn interrupted before tool approval',
        interrupt: true,
      }
    }

    const isGgTeamTool = isGgTeamMcpToolName(toolName)
    if (isGgTeamTool) {
      if (session.ggTeamToolApprovalPending || session.ggTeamToolInFlight) {
        return {
          behavior: 'deny',
          message: GG_TEAM_TOOL_IN_FLIGHT_DENY_MESSAGE,
        }
      }
      session.ggTeamToolApprovalPending = true
    }

    let approvedGgTeamTool = false

    try {
      const approvalId = `approval_${this.nextApproval++}`
      const approvalResponse = await new Promise<PendingApprovalResponse>(
        resolve => {
          session.pendingApprovals.set(approvalId, {
            turnId,
            resolve,
          })

          this.emit({
            event: 'approval.requested',
            sessionId: session.sessionId,
            turnId,
            payload: {
              approvalId,
              requestType: 'tool',
              content: {
                toolName,
                input,
                suggestions: getOptionalRecord(options)?.suggestions ?? null,
              },
            },
          })
        }
      )

      if (approvalResponse.decision === 'accept') {
        if (isGgTeamTool) {
          if (!session.ggTeamToolInvocationId) {
            session.ggTeamToolInvocationId = `ggtool_${this.nextGgToolInvocation++}`
          }
          session.ggTeamToolInFlight = true
          approvedGgTeamTool = true
        }
        let updatedInput = coerceToolInput(
          approvalResponse.updatedInput ?? input
        )
        if (isGgScopedMcpToolName(toolName)) {
          updatedInput = injectGgToolMetadataIntoToolInput(updatedInput, {
            callerAgentId: session.options.ggMcpServer?.callerAgentId,
            invocationId: isGgTeamTool ? session.ggTeamToolInvocationId : null,
          })
        }
        return {
          behavior: 'allow',
          updatedInput,
          updatedPermissions:
            getOptionalRecord(options)?.suggestions ?? undefined,
        }
      }

      return {
        behavior: 'deny',
        message: 'Tool execution declined by user',
        interrupt: true,
      }
    } finally {
      if (isGgTeamTool) {
        session.ggTeamToolApprovalPending = false
        if (!approvedGgTeamTool) {
          session.ggTeamToolInFlight = false
        }
      }
    }
  }

  private async reconnectGgTeamMcpServer(
    query: SdkQueryHandle,
    sessionId: string,
    turnId: string,
    toolName: string,
    serverName: string
  ): Promise<void> {
    if (typeof query.reconnectMcpServer !== 'function') {
      return
    }

    try {
      await query.reconnectMcpServer(serverName)
      process.stderr.write(
        `Detected Stream closed tool_result for ${toolName}; reconnected ${serverName} MCP server (sessionId=${sessionId}, turnId=${turnId})\n`
      )
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      process.stderr.write(
        `Failed to reconnect ${serverName} MCP server after Stream closed tool_result for ${toolName} (sessionId=${sessionId}, turnId=${turnId}): ${message}\n`
      )
    }
  }

  private resolvePendingApprovalsForTurn(
    session: SessionState,
    turnId: string,
    decision: ApprovalDecision
  ): void {
    const pendingApprovalIds = Array.from(session.pendingApprovals.entries())
      .filter(([, pending]) => pending.turnId === turnId)
      .map(([approvalId]) => approvalId)

    for (const approvalId of pendingApprovalIds) {
      const pending = session.pendingApprovals.get(approvalId)
      if (!pending) {
        continue
      }
      session.pendingApprovals.delete(approvalId)
      pending.resolve({ decision })
    }
  }

  private resolveAllPendingApprovals(
    session: SessionState,
    decision: ApprovalDecision
  ): void {
    const pendingApprovalIds = Array.from(session.pendingApprovals.keys())
    for (const approvalId of pendingApprovalIds) {
      const pending = session.pendingApprovals.get(approvalId)
      if (!pending) {
        continue
      }
      session.pendingApprovals.delete(approvalId)
      pending.resolve({ decision })
    }
  }

  private resolveSdkTurnUsage(
    session: SessionState,
    options: {
      assistantUsage?: ClaudeTurnUsage
      resultUsage?: ClaudeTurnUsage
      contextWindowSize?: number
    }
  ): ClaudeTurnUsage | undefined {
    const usage =
      options.assistantUsage ?? options.resultUsage ?? session.lastKnownUsage
    return applyContextWindowToUsage(
      usage ?? undefined,
      options.contextWindowSize
    )
  }

  private recordTurnAssistantMessageUuid(
    session: SessionState,
    turnId: string,
    uuid: string
  ): void {
    const normalizedUuid = uuid.trim()
    if (!normalizedUuid) {
      return
    }

    const assistantMessageIds =
      session.turnAssistantMessageIds.get(turnId) ?? []
    if (assistantMessageIds.includes(normalizedUuid)) {
      return
    }

    assistantMessageIds.push(normalizedUuid)
    session.turnAssistantMessageIds.set(turnId, assistantMessageIds)
  }

  private recordTurnUserMessageUuid(
    session: SessionState,
    turnId: string,
    uuid: string
  ): void {
    const normalizedUuid = uuid.trim()
    if (!normalizedUuid) {
      return
    }

    const existingTurnId = session.userMessageTurnIds.get(normalizedUuid)
    if (existingTurnId && existingTurnId !== turnId) {
      return
    }

    const userMessageIds = session.turnUserMessageIds.get(turnId) ?? []
    if (!userMessageIds.includes(normalizedUuid)) {
      userMessageIds.push(normalizedUuid)
      session.turnUserMessageIds.set(turnId, userMessageIds)
    }
    session.userMessageTurnIds.set(normalizedUuid, turnId)
    if (!session.turnRollbackBoundaryIds.has(turnId)) {
      session.turnRollbackBoundaryIds.set(turnId, normalizedUuid)
    }
  }

  private resolveRollbackBoundaryIdForTurn(
    session: SessionState,
    rollbackBoundaryId: string
  ): string {
    const boundaryFromTurnId =
      session.turnRollbackBoundaryIds.get(rollbackBoundaryId)
    if (boundaryFromTurnId) {
      return boundaryFromTurnId
    }

    const targetTurnId = session.userMessageTurnIds.get(rollbackBoundaryId)
    if (!targetTurnId) {
      return rollbackBoundaryId
    }

    return (
      session.turnRollbackBoundaryIds.get(targetTurnId) ?? rollbackBoundaryId
    )
  }

  private resolveHardForkBoundaryFromCapturedTurns(
    session: SessionState,
    rollbackBoundaryId: string
  ): {
    predecessorAssistantUuid: string | null
    rolledBackTurnIds: string[]
  } | null {
    const targetTurnId = session.userMessageTurnIds.get(rollbackBoundaryId)
    if (!targetTurnId) {
      return null
    }

    const targetTurnIndex = session.turnOrder.indexOf(targetTurnId)
    if (targetTurnIndex < 0) {
      return null
    }

    if (targetTurnIndex === 0) {
      return {
        predecessorAssistantUuid: null,
        rolledBackTurnIds: session.turnOrder.slice(targetTurnIndex),
      }
    }

    const previousTurnId = session.turnOrder[targetTurnIndex - 1]
    if (!previousTurnId) {
      return {
        predecessorAssistantUuid: null,
        rolledBackTurnIds: session.turnOrder.slice(targetTurnIndex),
      }
    }

    const previousAssistantMessageIds =
      session.turnAssistantMessageIds.get(previousTurnId) ?? []
    const predecessorAssistantUuid =
      previousAssistantMessageIds[previousAssistantMessageIds.length - 1]
    if (!predecessorAssistantUuid) {
      return null
    }

    return {
      predecessorAssistantUuid,
      rolledBackTurnIds: session.turnOrder.slice(targetTurnIndex),
    }
  }

  private async resolveHardForkBoundaryFromHistory(
    session: SessionState,
    sourceSessionRef: string,
    rollbackBoundaryId: string
  ): Promise<{
    predecessorAssistantUuid: string | null
    rolledBackTurnIds: string[]
  }> {
    const sessionMessages = await getSdkSessionMessages(
      sourceSessionRef,
      {
        dir: session.options.cwd,
      },
      this.sdkGetSessionMessagesOverride
    )

    let targetMessageIndex = sessionMessages.findIndex(message => {
      return (
        message.type === 'user' &&
        message.uuid === rollbackBoundaryId &&
        isSdkSessionHistoryUserPromptMessage(message)
      )
    })

    if (targetMessageIndex < 0) {
      const targetTurnIndex = session.turnOrder.indexOf(rollbackBoundaryId)
      if (targetTurnIndex >= 0) {
        const promptMessageIndexes = sessionMessages
          .map((message, index) => ({
            index,
            message,
          }))
          .filter(({ message }) =>
            isSdkSessionHistoryUserPromptMessage(message)
          )
          .map(({ index }) => index)
        const currentTurnHistoryStart = Math.max(
          0,
          promptMessageIndexes.length - session.turnOrder.length
        )
        const targetPromptOrdinal = currentTurnHistoryStart + targetTurnIndex
        targetMessageIndex =
          promptMessageIndexes[targetPromptOrdinal] ?? targetMessageIndex
      }
    }

    if (targetMessageIndex < 0) {
      throw new BridgeError(
        'TURN_NOT_FOUND',
        `Claude hard-fork boundary ${rollbackBoundaryId} was not found in session history`,
        {
          sessionId: session.sessionId,
          turnId: rollbackBoundaryId,
          providerSessionRef: sourceSessionRef,
        }
      )
    }

    for (let index = targetMessageIndex - 1; index >= 0; index -= 1) {
      const candidate = sessionMessages[index]
      if (candidate?.type !== 'assistant') {
        continue
      }

      const predecessorAssistantUuid = candidate.uuid.trim()
      if (predecessorAssistantUuid) {
        return {
          predecessorAssistantUuid,
          rolledBackTurnIds: resolveRolledBackTurnIdsFromHistory(
            session,
            sessionMessages,
            targetMessageIndex
          ),
        }
      }
    }

    return {
      predecessorAssistantUuid: null,
      rolledBackTurnIds: resolveRolledBackTurnIdsFromHistory(
        session,
        sessionMessages,
        targetMessageIndex
      ),
    }
  }

  private async executeSdkHardFork(
    session: SessionState,
    options: {
      sourceSessionRef: string
      requestedChildSessionRef: string
      predecessorAssistantUuid: string | null
    }
  ): Promise<string> {
    let query: SdkQueryHandle | null = null
    const emptyPrompt = emptySdkPrompt()

    try {
      query = await createSdkQuery({
        session,
        prompt: emptyPrompt,
        sdkQueryOverride: this.sdkQueryOverride,
        canUseTool: async () => ({
          behavior: 'deny',
          message: 'Tool use is disabled while hard-forking Claude history.',
        }),
        runtimeOptionOverrides: options.predecessorAssistantUuid
          ? {
              resume: options.sourceSessionRef,
              forkSession: true,
              resumeSessionAt: options.predecessorAssistantUuid,
              sessionId: options.requestedChildSessionRef,
            }
          : {
              resume: null,
              sessionId: options.requestedChildSessionRef,
            },
      })

      let resolvedChildSessionRef: string | null = null
      for await (const message of query) {
        const messageSessionRef = getOptionalString(
          getOptionalRecord(message)?.session_id
        )
        if (messageSessionRef) {
          resolvedChildSessionRef = messageSessionRef
        }
      }

      const canonicalChildSessionRef =
        resolvedChildSessionRef ?? options.requestedChildSessionRef
      if (
        options.predecessorAssistantUuid &&
        canonicalChildSessionRef === options.sourceSessionRef
      ) {
        throw new BridgeError(
          'PROTOCOL_VIOLATION',
          `Claude hard-fork unexpectedly preserved the source session ref ${options.sourceSessionRef}`,
          {
            sessionId: session.sessionId,
            providerSessionRef: options.sourceSessionRef,
          }
        )
      }

      return canonicalChildSessionRef
    } finally {
      if (query && typeof query.interrupt === 'function') {
        try {
          await query.interrupt()
        } catch {
          // Session mutation cleanup is best effort.
        }
      }
    }
  }

  private pruneTurnStateForHardFork(
    session: SessionState,
    rolledBackTurnIds: string[]
  ): void {
    if (rolledBackTurnIds.length === 0) {
      return
    }
    const rolledBackTurnIdSet = new Set(rolledBackTurnIds)
    session.turnOrder = session.turnOrder.filter(
      turnId => !rolledBackTurnIdSet.has(turnId)
    )

    for (const turnId of rolledBackTurnIds) {
      const userMessageIds = session.turnUserMessageIds.get(turnId) ?? []
      for (const userMessageId of userMessageIds) {
        session.userMessageTurnIds.delete(userMessageId)
      }
      session.turnUserMessageIds.delete(turnId)
      session.turnAssistantMessageIds.delete(turnId)
      session.turnRollbackBoundaryIds.delete(turnId)
      session.turnResults.delete(turnId)
      session.turnWaiters.delete(turnId)
      session.turnToolItems.delete(turnId)

      const promptStream = session.turnPromptStreams.get(turnId)
      if (promptStream) {
        promptStream.close()
      }
      session.turnPromptStreams.delete(turnId)
      session.turnPromptTexts.delete(turnId)
    }
  }

  private completeTurn(session: SessionState, result: ClaudeTurnResult): void {
    const assistantText =
      typeof result.assistantText === 'string' &&
      result.assistantText.trim().length > 0
        ? result.assistantText.trim()
        : undefined
    const normalizedResult: ClaudeTurnResult = {
      ...result,
      assistantText,
    }

    if (normalizedResult.usage) {
      session.lastKnownUsage = normalizedResult.usage
    }
    if (!session.turnOrder.includes(normalizedResult.turnId)) {
      session.turnOrder.push(normalizedResult.turnId)
    }
    session.turnResults.set(normalizedResult.turnId, normalizedResult)
    session.turnToolItems.delete(normalizedResult.turnId)
    this.resolvePendingApprovalsForTurn(
      session,
      normalizedResult.turnId,
      'decline'
    )
    const promptStream = session.turnPromptStreams.get(normalizedResult.turnId)
    if (promptStream) {
      promptStream.close()
      session.turnPromptStreams.delete(normalizedResult.turnId)
    }
    session.turnPromptTexts.delete(normalizedResult.turnId)
    if (session.activeTurnId === normalizedResult.turnId) {
      session.activeTurnId = null
    }

    const waiters = session.turnWaiters.get(normalizedResult.turnId) ?? []
    for (const waiter of waiters) {
      waiter(normalizedResult)
    }
    session.turnWaiters.delete(normalizedResult.turnId)

    this.emit({
      event: 'turn.completed',
      sessionId: session.sessionId,
      turnId: normalizedResult.turnId,
      payload: {
        turnId: normalizedResult.turnId,
        status: normalizedResult.status,
        usage: normalizedResult.usage,
        assistant_text: assistantText ?? null,
      },
    })
  }

  private removeTurnWaiter(
    session: SessionState,
    turnId: string,
    waiter: (result: ClaudeTurnResult) => void
  ): void {
    const waiters = session.turnWaiters.get(turnId)
    if (!waiters) {
      return
    }

    const index = waiters.indexOf(waiter)
    if (index >= 0) {
      waiters.splice(index, 1)
    }

    if (waiters.length === 0) {
      session.turnWaiters.delete(turnId)
    } else {
      session.turnWaiters.set(turnId, waiters)
    }
  }

  private requireSession(sessionId: string): SessionState {
    const session = this.sessions.get(sessionId)
    if (!session) {
      throw new BridgeError(
        'SESSION_NOT_FOUND',
        `Session not found: ${sessionId}`,
        {
          sessionId,
        }
      )
    }
    return session
  }
}

function unixTimeSeconds(): number {
  return Math.floor(Date.now() / 1000)
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function emptySdkPrompt(): AsyncIterable<never> {
  return (async function* (): AsyncIterable<never> {})()
}

function isSdkSessionHistoryUserPromptMessage(
  message: SdkSessionMessage | undefined
): boolean {
  if (!message || message.type !== 'user') {
    return false
  }

  const messageRecord = getOptionalRecord(message.message)
  const content = messageRecord?.content
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

function resolveRolledBackTurnIdsFromHistory(
  session: SessionState,
  sessionMessages: SdkSessionMessage[],
  targetMessageIndex: number
): string[] {
  const promptMessageIndexes = sessionMessages
    .map((message, index) => ({
      index,
      message,
    }))
    .filter(({ message }) => isSdkSessionHistoryUserPromptMessage(message))
    .map(({ index }) => index)

  const targetPromptOrdinal = promptMessageIndexes.indexOf(targetMessageIndex)
  if (targetPromptOrdinal < 0) {
    return []
  }

  const messageIndexByUuid = new Map<string, number>()
  for (const [index, message] of sessionMessages.entries()) {
    const uuid = message.uuid?.trim()
    if (!uuid || messageIndexByUuid.has(uuid)) {
      continue
    }
    messageIndexByUuid.set(uuid, index)
  }

  const rolledBackTurnIds: string[] = []
  for (const turnId of session.turnOrder) {
    const rollbackBoundaryId = session.turnRollbackBoundaryIds.get(turnId)
    if (!rollbackBoundaryId) {
      continue
    }

    const boundaryIndex = messageIndexByUuid.get(rollbackBoundaryId)
    if (
      boundaryIndex !== undefined &&
      boundaryIndex >= targetMessageIndex &&
      !rolledBackTurnIds.includes(turnId)
    ) {
      rolledBackTurnIds.push(turnId)
    }
  }

  if (rolledBackTurnIds.length > 0) {
    return rolledBackTurnIds
  }

  const currentTurnCount = session.turnOrder.length
  if (currentTurnCount === 0) {
    return []
  }

  const currentTurnHistoryStart = Math.max(
    0,
    promptMessageIndexes.length - currentTurnCount
  )
  if (targetPromptOrdinal < currentTurnHistoryStart) {
    return [...session.turnOrder]
  }

  const currentTurnIndex = targetPromptOrdinal - currentTurnHistoryStart
  if (currentTurnIndex >= session.turnOrder.length) {
    return []
  }

  return session.turnOrder.slice(currentTurnIndex)
}

function isGgTeamMcpToolName(toolName: string): boolean {
  const normalizedLeaf = normalizeToolNameLeaf(toolName)
  return normalizedLeaf.startsWith(GG_TEAM_TOOL_PREFIX)
}

function isGgScopedMcpToolName(toolName: string): boolean {
  const normalizedLeaf = normalizeToolNameLeaf(toolName)
  return (
    normalizedLeaf.startsWith(GG_TEAM_TOOL_PREFIX) ||
    normalizedLeaf.startsWith(GG_PROCESS_TOOL_PREFIX) ||
    normalizedLeaf.startsWith(GG_MARKDOWN_TOOL_PREFIX)
  )
}

function injectGgToolMetadataIntoToolInput(
  input: unknown,
  metadata: {
    callerAgentId?: string
    invocationId?: string | null
  }
): unknown {
  const callerAgentIdValue = metadata.callerAgentId?.trim()
  const invocationIdValue = metadata.invocationId?.trim()
  if (!callerAgentIdValue && !invocationIdValue) {
    return input
  }

  const inputRecord = getOptionalRecord(input)
  const nextRecord: Record<string, unknown> = {
    ...(inputRecord ?? {}),
  }
  if (callerAgentIdValue) {
    nextRecord[GG_CALLER_AGENT_ID_TOOL_INPUT_KEY] = callerAgentIdValue
  }
  if (invocationIdValue) {
    nextRecord[GG_TOOL_INVOCATION_ID_TOOL_INPUT_KEY] = invocationIdValue
  }

  return nextRecord
}

function resolveGgTeamMcpServerName(session: SessionState): string {
  if (session.options.ggMcpServer) {
    return session.options.ggMcpServer.serverName?.trim() || 'gg'
  }
  return 'gg'
}

function normalizeToolNameLeaf(toolName: string): string {
  const trimmed = toolName.trim()
  if (!trimmed) {
    return ''
  }

  const afterMcpServerPrefix = trimmed.startsWith(MCP_TOOL_PREFIX)
    ? (() => {
        const separatorIndex = trimmed.lastIndexOf('__')
        if (separatorIndex < 0) {
          return trimmed
        }
        return trimmed.slice(separatorIndex + 2)
      })()
    : trimmed

  const namespaceDelimiter = afterMcpServerPrefix.lastIndexOf('.')
  const leaf =
    namespaceDelimiter >= 0
      ? afterMcpServerPrefix.slice(namespaceDelimiter + 1)
      : afterMcpServerPrefix
  return leaf.trim().toLowerCase()
}

function isStreamClosedToolResult(output: unknown): boolean {
  return output === STREAM_CLOSED_TOOL_RESULT
}
