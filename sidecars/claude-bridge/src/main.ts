#!/usr/bin/env bun

import readline from 'node:readline'
import { ClaudeClient } from './claude-client'
import {
  asOptionalString,
  BridgeError,
  ensureString,
  errorResponse,
  successResponse,
} from './errors'
import {
  isRecord,
  parseBridgeRequest,
  PROTOCOL_VERSION,
  type BridgeRequest,
} from './protocol'

const DEFAULT_WAIT_TIMEOUT_MS = 300_000
const bridgeMode = resolveBridgeMode()

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
})

let nextSeq = 1
let shuttingDown = false

const client = new ClaudeClient(
  event => {
    send({
      event: event.event,
      seq: nextSeq++,
      sessionId: event.sessionId,
      turnId: event.turnId ?? null,
      payload: event.payload,
    })
  },
  { mode: bridgeMode }
)

rl.on('line', line => {
  if (!line.trim() || shuttingDown) {
    return
  }

  const request = parseBridgeRequest(line)
  if (!request) {
    process.stderr.write('Invalid bridge request payload\n')
    return
  }

  void processRequest(request)
})

async function processRequest(request: BridgeRequest): Promise<void> {
  try {
    const result = await handleRequest(request)
    send(successResponse(request.id, result))

    if (request.method === 'bridge.shutdown') {
      shuttingDown = true
      rl.close()
      process.exit(0)
    }
  } catch (error) {
    const bridgeError = normalizeError(error)
    send(errorResponse(request.id, bridgeError))
  }
}

async function handleRequest(
  request: BridgeRequest
): Promise<Record<string, unknown>> {
  switch (request.method) {
    case 'bridge.ping':
      return {
        ok: true,
        protocolVersion: PROTOCOL_VERSION,
        runtime: 'bun',
        runtimeVersion: Bun.version,
        mode: bridgeMode,
      }
    case 'bridge.capabilities':
      return {
        provider: 'claude',
        supportsStreaming: true,
        supportsApprovals: true,
        supportsInterrupt: true,
        supportsResume: true,
        supportsTools: true,
        supportsImages: true,
        supportsStructuredOutput: true,
      }
    case 'session.create': {
      const created = client.createSession({
        cwd: asOptionalString(request.params.cwd),
        model: asOptionalString(request.params.model),
        permissionMode: asOptionalString(request.params.permissionMode),
        settingSources: asStringArray(request.params.settingSources),
        systemPrompt: asNullableString(request.params.systemPrompt),
        allowedTools: asStringArray(request.params.allowedTools),
        disallowedTools: asStringArray(request.params.disallowedTools),
        thinkingEffort: asOptionalThinkingEffort(request.params.thinkingEffort),
        ggMcpServer: asGgMcpServerConfig(request.params.ggMcpServer),
      })
      return created
    }
    case 'session.resume': {
      const providerSessionRef = ensureString(
        request.params.providerSessionRef,
        'providerSessionRef'
      )
      const claudeCanonicalSessionRef = asOptionalString(
        request.params.claudeCanonicalSessionRef
      )
      const resumed = client.resumeSession(
        providerSessionRef,
        {
          cwd: asOptionalString(request.params.cwd),
          model: asOptionalString(request.params.model),
          permissionMode: asOptionalString(request.params.permissionMode),
          settingSources: asStringArray(request.params.settingSources),
          systemPrompt: asNullableString(request.params.systemPrompt),
          allowedTools: asStringArray(request.params.allowedTools),
          disallowedTools: asStringArray(request.params.disallowedTools),
          ggMcpServer: asGgMcpServerConfig(request.params.ggMcpServer),
        },
        providerSessionRef,
        claudeCanonicalSessionRef
      )
      return resumed
    }
    case 'session.send': {
      const sessionId = ensureString(request.params.sessionId, 'sessionId')
      const input = asInputItems(request.params.input)
      const expectedTurnId = asNullableString(request.params.expectedTurnId)
      const ack = await client.sendInput(sessionId, input, expectedTurnId)
      return ack
    }
    case 'session.hard_fork': {
      const sessionId = ensureString(request.params.sessionId, 'sessionId')
      const rollbackBoundaryId = ensureString(
        request.params.rollbackBoundaryId,
        'rollbackBoundaryId'
      )
      return await client.hardForkSession(sessionId, rollbackBoundaryId)
    }
    case 'session.interrupt': {
      const sessionId = ensureString(request.params.sessionId, 'sessionId')
      const turnId = ensureString(request.params.turnId, 'turnId')
      await client.interruptTurn(sessionId, turnId)
      return { ok: true }
    }
    case 'session.approval.respond': {
      const sessionId = ensureString(request.params.sessionId, 'sessionId')
      const turnId = ensureString(request.params.turnId, 'turnId')
      const approvalId = ensureString(request.params.approvalId, 'approvalId')
      const decision = ensureDecision(request.params.decision)
      await client.respondApproval(
        sessionId,
        turnId,
        approvalId,
        decision,
        request.params.updatedInput
      )
      return { ok: true }
    }
    case 'session.wait': {
      const sessionId = ensureString(request.params.sessionId, 'sessionId')
      const turnId = ensureString(request.params.turnId, 'turnId')
      const timeoutMs =
        asPositiveInt(request.params.timeoutMs) ?? DEFAULT_WAIT_TIMEOUT_MS
      return await client.waitForTurn(sessionId, turnId, timeoutMs)
    }
    case 'session.supported_commands': {
      const commands = await client.supportedCommands({
        cwd: asOptionalString(request.params.cwd),
        model: asOptionalString(request.params.model),
        permissionMode: asOptionalString(request.params.permissionMode),
        settingSources: asStringArray(request.params.settingSources),
        systemPrompt: asNullableString(request.params.systemPrompt),
        allowedTools: asStringArray(request.params.allowedTools),
        disallowedTools: asStringArray(request.params.disallowedTools),
        thinkingEffort: asOptionalThinkingEffort(request.params.thinkingEffort),
        ggMcpServer: asGgMcpServerConfig(request.params.ggMcpServer),
      })
      return { commands }
    }
    case 'session.supported_models': {
      const models = await client.supportedModels()
      return { models }
    }
    case 'session.close': {
      const sessionId = ensureString(request.params.sessionId, 'sessionId')
      const reason = asOptionalString(request.params.reason)
      await client.closeSession(sessionId, reason)
      return { ok: true }
    }
    case 'bridge.shutdown':
      return { ok: true }
    default:
      throw new BridgeError('BAD_REQUEST', `Unknown method: ${request.method}`)
  }
}

function asStringArray(value: unknown): string[] | undefined {
  if (value === undefined) {
    return undefined
  }
  if (!Array.isArray(value) || value.some(item => typeof item !== 'string')) {
    throw new BridgeError('BAD_REQUEST', 'Expected string[]')
  }
  return value
}

function resolveBridgeMode(): 'fake' | 'sdk' {
  const configuredMode = process.env.GG_CLAUDE_BRIDGE_MODE?.trim().toLowerCase()

  if (configuredMode === 'fake') {
    return 'fake'
  }

  if (configuredMode === 'sdk' || !configuredMode) {
    return 'sdk'
  }

  process.stderr.write(
    `Invalid GG_CLAUDE_BRIDGE_MODE=${configuredMode}; defaulting to sdk\n`
  )
  return 'sdk'
}

function asNullableString(value: unknown): string | null | undefined {
  if (value === undefined) {
    return undefined
  }
  if (value === null) {
    return null
  }
  if (typeof value === 'string') {
    return value
  }
  throw new BridgeError('BAD_REQUEST', 'Expected string | null')
}

function asOptionalThinkingEffort(
  value: unknown
): 'low' | 'medium' | 'high' | 'max' | undefined {
  if (value === undefined || value === null) {
    return undefined
  }
  if (value === 'low' || value === 'medium' || value === 'high' || value === 'max') {
    return value
  }
  throw new BridgeError(
    'BAD_REQUEST',
    'thinkingEffort must be one of: low, medium, high, max'
  )
}

function asPositiveInt(value: unknown): number | undefined {
  if (value === undefined || value === null) {
    return undefined
  }
  if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) {
    throw new BridgeError('BAD_REQUEST', 'Expected positive integer')
  }
  return value
}

function asGgMcpServerConfig(value: unknown):
  | {
      serverName?: string
      callerAgentId?: string
      command: string
      args?: string[]
      env?: Record<string, string>
    }
  | undefined {
  if (value === undefined || value === null) {
    return undefined
  }
  if (!isRecord(value)) {
    throw new BridgeError('BAD_REQUEST', 'Expected ggMcpServer object')
  }

  const serverName = asOptionalString(value.serverName)
  const callerAgentId = asOptionalString(value.callerAgentId)
  const command = ensureString(value.command, 'ggMcpServer.command')
  const args = asStringArray(value.args)
  const env = asStringRecord(value.env)

  return {
    serverName,
    callerAgentId,
    command,
    args,
    env,
  }
}

function asStringRecord(value: unknown): Record<string, string> | undefined {
  if (value === undefined || value === null) {
    return undefined
  }
  if (!isRecord(value)) {
    throw new BridgeError('BAD_REQUEST', 'Expected string record')
  }

  const entries = Object.entries(value)
  for (const [, entryValue] of entries) {
    if (typeof entryValue !== 'string') {
      throw new BridgeError('BAD_REQUEST', 'Expected string record values')
    }
  }
  return Object.fromEntries(entries)
}

function asInputItems(
  value: unknown
): Array<{ type: string; text?: string; data?: string; mediaType?: string }> {
  if (!Array.isArray(value)) {
    throw new BridgeError('BAD_REQUEST', 'Expected input[]')
  }

  return value.map(item => {
    if (!isRecord(item)) {
      throw new BridgeError('BAD_REQUEST', 'Invalid input item')
    }

    const type = ensureString(item.type, 'input.type')
    if (type === 'text') {
      return {
        type,
        text: ensureString(item.text, 'input.text'),
      }
    }

    return {
      type,
      text: asOptionalString(item.text),
      data: asOptionalString(item.data),
      mediaType: asOptionalString(item.mediaType),
    }
  })
}

function ensureDecision(value: unknown): 'accept' | 'decline' {
  if (value === 'accept' || value === 'decline') {
    return value
  }
  throw new BridgeError('BAD_REQUEST', 'Decision must be accept or decline')
}

function normalizeError(error: unknown): BridgeError {
  if (error instanceof BridgeError) {
    return error
  }

  if (error instanceof Error) {
    return new BridgeError('INTERNAL_ERROR', error.message)
  }

  return new BridgeError('INTERNAL_ERROR', 'Unknown bridge error')
}

function send(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value)}\n`)
}
