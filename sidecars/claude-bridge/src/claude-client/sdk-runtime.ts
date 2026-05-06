import {
  getSessionMessages as claudeAgentSdkGetSessionMessages,
  query as claudeAgentSdkQuery,
} from '@anthropic-ai/claude-agent-sdk'
import * as claudeAgentSdk from '@anthropic-ai/claude-agent-sdk'
import { existsSync, statSync } from 'node:fs'
import { delimiter, join, sep } from 'node:path'

import { BridgeError } from '../errors'
import type {
  GgMcpServerConfig,
  SdkGetSessionMessagesFn,
  SdkSupportedModel,
  SdkSupportedModelsFn,
  SdkCanUseToolFn,
  SdkQueryFn,
  SdkQueryHandle,
  SdkUserMessage,
  SessionState,
} from './types'

interface CreateSdkQueryParams {
  session: SessionState
  prompt: string | AsyncIterable<SdkUserMessage>
  canUseTool: SdkCanUseToolFn
  sdkQueryOverride?: SdkQueryFn
  runtimeOptionOverrides?: {
    forkSession?: boolean
    resume?: string | null
    resumeSessionAt?: string
    sessionId?: string
  }
}

let cachedSdkQuery: SdkQueryFn | null = null
let cachedSdkGetSessionMessages: SdkGetSessionMessagesFn | null = null
let cachedSdkSupportedModels: SdkSupportedModelsFn | null = null
let cachedClaudeCodeExecutablePath: string | null | undefined = undefined

export async function prewarmSdkDependencies(): Promise<void> {
  resolveClaudeCodeExecutablePath()
  await loadSdkQuery()
  await loadSdkGetSessionMessages()
}

export async function createSdkQuery(
  params: CreateSdkQueryParams
): Promise<SdkQueryHandle> {
  const {
    session,
    prompt,
    canUseTool,
    sdkQueryOverride,
    runtimeOptionOverrides,
  } = params
  const query = sdkQueryOverride ?? (await loadSdkQuery())
  const options: Record<string, unknown> = {
    includePartialMessages: true,
    canUseTool,
    configScope: 'user',
  }

  if (session.options.cwd) {
    options.cwd = session.options.cwd
  }
  if (session.options.model) {
    options.model = session.options.model
  }
  if (session.options.permissionMode) {
    options.permissionMode = session.options.permissionMode
  }
  if (
    session.options.settingSources &&
    session.options.settingSources.length > 0
  ) {
    options.settingSources = session.options.settingSources
  }
  if (session.options.systemPrompt) {
    options.systemPrompt = session.options.systemPrompt
  }
  if (session.options.allowedTools && session.options.allowedTools.length > 0) {
    options.allowedTools = session.options.allowedTools
  }
  if (
    session.options.disallowedTools &&
    session.options.disallowedTools.length > 0
  ) {
    options.disallowedTools = session.options.disallowedTools
  }
  if (session.options.thinkingEffort) {
    options.env = {
      ...process.env,
      CLAUDE_CODE_EFFORT_LEVEL: session.options.thinkingEffort,
    }
  }
  if (runtimeOptionOverrides?.resume !== null) {
    const resumeSessionRef =
      runtimeOptionOverrides?.resume ?? session.sdkSessionRef
    if (resumeSessionRef) {
      options.resume = resumeSessionRef
    }
  }
  if (runtimeOptionOverrides?.forkSession) {
    options.forkSession = true
  }
  if (runtimeOptionOverrides?.resumeSessionAt) {
    options.resumeSessionAt = runtimeOptionOverrides.resumeSessionAt
  }
  if (runtimeOptionOverrides?.sessionId) {
    options.sessionId = runtimeOptionOverrides.sessionId
  }

  const pathToClaudeCodeExecutable = resolveClaudeCodeExecutablePath()
  if (pathToClaudeCodeExecutable) {
    options.pathToClaudeCodeExecutable = pathToClaudeCodeExecutable
  }

  const externalGgMcpServer = createExternalGgMcpServerConfig(
    session.options.ggMcpServer
  )
  if (!externalGgMcpServer) {
    throw new BridgeError(
      'INTERNAL_ERROR',
      'Missing external ggMcpServer config for SDK mode'
    )
  }
  if (Array.isArray(options.allowedTools)) {
    options.allowedTools = normalizeGgMcpToolNamesForServer(
      options.allowedTools as string[],
      externalGgMcpServer.name
    )
  }
  if (Array.isArray(options.disallowedTools)) {
    options.disallowedTools = normalizeGgMcpToolNamesForServer(
      options.disallowedTools as string[],
      externalGgMcpServer.name
    )
  }
  options.mcpServers = {
    [externalGgMcpServer.name]: externalGgMcpServer.server,
  }

  return query({
    prompt,
    options,
  })
}

export async function getSdkSessionMessages(
  sessionId: string,
  options?: {
    dir?: string
    limit?: number
    offset?: number
  },
  sdkGetSessionMessagesOverride?: SdkGetSessionMessagesFn
) {
  const getSessionMessages =
    sdkGetSessionMessagesOverride ?? (await loadSdkGetSessionMessages())
  return getSessionMessages(sessionId, options)
}

export async function getSdkSupportedModels(
  sdkSupportedModelsOverride?: SdkSupportedModelsFn
): Promise<SdkSupportedModel[]> {
  const supportedModels =
    sdkSupportedModelsOverride ?? (await loadSdkSupportedModels())
  return supportedModels()
}

export function resetSdkRuntimeCachesForTests(): void {
  cachedSdkQuery = null
  cachedSdkGetSessionMessages = null
  cachedSdkSupportedModels = null
  cachedClaudeCodeExecutablePath = undefined
}

function createExternalGgMcpServerConfig(config?: GgMcpServerConfig): {
  name: string
  server: Record<string, unknown>
} | null {
  if (!config) {
    return null
  }

  const command = config.command.trim()
  if (!command) {
    throw new BridgeError(
      'BAD_REQUEST',
      'Invalid ggMcpServer config: command is empty'
    )
  }

  const name = config.serverName?.trim() || 'gg'
  const server: Record<string, unknown> = {
    type: 'stdio',
    command,
  }

  const serverEnv: Record<string, string> = {
    ...(config.env ?? {}),
  }
  const callerAgentId = config.callerAgentId?.trim()
  if (callerAgentId && !serverEnv.GG_MCP_CALLER_AGENT_ID?.trim()) {
    // Keep a stable per-session fallback caller identity in case the
    // SDK canUseTool hook is skipped for a tool invocation path.
    serverEnv.GG_MCP_CALLER_AGENT_ID = callerAgentId
  }

  if (config.args && config.args.length > 0) {
    server.args = config.args
  }
  if (Object.keys(serverEnv).length > 0) {
    server.env = serverEnv
  }

  return { name, server }
}

function normalizeGgMcpToolNamesForServer(
  toolNames: string[],
  serverName: string
): string[] {
  const seen = new Set<string>()
  const normalized: string[] = []
  for (const toolName of toolNames) {
    const rewritten = rewriteGgMcpToolNameForServer(toolName, serverName)
    if (seen.has(rewritten)) {
      continue
    }
    seen.add(rewritten)
    normalized.push(rewritten)
  }
  return normalized
}

function rewriteGgMcpToolNameForServer(
  toolName: string,
  serverName: string
): string {
  if (!toolName.startsWith('mcp__')) {
    return toolName
  }

  const afterPrefix = toolName.slice('mcp__'.length)
  const serverSeparatorIndex = afterPrefix.indexOf('__')
  if (serverSeparatorIndex < 0) {
    return toolName
  }

  const toolPart = afterPrefix.slice(serverSeparatorIndex + 2)
  if (
    toolPart.startsWith('gg_team_') ||
    toolPart.startsWith('gg_process_') ||
    toolPart.startsWith('gg_markdown_')
  ) {
    return `mcp__${serverName}__${toolPart}`
  }
  return toolName
}

function resolveClaudeCodeExecutablePath(): string | undefined {
  if (cachedClaudeCodeExecutablePath !== undefined) {
    return cachedClaudeCodeExecutablePath ?? undefined
  }

  const explicitCandidates = [
    process.env.GG_CLAUDE_CODE_EXECUTABLE,
    process.env.CLAUDE_CODE_EXECUTABLE,
  ]
    .map(value => value?.trim())
    .filter((value): value is string => Boolean(value))

  for (const candidate of explicitCandidates) {
    const resolved = resolveExecutableCandidate(candidate)
    if (resolved) {
      cachedClaudeCodeExecutablePath = resolved
      return resolved
    }
  }

  for (const candidate of defaultClaudeExecutableCandidates()) {
    if (isExecutablePath(candidate)) {
      cachedClaudeCodeExecutablePath = candidate
      return candidate
    }
  }

  const pathResolved = resolveExecutableFromPath('claude')
  if (pathResolved) {
    cachedClaudeCodeExecutablePath = pathResolved
    return pathResolved
  }

  cachedClaudeCodeExecutablePath = null
  return undefined
}

function resolveExecutableCandidate(candidate: string): string | undefined {
  if (isExecutablePath(candidate)) {
    return candidate
  }

  if (candidate.includes(sep)) {
    return undefined
  }

  const resolvedFromPath = resolveExecutableFromPath(candidate)
  if (resolvedFromPath) {
    return resolvedFromPath
  }

  return undefined
}

function defaultClaudeExecutableCandidates(): string[] {
  const candidates = new Set<string>()
  const homeDir = process.env.HOME?.trim()
  if (homeDir) {
    candidates.add(join(homeDir, '.local', 'bin', 'claude'))
    candidates.add(join(homeDir, '.bun', 'bin', 'claude'))
    candidates.add(join(homeDir, 'bin', 'claude'))
  }
  candidates.add('/opt/homebrew/bin/claude')
  candidates.add('/usr/local/bin/claude')
  candidates.add('/usr/bin/claude')

  return Array.from(candidates)
}

function resolveExecutableFromPath(commandName: string): string | undefined {
  const pathEnv = process.env.PATH
  if (!pathEnv) {
    return undefined
  }

  const commandNames = commandName.endsWith('.exe')
    ? [commandName]
    : platformExecutableNames(commandName)

  for (const directory of pathEnv.split(delimiter)) {
    const trimmed = directory.trim()
    if (!trimmed) {
      continue
    }
    for (const name of commandNames) {
      const candidate = join(trimmed, name)
      if (isExecutablePath(candidate)) {
        return candidate
      }
    }
  }

  return undefined
}

function platformExecutableNames(commandName: string): string[] {
  if (process.platform !== 'win32') {
    return [commandName]
  }

  const pathExt = process.env.PATHEXT?.split(';')
    .map(value => value.trim().toLowerCase())
    .filter(Boolean) ?? ['.exe', '.cmd', '.bat']

  const names = new Set<string>([commandName])
  for (const ext of pathExt) {
    if (commandName.toLowerCase().endsWith(ext)) {
      names.add(commandName)
      continue
    }
    names.add(`${commandName}${ext}`)
  }
  return Array.from(names)
}

function isExecutablePath(candidate: string): boolean {
  if (!existsSync(candidate)) {
    return false
  }

  try {
    const stat = statSync(candidate)
    return stat.isFile() || stat.isSymbolicLink()
  } catch {
    return false
  }
}

async function loadSdkQuery(): Promise<SdkQueryFn> {
  if (cachedSdkQuery) {
    return cachedSdkQuery
  }

  if (typeof claudeAgentSdkQuery !== 'function') {
    throw new BridgeError(
      'PROTOCOL_VIOLATION',
      'Claude SDK module did not export a query() function'
    )
  }

  cachedSdkQuery = claudeAgentSdkQuery as SdkQueryFn
  return cachedSdkQuery
}

async function loadSdkGetSessionMessages(): Promise<SdkGetSessionMessagesFn> {
  if (cachedSdkGetSessionMessages) {
    return cachedSdkGetSessionMessages
  }

  if (typeof claudeAgentSdkGetSessionMessages !== 'function') {
    throw new BridgeError(
      'PROTOCOL_VIOLATION',
      'Claude SDK module did not export a getSessionMessages() function'
    )
  }

  cachedSdkGetSessionMessages =
    claudeAgentSdkGetSessionMessages as SdkGetSessionMessagesFn
  return cachedSdkGetSessionMessages
}

async function loadSdkSupportedModels(): Promise<SdkSupportedModelsFn> {
  if (cachedSdkSupportedModels) {
    return cachedSdkSupportedModels
  }

  const sdkRecord = claudeAgentSdk as unknown as Record<string, unknown>
  const queryNamespace = sdkRecord.Query as
    | {
        supportedModels?: unknown
      }
    | undefined
  const queryNamespaceSupportedModels = queryNamespace?.supportedModels
  if (typeof queryNamespaceSupportedModels === 'function') {
    cachedSdkSupportedModels =
      queryNamespaceSupportedModels as SdkSupportedModelsFn
    return cachedSdkSupportedModels
  }

  const topLevelSupportedModels = sdkRecord.supportedModels
  if (typeof topLevelSupportedModels === 'function') {
    cachedSdkSupportedModels = topLevelSupportedModels as SdkSupportedModelsFn
    return cachedSdkSupportedModels
  }

  const query = await loadSdkQuery()
  cachedSdkSupportedModels = async () => {
    const probeOptions: Record<string, unknown> = {
      includePartialMessages: false,
      canUseTool: async () => ({
        behavior: 'deny',
        message:
          'Tool use is disabled while discovering supported Claude models.',
      }),
    }
    const pathToClaudeCodeExecutable = resolveClaudeCodeExecutablePath()
    if (pathToClaudeCodeExecutable) {
      probeOptions.pathToClaudeCodeExecutable = pathToClaudeCodeExecutable
    }
    const probeQuery = query({
      prompt: '',
      options: probeOptions,
    })

    try {
      if (typeof probeQuery.supportedModels !== 'function') {
        return []
      }

      return await probeQuery.supportedModels()
    } finally {
      if (typeof probeQuery.interrupt === 'function') {
        try {
          await probeQuery.interrupt()
        } catch {
          // Best-effort cleanup after supported model discovery.
        }
      }
    }
  }

  return cachedSdkSupportedModels
}
