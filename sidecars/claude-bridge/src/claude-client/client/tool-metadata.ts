const GG_MESSAGE_TOOL_NAME = 'gg_message'
const GG_TEAM_TOOL_NAME = 'gg_team'
const GG_PROCESS_TOOL_NAME = 'gg_process'
const LEGACY_GG_TEAM_TOOL_PREFIX = 'gg_team_'
const LEGACY_GG_PROCESS_TOOL_PREFIX = 'gg_process_'
const LEGACY_GG_MARKDOWN_TOOL_PREFIX = 'gg_markdown_'
const MCP_TOOL_PREFIX = 'mcp__'
const STREAM_CLOSED_TOOL_RESULT = 'Stream closed'

export const GG_SERIALIZED_TOOL_IN_FLIGHT_DENY_MESSAGE =
  'Another serialized GG tool call is already in flight for this session. Retry this tool call after the current call completes.'

export function isGgSerializedMcpToolName(toolName: string): boolean {
  const normalizedLeaf = normalizeToolNameLeaf(toolName)
  return (
    normalizedLeaf === GG_MESSAGE_TOOL_NAME ||
    normalizedLeaf === GG_TEAM_TOOL_NAME ||
    normalizedLeaf.startsWith(LEGACY_GG_TEAM_TOOL_PREFIX)
  )
}

export function isGgScopedMcpToolName(toolName: string): boolean {
  const normalizedLeaf = normalizeToolNameLeaf(toolName)
  return (
    normalizedLeaf === GG_MESSAGE_TOOL_NAME ||
    normalizedLeaf === GG_TEAM_TOOL_NAME ||
    normalizedLeaf === GG_PROCESS_TOOL_NAME ||
    normalizedLeaf.startsWith(LEGACY_GG_TEAM_TOOL_PREFIX) ||
    normalizedLeaf.startsWith(LEGACY_GG_PROCESS_TOOL_PREFIX) ||
    normalizedLeaf.startsWith(LEGACY_GG_MARKDOWN_TOOL_PREFIX)
  )
}

export function isStreamClosedToolResult(output: unknown): boolean {
  return output === STREAM_CLOSED_TOOL_RESULT
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
