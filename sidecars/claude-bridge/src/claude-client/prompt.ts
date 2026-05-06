import type {
  ClaudeInputItem,
  ClaudeVisionMediaType,
  SdkUserContentBlock,
  SdkUserMessage,
  SessionState,
  TurnPromptStreamHandle,
} from './types'

export function setTurnPromptText(
  session: SessionState,
  turnId: string,
  input: ClaudeInputItem[]
): void {
  session.turnPromptTexts.set(turnId, extractPromptText(input))
}

export function appendTurnPromptText(
  session: SessionState,
  turnId: string,
  input: ClaudeInputItem[]
): void {
  const appended = extractPromptText(input)
  if (!appended) {
    return
  }

  const existing = session.turnPromptTexts.get(turnId)
  if (!existing) {
    session.turnPromptTexts.set(turnId, appended)
    return
  }

  session.turnPromptTexts.set(turnId, `${existing}\n${appended}`)
}

export function resolveTurnPromptText(
  session: SessionState,
  turnId: string,
  fallback: string
): string {
  return session.turnPromptTexts.get(turnId) ?? fallback
}

export function createTurnPromptStream(
  session: SessionState,
  initialInput: ClaudeInputItem[]
): TurnPromptStreamHandle {
  const queue: SdkUserMessage[] = []
  const waiters: Array<() => void> = []
  let closed = false

  const notifyWaiter = () => {
    const waiter = waiters.shift()
    waiter?.()
  }

  const pushInput = (input: ClaudeInputItem[]) => {
    const message = buildSdkUserMessage(session, input)
    if (!message) {
      return
    }
    queue.push(message)
    notifyWaiter()
  }

  const close = () => {
    if (closed) {
      return
    }
    closed = true
    while (waiters.length > 0) {
      notifyWaiter()
    }
  }

  pushInput(initialInput)

  return {
    prompt: (async function* (): AsyncIterable<SdkUserMessage> {
      while (true) {
        if (queue.length > 0) {
          yield queue.shift() as SdkUserMessage
          continue
        }
        if (closed) {
          return
        }
        await new Promise<void>(resolve => {
          waiters.push(resolve)
        })
      }
    })(),
    pushInput,
    close,
  }
}

function buildSdkUserMessage(
  session: SessionState,
  input: ClaudeInputItem[]
): SdkUserMessage | null {
  const contentBlocks = buildSdkPromptContentBlocks(input)
  if (contentBlocks.length === 0) {
    return null
  }

  const sessionId =
    session.sdkSessionRef ?? session.providerSessionRef ?? session.sessionId

  return {
    type: 'user',
    session_id: sessionId,
    message: {
      role: 'user',
      content: contentBlocks,
    },
    parent_tool_use_id: null,
  }
}

export function extractPromptText(input: ClaudeInputItem[]): string {
  return input
    .map(item => {
      if (item.type === 'text' && typeof item.text === 'string') {
        return item.text
      }
      if (item.type === 'image') {
        const mediaType =
          typeof item.mediaType === 'string' && item.mediaType.length > 0
            ? item.mediaType
            : 'image'
        return `[Attached image: ${mediaType}]`
      }
      return ''
    })
    .filter(Boolean)
    .join('\n')
}

export function buildSdkPrompt(
  session: SessionState,
  input: ClaudeInputItem[]
): string | AsyncIterable<SdkUserMessage> {
  const containsImage = input.some(item => item.type === 'image')
  if (!containsImage) {
    return extractPromptText(input)
  }

  const contentBlocks = buildSdkPromptContentBlocks(input)
  if (contentBlocks.length === 0) {
    return extractPromptText(input)
  }

  const sessionId =
    session.sdkSessionRef ?? session.providerSessionRef ?? session.sessionId
  return (async function* (): AsyncIterable<SdkUserMessage> {
    yield {
      type: 'user',
      session_id: sessionId,
      message: {
        role: 'user',
        content: contentBlocks,
      },
      parent_tool_use_id: null,
    }
  })()
}

function buildSdkPromptContentBlocks(
  input: ClaudeInputItem[]
): SdkUserContentBlock[] {
  const blocks: SdkUserContentBlock[] = []

  for (const item of input) {
    if (item.type === 'text') {
      const text = item.text?.trim()
      if (text) {
        blocks.push({ type: 'text', text })
      }
      continue
    }

    if (item.type !== 'image') {
      continue
    }

    const mediaType = normalizeClaudeVisionMediaType(item.mediaType)
    const imageData = normalizeBase64Payload(item.data)

    if (!mediaType || !imageData) {
      const fallbackLabel = item.mediaType?.trim() || 'unknown'
      blocks.push({
        type: 'text',
        text: `[Attached image: ${fallbackLabel}]`,
      })
      continue
    }

    blocks.push({
      type: 'image',
      source: {
        type: 'base64',
        media_type: mediaType,
        data: imageData,
      },
    })
  }

  return blocks
}

function normalizeClaudeVisionMediaType(
  mediaType: string | undefined
): ClaudeVisionMediaType | null {
  if (!mediaType) {
    return null
  }

  const normalized = mediaType.trim().toLowerCase().split(';')[0]?.trim()
  if (!normalized) {
    return null
  }

  if (normalized === 'image/jpg') {
    return 'image/jpeg'
  }

  if (
    normalized === 'image/jpeg' ||
    normalized === 'image/png' ||
    normalized === 'image/gif' ||
    normalized === 'image/webp'
  ) {
    return normalized
  }

  return null
}

function normalizeBase64Payload(data: string | undefined): string | null {
  if (!data) {
    return null
  }

  const stripped = data.trim().replace(/^data:[^;]+;base64,/i, '')
  if (!stripped) {
    return null
  }

  return stripped
}
