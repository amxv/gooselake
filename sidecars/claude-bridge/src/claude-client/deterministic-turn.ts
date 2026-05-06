import { resolveTurnPromptText } from './prompt'
import { usageForPrompt } from './sdk-parsing'
import type {
  ClaudeBridgeEventCallback,
  ClaudeTurnResult,
  PendingApprovalResponse,
  SessionState,
} from './types'

export interface DeterministicTurnContext {
  emit: ClaudeBridgeEventCallback
  createApprovalId: () => string
  completeTurn: (session: SessionState, result: ClaudeTurnResult) => void
  sleep: (ms: number) => Promise<void>
}

export async function runTurnDeterministic(
  context: DeterministicTurnContext,
  session: SessionState,
  turnId: string,
  prompt: string
): Promise<void> {
  const commandItemId = `item_cmd_${turnId}`
  const messageItemId = `item_msg_${turnId}`
  const resolvePrompt = () => resolveTurnPromptText(session, turnId, prompt)

  if (resolvePrompt().includes('[needs-approval]')) {
    context.emit({
      event: 'item.started',
      sessionId: session.sessionId,
      turnId,
      payload: {
        item: {
          type: 'commandExecution',
          id: commandItemId,
          toolName: 'Bash',
          input: {
            command: 'echo hello from claude bridge',
          },
        },
      },
    })

    const approvalId = context.createApprovalId()
    const approvalResponse = await new Promise<PendingApprovalResponse>(
      resolve => {
        session.pendingApprovals.set(approvalId, {
          turnId,
          resolve,
        })

        context.emit({
          event: 'approval.requested',
          sessionId: session.sessionId,
          turnId,
          payload: {
            approvalId,
            requestType: 'tool',
            content: {
              toolName: 'Bash',
              input: {
                command: 'echo hello from claude bridge',
              },
            },
          },
        })
      }
    )

    if (approvalResponse.decision !== 'accept') {
      context.emit({
        event: 'item.completed',
        sessionId: session.sessionId,
        turnId,
        payload: {
          item: {
            type: 'commandExecution',
            id: commandItemId,
            toolName: 'Bash',
            status: 'declined',
          },
        },
      })
      context.completeTurn(session, {
        turnId,
        status: 'failed',
        usage: usageForPrompt(resolvePrompt()),
      })
      return
    }

    context.emit({
      event: 'item.completed',
      sessionId: session.sessionId,
      turnId,
      payload: {
        item: {
          type: 'commandExecution',
          id: commandItemId,
          toolName: 'Bash',
          status: 'completed',
          output: 'hello from claude bridge',
        },
      },
    })
  }

  context.emit({
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

  await context.sleep(resolvePrompt().includes('[slow]') ? 350 : 30)

  if (session.interruptedTurns.has(turnId)) {
    session.interruptedTurns.delete(turnId)
    context.completeTurn(session, {
      turnId,
      status: 'interrupted',
      usage: usageForPrompt(resolvePrompt()),
    })
    return
  }

  const effectivePrompt = resolvePrompt()
  const delta = `fake-claude-response(${effectivePrompt})`
  context.emit({
    event: 'message.delta',
    sessionId: session.sessionId,
    turnId,
    payload: {
      itemId: messageItemId,
      delta,
    },
  })

  context.emit({
    event: 'item.completed',
    sessionId: session.sessionId,
    turnId,
    payload: {
      item: {
        type: 'agentMessage',
        id: messageItemId,
        text: delta,
      },
    },
  })

  context.completeTurn(session, {
    turnId,
    status: 'completed',
    usage: usageForPrompt(effectivePrompt),
  })
}
