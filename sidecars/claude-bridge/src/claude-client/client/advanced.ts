import { extractCompactBoundaryMetadata, hasCompactionInProgressStatus } from '../sdk-parsing'
import { realpathSync } from 'node:fs'
import { resolve } from 'node:path'
import { BridgeError } from '../../errors'
import { createSdkQuery } from '../sdk-runtime'
import type { BridgeMode, ClaudeBridgeEventCallback, SdkQueryFn, SessionState } from '../types'

const canonical = (path: string) => {
  try { return realpathSync(path) } catch { return resolve(path) }
}

export function assertIdle(session: SessionState): void {
  if (session.activeTurnId || session.activeSdkQuery || session.pendingApprovals.size || session.rebinding) {
    throw new BridgeError('TURN_IN_PROGRESS', 'Claude session is busy')
  }
}

export async function rebindSession(session: SessionState, destination: string, mode: BridgeMode, sdkQuery?: SdkQueryFn): Promise<Record<string, unknown>> {
  assertIdle(session)
  if (!destination.trim()) throw new BridgeError('BAD_REQUEST', 'destinationCwd must not be empty')
  session.rebinding = true
  try {
    let effectiveCwd = canonical(destination)
    let native = session.sdkSessionRef
    if (mode === 'sdk') {
      if (!native) throw new BridgeError('BAD_REQUEST', 'Rebind requires canonical SDK identity')
      const query = await createSdkQuery({
        session: { ...session, options: { ...session.options, cwd: destination } },
        prompt: (async function* () {})(),
        sdkQueryOverride: sdkQuery,
        runtimeOptionOverrides: { disableBuiltInTools: true },
        canUseTool: async () => ({ behavior: 'deny', message: 'Tools unavailable during cwd verification' }),
      })
      try {
        let verified = false
        for await (const raw of query) {
          const message = raw as Record<string, unknown>
          if (message.type !== 'system' || message.subtype !== 'init') continue
          if (typeof message.cwd !== 'string' || !message.cwd.trim()) throw new BridgeError('PROTOCOL_VIOLATION', 'Initialization omitted cwd')
          effectiveCwd = canonical(message.cwd)
          if (effectiveCwd !== canonical(destination)) throw new BridgeError('PROTOCOL_VIOLATION', 'Initialization cwd mismatch')
          if (typeof message.session_id === 'string' && message.session_id.trim()) {
            native = message.session_id.trim()
          }
          verified = true
          break
        }
        if (!verified) throw new BridgeError('PROTOCOL_VIOLATION', 'Rebind ended before initialization')
      } finally {
        query.close?.()
        await query.interrupt?.()
      }
    }
    session.options = { ...session.options, cwd: destination }
    session.sdkSessionRef = native
    session.bindingGeneration++
    session.requiredInitCwd = effectiveCwd
    return { sessionId: session.sessionId, providerSessionRef: session.providerSessionRef,
      claudeCanonicalSessionRef: native ?? undefined, effectiveCwd, bindingGeneration: session.bindingGeneration }
  } finally { session.rebinding = false }
}

export async function compactSession(session: SessionState, mode: BridgeMode, emit: ClaudeBridgeEventCallback, sdkQuery?: SdkQueryFn): Promise<{ outcome: 'accepted' | 'not_performed' }> {
  assertIdle(session)
  if (session.requiredInitCwd) throw new BridgeError('BAD_REQUEST', 'Claude session is awaiting post-rebind cwd verification')
  if (!session.sdkSessionRef) throw new BridgeError('BAD_REQUEST', 'Compaction requires canonical SDK identity')
  if (mode !== 'sdk') return { outcome: 'not_performed' }
  session.rebinding = true
  let query
  try {
    query = await createSdkQuery({ session, prompt: '/compact', sdkQueryOverride: sdkQuery, runtimeOptionOverrides: { disableBuiltInTools: true },
      canUseTool: async () => ({ behavior: 'deny', message: 'Tools unavailable during manual compaction' }) })
    session.activeSdkQuery = query
    let boundary: ReturnType<typeof extractCompactBoundaryMetadata> = undefined
    let sawInProgress = false
    for await (const raw of query) {
      const message = raw as Record<string, unknown>
      if (message.type === 'system' && message.subtype === 'init' && message.session_id !== session.sdkSessionRef) {
        throw new BridgeError('PROTOCOL_VIOLATION', 'Compaction native identity mismatch')
      }
      if (hasCompactionInProgressStatus(message)) sawInProgress = true
      const observed = extractCompactBoundaryMetadata(message)
      if (observed) boundary = observed
    }
    if (!boundary) return { outcome: 'not_performed' }
    // Manual-compaction correlation is manager-owned. Do not fabricate a
    // bridge-local run id; the manager will supply durable correlation when
    // the typed control API is activated.
    if (sawInProgress) emit({ event: 'context.compaction', sessionId: session.sessionId,
      payload: { phase: 'started' } })
    emit({ event: 'context.compaction', sessionId: session.sessionId,
      payload: { phase: 'completed', ...boundary } })
    return { outcome: 'accepted' }
  } finally {
    if (session.activeSdkQuery === query) session.activeSdkQuery = null
    session.rebinding = false
    query?.close?.()
  }
}
