import { BridgeError } from '../errors'
import { isRecord } from '../protocol'
import type {
  ClaudePermissionIntent,
  ClaudePermissionMode,
  ClaudeSessionOptions,
  ClaudeSettingSource,
  ClaudeSettingSourcesIntent,
} from './types'

const PERMISSION_MODES = new Set<ClaudePermissionMode>([
  'default',
  'acceptEdits',
  'bypassPermissions',
  'plan',
  'dontAsk',
])
const SETTING_SOURCES = new Set<ClaudeSettingSource>([
  'user',
  'project',
  'local',
])
const CANONICAL_SOURCE_ORDER = new Map<ClaudeSettingSource, number>([
  ['user', 0],
  ['project', 1],
  ['local', 2],
])
const FORBIDDEN_SDK_POLICY_FIELDS = [
  'permissionMode',
  'settingSources',
  'allowDangerouslySkipPermissions',
  'configScope',
  'sandbox',
] as const

export const INHERIT_PERMISSION_INTENT: ClaudePermissionIntent = {
  kind: 'inherit_provider_configuration',
}

export function parseClaudeSessionOptionsPolicy(
  params: Record<string, unknown>,
  cwd?: string
): {
  permissionIntent: ClaudePermissionIntent
  settingSourcesIntent: ClaudeSettingSourcesIntent
} {
  rejectDirectSdkPolicyFields(params)
  const permissionIntent = parseClaudePermissionIntent(params.permissionIntent)
  const settingSourcesIntent = parseClaudeSettingSourcesIntent(
    params.settingSourcesIntent
  )
  validateClaudeSessionPolicy(
    {
      permissionIntent,
      settingSourcesIntent,
    },
    cwd
  )
  return { permissionIntent, settingSourcesIntent }
}

export function parseClaudeSessionBindingPolicy(
  params: Record<string, unknown>,
  cwd?: string
): Pick<ClaudeSessionOptions, 'settingSourcesIntent'> {
  rejectDirectSdkPolicyFields(params)
  if ('permissionIntent' in params) {
    throw badPolicy(
      'Session create and resume cannot retain permission authority; send it on session.send'
    )
  }
  const settingSourcesIntent = parseClaudeSettingSourcesIntent(
    params.settingSourcesIntent
  )
  validateClaudeSettingSourcesIntent(settingSourcesIntent, cwd)
  return { settingSourcesIntent }
}

export function parseClaudeSettingSourcesPolicy(
  params: Record<string, unknown>,
  cwd?: string
): ClaudeSettingSourcesIntent {
  rejectDirectSdkPolicyFields(params)
  const settingSourcesIntent = parseClaudeSettingSourcesIntent(
    params.settingSourcesIntent
  )
  validateClaudeSettingSourcesIntent(settingSourcesIntent, cwd)
  return settingSourcesIntent
}

export function validateClaudeSessionOptions(
  options: ClaudeSessionOptions
): ClaudeSessionOptions {
  validateClaudeSettingSourcesIntent(options.settingSourcesIntent, options.cwd)
  if (
    typeof options.harnessInstructions !== 'string' ||
    options.harnessInstructions.trim().length === 0
  ) {
    throw new BridgeError(
      'BAD_REQUEST',
      'Claude regular sessions require nonblank harnessInstructions'
    )
  }
  return options
}

export function validateClaudeSessionPolicy(
  policy: {
    permissionIntent: ClaudePermissionIntent
    settingSourcesIntent: ClaudeSettingSourcesIntent
  },
  cwd?: string
): void {
  validatePermissionIntent(policy.permissionIntent)
  validateClaudeSettingSourcesIntent(policy.settingSourcesIntent, cwd)
}

export function resolveClaudeSettingSources(
  intent: ClaudeSettingSourcesIntent,
  cwd?: string
): ClaudeSettingSource[] {
  validateClaudeSettingSourcesIntent(intent, cwd)
  switch (intent.kind) {
    case 'standard':
      return hasWorkingDirectory(cwd) ? ['user', 'project', 'local'] : ['user']
    case 'explicit':
      return [...intent.sources]
    case 'isolated':
      return []
  }
}

export function parseClaudePermissionIntent(
  value: unknown
): ClaudePermissionIntent {
  if (!isRecord(value)) {
    throw badPolicy('permissionIntent must be a tagged object')
  }
  if (value.kind === 'inherit_provider_configuration') {
    ensureExactKeys(value, ['kind'], 'permissionIntent')
    return INHERIT_PERMISSION_INTENT
  }
  if (value.kind === 'provider_default') {
    throw badPolicy(
      'Claude does not support provider_default permission intent'
    )
  }
  if (value.kind === 'explicit') {
    ensureExactKeys(value, ['kind', 'mode'], 'permissionIntent')
    if (
      typeof value.mode !== 'string' ||
      !PERMISSION_MODES.has(value.mode as ClaudePermissionMode)
    ) {
      throw badPolicy('permissionIntent explicit mode is invalid')
    }
    return {
      kind: 'explicit',
      mode: value.mode as ClaudePermissionMode,
    }
  }
  throw badPolicy('permissionIntent kind is invalid')
}

function parseClaudeSettingSourcesIntent(
  value: unknown
): ClaudeSettingSourcesIntent {
  if (!isRecord(value)) {
    throw badPolicy('settingSourcesIntent must be a tagged object')
  }
  if (value.kind === 'standard') {
    ensureExactKeys(value, ['kind'], 'settingSourcesIntent')
    return { kind: 'standard' }
  }
  if (value.kind === 'isolated') {
    ensureExactKeys(value, ['kind'], 'settingSourcesIntent')
    return { kind: 'isolated' }
  }
  if (value.kind === 'explicit') {
    ensureExactKeys(value, ['kind', 'sources'], 'settingSourcesIntent')
    if (!Array.isArray(value.sources)) {
      throw badPolicy('settingSourcesIntent explicit sources must be an array')
    }
    const sources = value.sources.map(source => {
      if (
        typeof source !== 'string' ||
        !SETTING_SOURCES.has(source as ClaudeSettingSource)
      ) {
        throw badPolicy('settingSourcesIntent contains an invalid source')
      }
      return source as ClaudeSettingSource
    })
    return { kind: 'explicit', sources }
  }
  throw badPolicy('settingSourcesIntent kind is invalid')
}

function validatePermissionIntent(intent: ClaudePermissionIntent): void {
  if (intent.kind === 'inherit_provider_configuration') {
    return
  }
  if (
    intent.kind !== 'explicit' ||
    !PERMISSION_MODES.has(intent.mode as ClaudePermissionMode)
  ) {
    throw badPolicy('Claude permission intent is invalid')
  }
}

function validateClaudeSettingSourcesIntent(
  intent: ClaudeSettingSourcesIntent,
  cwd?: string
): void {
  if (intent.kind === 'standard' || intent.kind === 'isolated') {
    return
  }
  if (intent.kind !== 'explicit' || !Array.isArray(intent.sources)) {
    throw badPolicy('Claude setting sources intent is invalid')
  }
  if (intent.sources.length === 0) {
    throw badPolicy(
      'Explicit setting sources must be non-empty; use isolated for none'
    )
  }

  let previousOrder = -1
  const seen = new Set<ClaudeSettingSource>()
  for (const source of intent.sources) {
    if (!SETTING_SOURCES.has(source)) {
      throw badPolicy(`Unknown Claude setting source: ${String(source)}`)
    }
    if (seen.has(source)) {
      throw badPolicy(`Duplicate Claude setting source: ${source}`)
    }
    seen.add(source)
    const order = CANONICAL_SOURCE_ORDER.get(source)
    if (order === undefined || order <= previousOrder) {
      throw badPolicy(
        'Explicit Claude setting sources must use user, project, local ordering'
      )
    }
    previousOrder = order
    if (
      !hasWorkingDirectory(cwd) &&
      (source === 'project' || source === 'local')
    ) {
      throw badPolicy(`Claude setting source ${source} requires a cwd`)
    }
  }
}

function rejectDirectSdkPolicyFields(params: Record<string, unknown>): void {
  const forbidden = FORBIDDEN_SDK_POLICY_FIELDS.find(field => field in params)
  if (forbidden) {
    throw badPolicy(
      `Bridge requests must use typed policy intent; ${forbidden} is not accepted`
    )
  }
}

function ensureExactKeys(
  value: Record<string, unknown>,
  keys: string[],
  name: string
): void {
  const expected = new Set(keys)
  if (Object.keys(value).some(key => !expected.has(key))) {
    throw badPolicy(`${name} contains unsupported fields`)
  }
}

function hasWorkingDirectory(cwd?: string): boolean {
  return typeof cwd === 'string' && cwd.trim().length > 0
}

function badPolicy(message: string): BridgeError {
  return new BridgeError('BAD_REQUEST', message, {
    code: 'invalid_claude_session_policy',
  })
}
