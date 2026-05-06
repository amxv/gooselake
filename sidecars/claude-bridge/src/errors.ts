import type {
  BridgeErrorCode,
  BridgeErrorResponse,
  BridgeErrorShape,
  BridgeSuccessResponse,
} from './protocol'

export class BridgeError extends Error {
  readonly code: BridgeErrorCode
  readonly details: unknown

  constructor(code: BridgeErrorCode, message: string, details: unknown = null) {
    super(message)
    this.name = 'BridgeError'
    this.code = code
    this.details = details
  }
}

export function successResponse(
  id: string,
  result: Record<string, unknown>
): BridgeSuccessResponse {
  return { id, result }
}

export function errorResponse(
  id: string,
  error: BridgeError
): BridgeErrorResponse {
  return {
    id,
    error: toErrorShape(error),
  }
}

export function toErrorShape(error: BridgeError): BridgeErrorShape {
  return {
    code: error.code,
    message: error.message,
    details: error.details,
  }
}

export function ensureString(
  value: unknown,
  field: string,
  errorCode: BridgeErrorCode = 'BAD_REQUEST'
): string {
  if (typeof value === 'string' && value.length > 0) {
    return value
  }
  throw new BridgeError(errorCode, `Invalid or missing string field: ${field}`)
}

export function asOptionalString(value: unknown): string | undefined {
  if (typeof value === 'string' && value.length > 0) {
    return value
  }
  return undefined
}
