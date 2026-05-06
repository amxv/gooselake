import { describe, expect, test } from 'bun:test'

import { parseBridgeRequest, PROTOCOL_VERSION } from '../src/protocol'

describe('protocol parsing', () => {
  test('parses a valid bridge request', () => {
    const parsed = parseBridgeRequest(
      JSON.stringify({
        id: 'req_1',
        method: 'bridge.ping',
        params: {},
      })
    )

    expect(parsed).not.toBeNull()
    expect(parsed?.id).toBe('req_1')
    expect(parsed?.method).toBe('bridge.ping')
  })

  test('rejects invalid payload shape', () => {
    const parsed = parseBridgeRequest(
      JSON.stringify({
        id: 123,
        method: 'bridge.ping',
        params: {},
      })
    )
    expect(parsed).toBeNull()
  })

  test('rejects non-json payloads', () => {
    const parsed = parseBridgeRequest('not-json')
    expect(parsed).toBeNull()
  })

  test('exports protocol version', () => {
    expect(PROTOCOL_VERSION).toBe('0.1.0')
  })

  test('parses supported-models request method', () => {
    const parsed = parseBridgeRequest(
      JSON.stringify({
        id: 'req_models',
        method: 'session.supported_models',
        params: {},
      })
    )
    expect(parsed?.method).toBe('session.supported_models')
  })
})
