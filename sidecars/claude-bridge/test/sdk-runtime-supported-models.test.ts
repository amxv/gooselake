import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test'
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

describe('sdk runtime supported model discovery', () => {
  let tempDir: string
  let fakeClaudeExecutablePath: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'gg-sdk-runtime-'))
    fakeClaudeExecutablePath = join(tempDir, 'claude')
    writeFileSync(fakeClaudeExecutablePath, '#!/bin/sh\nexit 0\n', 'utf8')
    chmodSync(fakeClaudeExecutablePath, 0o755)
    process.env.GG_CLAUDE_CODE_EXECUTABLE = fakeClaudeExecutablePath
  })

  afterEach(async () => {
    delete process.env.GG_CLAUDE_CODE_EXECUTABLE
    const sdkRuntime = await import('../src/claude-client/sdk-runtime')
    sdkRuntime.resetSdkRuntimeCachesForTests()
    mock.restore()
    rmSync(tempDir, { recursive: true, force: true })
  })

  test('fallback probe passes resolved Claude executable path to query options', async () => {
    const queryCalls: Array<Record<string, unknown>> = []
    mock.module('@anthropic-ai/claude-agent-sdk', () => ({
      query: (params: { options: Record<string, unknown> }) => {
        queryCalls.push(params.options)
        return {
          supportedModels: async () => [{ value: 'claude-sonnet-4-6' }],
          interrupt: async () => undefined,
        }
      },
      getSessionMessages: async () => [],
      Query: undefined,
      supportedModels: undefined,
    }))

    const sdkRuntime = await import('../src/claude-client/sdk-runtime')
    sdkRuntime.resetSdkRuntimeCachesForTests()

    const supportedModels = await sdkRuntime.getSdkSupportedModels()
    expect(supportedModels).toEqual([{ value: 'claude-sonnet-4-6' }])
    expect(queryCalls).toHaveLength(1)
    expect(queryCalls[0]?.pathToClaudeCodeExecutable).toBe(
      fakeClaudeExecutablePath
    )
  })
})
