import { describe, expect, test } from 'bun:test'
import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

function bunCompileTarget(): string {
  if (process.platform === 'darwin' && process.arch === 'arm64') {
    return 'bun-darwin-arm64'
  }
  if (process.platform === 'darwin' && process.arch === 'x64') {
    return 'bun-darwin-x64'
  }
  if (process.platform === 'linux' && process.arch === 'x64') {
    return 'bun-linux-x64-baseline'
  }
  if (process.platform === 'linux' && process.arch === 'arm64') {
    return 'bun-linux-arm64'
  }
  if (process.platform === 'win32' && process.arch === 'x64') {
    return 'bun-windows-x64-baseline'
  }
  if (process.platform === 'win32' && process.arch === 'arm64') {
    return 'bun-windows-arm64'
  }

  throw new Error(
    `Unsupported platform/arch for compile smoke test: ${process.platform}/${process.arch}`
  )
}

describe('compiled claude bridge SDK import', () => {
  test('compiled binary prewarms sdk without module resolution errors', () => {
    const sidecarRoot = dirname(dirname(fileURLToPath(import.meta.url)))
    const tempDir = mkdtempSync(join(tmpdir(), 'gg-claude-bridge-compiled-'))
    const executableName =
      process.platform === 'win32' ? 'claude-bridge.exe' : 'claude-bridge'
    const outputBinary = join(tempDir, executableName)
    const sdkDependencyPath = join(
      sidecarRoot,
      'node_modules',
      '@anthropic-ai',
      'claude-agent-sdk'
    )

    try {
      if (!existsSync(sdkDependencyPath)) {
        const install = spawnSync('bun', ['install', '--frozen-lockfile'], {
          cwd: sidecarRoot,
          encoding: 'utf8',
        })
        expect(install.status).toBe(0)
      }

      const compile = spawnSync(
        'bun',
        [
          'build',
          'src/main.ts',
          '--compile',
          '--target',
          bunCompileTarget(),
          '--outfile',
          outputBinary,
        ],
        {
          cwd: sidecarRoot,
          encoding: 'utf8',
        }
      )

      const compileStdout = compile.stdout ?? ''
      const compileStderr = compile.stderr ?? ''
      expect(compile.status).toBe(0)
      expect(compileStderr.toLowerCase()).not.toContain('error:')
      expect(compileStdout.toLowerCase()).toContain('compile')

      const input = [
        JSON.stringify({
          id: 'req_ping',
          method: 'bridge.ping',
          params: {},
        }),
        JSON.stringify({
          id: 'req_shutdown',
          method: 'bridge.shutdown',
          params: {},
        }),
      ].join('\n')

      const run = spawnSync(outputBinary, [], {
        cwd: sidecarRoot,
        encoding: 'utf8',
        input: `${input}\n`,
        env: {
          ...process.env,
          GG_CLAUDE_BRIDGE_MODE: 'sdk',
          GG_CLAUDE_BRIDGE_PREWARM: '1',
        },
      })

      const stdout = run.stdout ?? ''
      const stderr = run.stderr ?? ''
      expect(run.status).toBe(0)
      expect(stderr).not.toContain(
        'Failed to load @anthropic-ai/claude-agent-sdk'
      )
      expect(stdout).toContain('"id":"req_ping"')
      expect(stdout).toContain('"id":"req_shutdown"')
    } finally {
      rmSync(tempDir, { recursive: true, force: true })
    }
  }, 60_000)
})
