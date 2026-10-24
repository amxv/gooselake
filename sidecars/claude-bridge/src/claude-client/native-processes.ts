import { spawn, type ChildProcess } from 'node:child_process'
import type { SpawnOptions } from '@anthropic-ai/claude-agent-sdk'

import { BridgeError } from '../errors'
import type { SessionState } from './types'

type SpawnNativeProcess = (options: SpawnOptions) => ChildProcess
type NativeChild = { process: ChildProcess; exited: Promise<void> }

function spawnLocal(options: SpawnOptions): ChildProcess {
  const child = spawn(options.command, options.args, {
    cwd: options.cwd,
    env: options.env,
    signal: options.signal,
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
  })
  // The SDK custom-spawn interface owns stdin/stdout. Keep its diagnostic
  // stream separate from the bridge JSON protocol and continuously drained.
  child.stderr.on('data', chunk => process.stderr.write(chunk))
  return child
}

export class SessionNativeProcesses {
  private sealed = false
  private readonly children = new Set<NativeChild>()

  constructor(private readonly spawnProcess: SpawnNativeProcess = spawnLocal) {}

  readonly spawn = (options: SpawnOptions): ChildProcess => {
    if (this.sealed) {
      throw new BridgeError('TURN_IN_PROGRESS', 'Native session is sealed for retirement')
    }
    const child = this.spawnProcess(options)
    let observeExit!: () => void
    const record: NativeChild = {
      process: child,
      exited: new Promise(resolve => { observeExit = resolve }),
    }
    this.children.add(record)
    const complete = () => {
      this.children.delete(record)
      observeExit()
    }
    child.once('exit', complete)
    child.once('error', () => {
      // A spawn error without a pid proves no child was created. Errors from
      // a started child never substitute for its actual exit notification.
      if (child.pid === undefined) complete()
    })
    return child
  }

  async closeVerified(timeoutMs = 5000): Promise<void> {
    // Seal before yielding so a delayed SDK spawn cannot escape this receipt.
    this.sealed = true
    const children = [...this.children]
    for (const child of children) {
      try {
        child.process.kill('SIGKILL')
      } catch (error) {
        throw new BridgeError('INTERNAL_ERROR', `Could not terminate native session child: ${String(error)}`)
      }
    }
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      await Promise.race([
        Promise.all(children.map(child => child.exited)),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new BridgeError('TIMEOUT',
            'Native session child exit was not observed; retirement needs reconciliation')), timeoutMs)
        }),
      ])
    } finally {
      if (timer !== undefined) clearTimeout(timer)
    }
  }
}

const sessionProcesses = new WeakMap<object, SessionNativeProcesses>()

export function nativeProcessesFor(session: SessionState): SessionNativeProcesses {
  // Cwd verification queries copy SessionState with temporary options. Their
  // children remain owned by the same attachment through this shared token.
  let processes = sessionProcesses.get(session.nativeProcessScope)
  if (!processes) {
    processes = new SessionNativeProcesses()
    sessionProcesses.set(session.nativeProcessScope, processes)
  }
  return processes
}
