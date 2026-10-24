import { expect, test } from 'bun:test'
import { EventEmitter } from 'node:events'
import type { ChildProcess } from 'node:child_process'
import type { SpawnOptions } from '@anthropic-ai/claude-agent-sdk'

import { SessionNativeProcesses } from '../src/claude-client/native-processes'

const options: SpawnOptions = {
  command: 'test-native-process', args: [], env: {}, signal: new AbortController().signal,
}

test('a successful kill request cannot replace the native child exit event', async () => {
  let kills = 0
  const child = Object.assign(new EventEmitter(), {
    pid: 42,
    killed: false,
    kill() { kills++; this.killed = true; return true },
  })
  const processes = new SessionNativeProcesses(() => child as unknown as ChildProcess)
  processes.spawn(options)
  await expect(processes.closeVerified(10)).rejects.toMatchObject({ code: 'TIMEOUT' })
  expect(kills).toBe(1)
  expect(() => processes.spawn(options)).toThrow('sealed')
  child.emit('exit', null, 'SIGKILL')
  await expect(processes.closeVerified(10)).resolves.toBeUndefined()
})

test('native kill failures preserve a sealed attachment until actual exit is observed', async () => {
  const child = Object.assign(new EventEmitter(), {
    pid: 42,
    kill() { throw new Error('permission denied') },
  })
  const processes = new SessionNativeProcesses(() => child as unknown as ChildProcess)
  processes.spawn(options)
  await expect(processes.closeVerified(10)).rejects.toThrow('permission denied')
  expect(() => processes.spawn(options)).toThrow('sealed')
  child.emit('exit', 0, null)
  await expect(processes.closeVerified(10)).resolves.toBeUndefined()
})

test('an error from a started child is not proof that it stopped', async () => {
  const child = Object.assign(new EventEmitter(), { pid: 42, kill: () => true })
  const processes = new SessionNativeProcesses(() => child as unknown as ChildProcess)
  processes.spawn(options)
  child.emit('error', new Error('transport failed'))
  await expect(processes.closeVerified(10)).rejects.toMatchObject({ code: 'TIMEOUT' })
  child.emit('exit', 0, null)
  await processes.closeVerified(10)
})

test('the SDK abort signal reaches its tracked native child', async () => {
  const controller = new AbortController()
  const processes = new SessionNativeProcesses()
  const child = processes.spawn({
    command: process.execPath, args: ['-e', 'setInterval(() => {}, 1000)'],
    cwd: '/tmp', env: process.env, signal: controller.signal,
  })
  const exited = new Promise<void>(resolve => child.once('exit', () => resolve()))
  try {
    controller.abort()
    await exited
    expect(child.signalCode).toBe('SIGTERM')
    await processes.closeVerified(100)
  } finally {
    await processes.closeVerified(1000)
  }
})
