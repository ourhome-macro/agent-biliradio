import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { build } from 'esbuild'

const source = await readFile(new URL('../src/video/FeedEventQueue.ts', import.meta.url), 'utf8')
const bundle = await build({ stdin: { contents: source, loader: 'ts' }, bundle: true, write: false, format: 'esm', platform: 'node',
  plugins: [{ name: 'api', setup(builder) {
    builder.onResolve({ filter: /^@\/api\/feed$/ }, () => ({ path: 'feed', namespace: 'mock' }))
    builder.onLoad({ filter: /.*/, namespace: 'mock' }, () => ({ contents: 'export const feedApi = globalThis.__queueApi' }))
  } }],
})
const api = globalThis.__queueApi = {}
const { FeedEventQueue } = await import('data:text/javascript;base64,' + Buffer.from(bundle.outputFiles[0].text).toString('base64'))
const tick = async () => { for (let i = 0; i < 12; i++) await Promise.resolve() }

test('atomic rejected mixed batch retains valid final-watch records on both sides', async t => {
  const timers = [], originals = { setTimeout, clearTimeout }
  globalThis.setTimeout = (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer }
  globalThis.clearTimeout = timer => { if (timer) timer.cancelled = true }
  t.after(() => Object.assign(globalThis, originals))
  let releaseFirst
  const gate = new Promise(resolve => { releaseFirst = resolve })
  const accepted = [], attempts = []
  api.events = async batch => {
    attempts.push(batch.map(event => event.eventId))
    if (batch[0].eventId === 'initial') await gate
    if (batch.some(event => event.eventId === 'bad')) throw Object.assign(new Error('invalid'), { status: 400 })
    accepted.push(...batch.map(event => event.eventId))
  }
  const queue = new FeedEventQueue()
  for (const eventId of ['initial', 'valid-before', 'bad', 'valid-after'])
    queue.enqueue({ eventId, itemId: 'item', type: 'watch_progress', playbackId: 'playback', watchMs: 100 })
  releaseFirst(); await tick()
  for (let count = 0; queue.pending && count < 12; count++) {
    const timer = timers.find(timer => !timer.cancelled && !timer.fired && timer.delay === 0)
    assert.ok(timer); timer.fired = true; timer.fn(); await tick()
  }
  assert.deepEqual(accepted, ['initial', 'valid-before', 'valid-after'])
  assert.ok(attempts.some(ids => ids.join(',') === 'valid-before,bad,valid-after'))
  assert.equal(queue.pending, 0)
})
