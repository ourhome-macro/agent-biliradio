import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { build } from 'esbuild'

const source = await readFile(new URL('../src/stores/feedStore.ts', import.meta.url), 'utf8')
const bundle = await build({
  stdin: { contents: source + '\nexport { createPinia, setActivePinia } from "pinia"',
    loader: 'ts', resolveDir: new URL('..', import.meta.url).pathname.replace(/^\/(\w:)/, '$1') },
  bundle: true, write: false, format: 'esm', platform: 'node',
  plugins: [{ name: 'feed-api-fixture', setup(builder) {
    builder.onResolve({ filter: /^@\/api\// }, args => ({ path: args.path, namespace: 'fixture' }))
    builder.onLoad({ filter: /.*/, namespace: 'fixture' }, args => ({
      contents: args.path.endsWith('/feed')
        ? 'export const feedApi = globalThis.__feedMock'
        : 'export const apiRequest = (...args) => globalThis.__feedOpen(...args)',
    }))
  } }],
})
const mocks = globalThis.__feedMock = {}
const { useFeedStore, createPinia, setActivePinia } = await import(
  'data:text/javascript;base64,' + Buffer.from(bundle.outputFiles[0].text).toString('base64'))
const page = (ids = [], rest = {}) => ({ sessionId: 's1', revision: 'r1', mode: 'personal',
  items: ids.map(id => ({ contentId: id, itemId: 'item-' + id, suppressed: false })),
  nextCursor: 'c1', hasMore: true, supplyState: 'replenishing', expiresAt: Date.now() / 1000 + 1800, ...rest })
const tick = async () => { for (let i = 0; i < 12; i++) await Promise.resolve() }
function fixture(t) {
  setActivePinia(createPinia())
  const timers = []
  const previous = { setTimeout, clearTimeout, document: globalThis.document }
  globalThis.setTimeout = (fn, delay) => { const timer = { fn, delay }; timers.push(timer); return timer }
  globalThis.clearTimeout = timer => { if (timer) timer.cancelled = true }
  globalThis.document = { hidden: false }
  const store = useFeedStore()
  t.after(() => { store.stop(); Object.assign(globalThis, previous) })
  const fire = async () => {
    const timer = timers.find(entry => !entry.cancelled && !entry.fired)
    assert.ok(timer, 'a bounded poll is scheduled')
    timer.fired = true; timer.fn(); await tick(); return timer.delay
  }
  return { store, fire, timers }
}

test('empty supply automatically continues and stops polling when content arrives', async t => {
  const { store, fire } = fixture(t)
  mocks.create = async () => page()
  mocks.page = async () => page(['new'], { nextCursor: 'c2', supplyState: 'available' })
  await store.reset()
  assert.equal(store.supplyPolling, true)
  assert.equal(await fire(), 2000)
  assert.deepEqual(store.items.map(item => item.contentId), ['new'])
  assert.equal(store.supplyPolling, false)
})

test('empty tail polling is bounded and explicit retry reopens its budget', async t => {
  const { store, fire } = fixture(t)
  let calls = 0
  mocks.create = async () => page()
  mocks.page = async () => { calls++; return page([], { nextCursor: 'c' + calls }) }
  await store.reset()
  const delays = []
  for (let i = 0; i < 6; i++) delays.push(await fire())
  assert.deepEqual(delays, [2000, 4000, 8000, 12000, 15000, 15000])
  assert.equal(store.supplyPaused, true)
  await store.more()
  assert.equal(calls, 6)
  mocks.page = async () => page(['fresh'])
  await store.retrySupply()
  assert.equal(store.supplyPaused, false)
  assert.equal(store.items[0].contentId, 'fresh')
})

test('session expiry renews once, preserving the active item and deduplicating content', async t => {
  const { store } = fixture(t)
  let creates = 0
  mocks.create = async () => ++creates === 1 ? page(['a']) : page(['a', 'b'], { sessionId: 's2' })
  mocks.page = async () => { throw Object.assign(new Error('expired'), { status: 410 }) }
  await store.reset()
  const first = store.items[0]
  await store.more()
  assert.equal(store.renewalCount, 1)
  assert.equal(store.sessionId, 's2')
  assert.equal(store.items[0], first)
  assert.deepEqual(store.items.map(item => item.contentId), ['a', 'b'])
})

test('renewal keeps submitted conditions when the input contains an unsubmitted draft', async t => {
  const { store } = fixture(t)
  const requests = []
  mocks.create = async (mode, text) => { requests.push({ mode, text }); return page(['a']) }
  mocks.page = async () => { throw Object.assign(new Error('expired'), { status: 410 }) }
  store.requestText = 'rock'
  await store.reset()
  store.requestText = 'jazz draft'
  await store.more()
  assert.deepEqual(requests.map(request => request.text), ['rock', 'rock'])
})

test('reset rejects stale page responses even when transport ignores abort', async t => {
  const { store } = fixture(t)
  let resolveOld
  mocks.create = async () => page(['a'])
  mocks.page = () => new Promise(resolve => { resolveOld = resolve })
  await store.reset()
  const stale = store.more()
  mocks.create = async () => page(['b'], { sessionId: 'fresh' })
  await store.reset()
  resolveOld(page(['old']))
  await stale
  assert.deepEqual(store.items.map(item => item.contentId), ['b'])
  assert.equal(store.sessionId, 'fresh')
})

test('hidden tabs pause supply and auth errors never auto retry', async t => {
  const { store, fire } = fixture(t)
  mocks.create = async () => page()
  mocks.page = async () => { throw Object.assign(new Error('unauthorized'), { status: 401 }) }
  await store.reset()
  document.hidden = true; store.resumeSupply()
  assert.equal(store.supplyPolling, false)
  document.hidden = false; store.resumeSupply()
  await fire()
  assert.equal(store.supplyPolling, false)
  assert.equal(store.error, 'unauthorized')
})

test('history content opens as an authorized singleton session', async t => {
  const { store } = fixture(t)
  let opened
  globalThis.__feedOpen = async path => { opened = path; return page(['history'], { hasMore: false }) }
  await store.openContent('history')
  assert.equal(opened, '/api/feed/content/history/open')
  assert.equal(store.hasMore, false)
  assert.equal(store.items[0].contentId, 'history')
})

test('unfollow starts a new following revision and discards an in-flight old page', async t => {
  const { store } = fixture(t)
  let resolveOld, creations = 0
  const requests = []
  store.mode = 'following'; store.requestText = 'rock'
  mocks.create = async (mode, text) => {
    requests.push({ mode, text })
    return ++creations === 1
      ? page(['old'], { mode, revision: 'following-1', followingState: { reason: 'available', snapshotVersion: 1 } })
      : page(['new'], { mode, revision: 'following-2', followingState: { reason: 'available', snapshotVersion: 2 } })
  }
  mocks.page = () => new Promise(resolve => { resolveOld = resolve })
  await store.reset()
  store.items[0].track = { ownerMid: 123 }
  const oldItem = store.items[0]
  const stale = store.more()
  store.requestText = 'unsaved jazz'
  await store.followingChanged('123', false)
  assert.equal(oldItem.suppressed, true)
  resolveOld(page(['unfollowed-author']))
  await stale
  assert.deepEqual(store.items.map(item => item.contentId), ['new'])
  assert.equal(store.revision, 'following-2')
  assert.equal(store.followingState.snapshotVersion, 2)
  assert.deepEqual(requests, [{ mode: 'following', text: 'rock' }, { mode: 'following', text: 'rock' }])
})

test('follow changes preserve personal feed playback identity', async t => {
  const { store } = fixture(t)
  mocks.create = async () => page(['a'])
  await store.reset()
  const previous = store.items[0], version = store.generation
  await store.followingChanged('123', false)
  assert.equal(store.items[0], previous)
  assert.equal(store.items[0].suppressed, false)
  assert.equal(store.generation, version)
})
