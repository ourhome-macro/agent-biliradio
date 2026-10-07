<template>
  <div class="feed-page">
    <header class="feed-header">
      <div><h1>音乐视频</h1><p>从一段现场、一次翻唱，找到此刻想听的声音。</p></div>
      <div class="feed-tabs"><FeedHistory @select="openContent" />
        <button :class="{ selected: feed.mode === 'personal' }" @click="changeMode('personal')">为你推荐</button>
        <button :class="{ selected: feed.mode === 'new' }" @click="changeMode('new')">新发现</button>
        <button :class="{ selected: feed.mode === 'following' }" @click="changeMode('following')">关注</button>
      </div>
    </header>
    <form class="feed-condition" @submit.prevent="refresh">
      <input v-model="feed.requestText" maxlength="2000" placeholder="想看什么？例如日语摇滚，不要 Rap" aria-label="当前视频偏好" />
      <button :disabled="feed.loading">换一组</button>
    </form>
    <div v-if="feed.mode === 'following'" class="following-context">
      <FollowingSync @updated="refresh" />
      <span v-if="followingHint" role="status">{{ followingHint }}</span>
      <button v-if="feed.followingState?.hasUpdates" @click="refresh">关注已变化，更新列表</button>
    </div>
    <p v-if="feed.error" class="feed-error" role="alert">{{ feed.error }}</p>
    <div v-if="!feed.items.length" class="feed-empty" role="status">
      {{ feed.loading ? '正在准备推荐…' : followingHint || (feed.supplyState === 'replenishing' ? '正在发现新的音乐视频…' : '暂时没有合适的视频，试试调整条件。') }}
      <button v-if="!feed.loading" @click="refresh">刷新</button>
    </div>
    <div v-else ref="scroller" class="feed-scroller" @scroll.passive="onScroll">
      <div :style="{ height: `${rangeStart * viewportHeight}px` }" aria-hidden="true" />
      <article v-for="entry in windowItems" :key="entry.item.itemId" :data-feed-item="entry.item.itemId" class="feed-slide" :style="{ height: `${viewportHeight}px` }">
        <div v-if="entry.item.suppressed" class="feed-empty">这条内容已不可用</div>
        <div v-else class="feed-card">
          <video v-if="entry.index === activeIndex" ref="video" :muted="muted" playsinline preload="none"
            :poster="mediaUrl(entry.item.coverUrl || entry.item.track?.cover)" @playing="onPlaying" @pause="onPause"
            @waiting="onWaiting" @ended="onEnded" @timeupdate="onTime" @seeking="onPause" @seeked="onSeeked" @loadedmetadata="onTime" />
          <img v-else :src="mediaUrl(entry.item.coverUrl || entry.item.track?.cover)" :alt="entry.item.track?.title" loading="lazy" />
          <div class="feed-caption"><strong>{{ entry.item.track?.title }}</strong><span>{{ entry.item.track?.owner }}</span></div>
          <div v-if="entry.index === activeIndex" class="feed-controls">
            <span role="status">{{ status }}</span>
            <button @click="togglePlay">{{ playing ? '暂停' : '播放' }}</button>
            <button @click="muted = !muted">{{ muted ? '开启声音' : '静音' }}</button>
            <button v-if="!playing && status.includes('重试')" @click="begin(entry.item)">重试</button>
            <button @click="next()">下一个 ↓</button><button @click="fullscreen">全屏</button>
            <label class="feed-progress"><span>{{ clock(position) }} / {{ clock(duration) }}</span><input type="range" min="0" :max="duration || 1" step="0.1" :value="position" :disabled="!canSeek" aria-label="视频进度" @change="seek" /><small v-if="!canSeek">保存完成后可拖动进度</small></label>
          </div>
        </div>
      </article>
      <div :style="{ height: `${Math.max(0, feed.items.length - rangeEnd) * viewportHeight}px` }" aria-hidden="true" />
      <div class="feed-footer">{{ feed.loading ? '正在加载…' : feed.hasMore ? '' : '这一组已经看完' }}<button v-if="feed.error" @click="feed.more">重试加载</button></div>
    </div>
    <button v-if="feed.supplyPaused" @click="feed.retrySupply">继续查找视频</button>
    <FeedActions :item="active" @dismiss="dismiss" @restore="restore" @status="status = $event" />
    <FollowAuthor :item="active" @changed="followChanged" />
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useFeedStore } from '@/stores/feedStore'
import { feedApi, type FeedEvent, type FeedItem, type Playback, type FeedMode } from '@/api/feed'
import type { CreatorFollow } from '@/api/following'
import { fetchSettings, mediaUrl } from '@/api/client'
import { VideoPlayerAdapter } from '@/video/VideoPlayerAdapter'
import { FeedEventQueue } from '@/video/FeedEventQueue'
import { claimVideoPlayback } from '@/media/PlaybackCoordinator'
import { usePlayerStore } from '@/stores/playerStore'
import FeedActions from '@/components/feed/FeedActions.vue'
import FeedHistory from '@/components/feed/FeedHistory.vue'
import FollowAuthor from '@/components/feed/FollowAuthor.vue'
import FollowingSync from '@/components/feed/FollowingSync.vue'

const feed = useFeedStore(), route = useRoute()
const scroller = ref<HTMLDivElement | null>(null)
const video = ref<HTMLVideoElement[] | HTMLVideoElement | null>(null)
const activeIndex = ref(0), viewportHeight = ref(600), muted = ref(true), playing = ref(false)
const status = ref(''), position = ref(0), duration = ref(0), canSeek = ref(false)
const rangeStart = computed(() => Math.max(0, activeIndex.value - 1))
const rangeEnd = computed(() => Math.min(feed.items.length, activeIndex.value + 2))
const windowItems = computed(() => feed.items.slice(rangeStart.value, rangeEnd.value).map((item, n) => ({ item, index: rangeStart.value + n })))
const active = computed(() => feed.items[activeIndex.value])
const followingHint = computed(() => feed.mode === 'following' ? ({ no_follows: '还没有关注的作者，可先同步 B 站关注或在视频下方关注作者。', syncing: '正在同步关注作者的音乐视频…', no_music: '关注作者暂时没有符合条件的音乐视频。', account_changed: 'B 站账号已变化，请重新同步关注列表。', login_required: '绑定 B 站账号后，可浏览关注作者的视频。', upstream_error: '关注作者的视频暂时获取失败，已加载的视频仍可观看，稍后可重试。', available: '' }[feed.followingState?.reason || 'available']) : '')
const adapter = new VideoPlayerAdapter(), events = new FeedEventQueue()
let generation = 0, playback: Playback | null = null, currentItem: FeedItem | undefined
let heartbeat: number | undefined, exposureTimer: number | undefined, heartbeatFailures = 0
let signedRefreshTimer: number | undefined, signedRefreshAt = 0
let releaseOwner: (() => void) | null = null, watchMs = 0, lastTime = 0, lastMediaTime = 0, started = false
let observer: ResizeObserver | null = null, exposureSince = 0, exposureSent = false
let attachedKey = '', attachedMode = '', signedUntil = 0, recoveryCount = 0, recovering = false
let bufferingSince = 0, bufferingMs = 0, beginAt = 0, startupMs = 0, pendingNext = false, mounted = false
const element = () => Array.isArray(video.value) ? video.value[0] : video.value
const delay = (ms: number) => new Promise(resolve => window.setTimeout(resolve, ms))
const clock = (seconds: number) => `${Math.floor(seconds / 60)}:${String(Math.floor(seconds % 60)).padStart(2, '0')}`
function emit(type: string, extra: Partial<FeedEvent> = {}) {
  if (!currentItem || currentItem.suppressed && type === 'impression') return
  events.enqueue({ eventId: crypto.randomUUID(), itemId: currentItem.itemId, type,
    ...(playback ? { playbackId: playback.playbackId } : {}), ...extra })
}
function flushWatch() {
  onTime()
  if (started) emit('watch_progress', { watchMs: Math.round(watchMs), bufferingMs: Math.round(bufferingMs), startupMs: Math.round(startupMs) })
}
function stop(reason = 'interrupt') {
  if (playback) { flushWatch(); if (started && reason !== 'complete') emit(reason) }
  generation += 1; window.clearTimeout(heartbeat); window.clearInterval(exposureTimer); window.clearTimeout(signedRefreshTimer)
  playing.value = false; lastTime = 0; adapter.stop()
  if (playback) { const id = playback.playbackId; void events.flush().finally(() => feedApi.release(id).catch(() => undefined)); playback = null }
  releaseOwner?.(); releaseOwner = null; currentItem = undefined; recovering = false
}
function checkExposure() {
  if (exposureSent || !currentItem || document.hidden || !scroller.value) { exposureSince = 0; return }
  const card = scroller.value.querySelector(`[data-feed-item="${currentItem.itemId}"]`)
  if (!card) { exposureSince = 0; return }
  const rect = card.getBoundingClientRect(), viewport = scroller.value.getBoundingClientRect()
  const height = Math.max(0, Math.min(rect.bottom, viewport.bottom, innerHeight) - Math.max(rect.top, viewport.top, 0))
  const width = Math.max(0, Math.min(rect.right, viewport.right, innerWidth) - Math.max(rect.left, viewport.left, 0))
  const ratio = rect.width * rect.height > 0 ? height * width / (rect.width * rect.height) : 0
  if (ratio < .5) { exposureSince = 0; return }
  const now = performance.now()
  if (!exposureSince) exposureSince = now
  if (now - exposureSince >= 1000) { exposureSent = true; emit('impression', { visibleRatio: Math.min(1, ratio), visibleMs: Math.floor(now - exposureSince) }) }
}
async function bounded<T>(request: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController(), timer = window.setTimeout(() => controller.abort(), 3500)
  try { return await request(controller.signal) } finally { clearTimeout(timer) }
}
function generationKey(value: Playback) { return value.generationKey || `${value.assetId}:${value.generation}` }
async function attach(value: Playback, version: number, preserve = true) {
  if (!value.manifestUrl) throw new Error('unavailable')
  await nextTick()
  const target = element()
  if (!target || version !== generation) return
  const oldPosition = preserve ? target.currentTime || position.value : 0
  const autoplay = !preserve || !started || !target.paused
  attachedKey = generationKey(value); attachedMode = value.mode
  signedUntil = value.manifestExpiresAt || 0
  signedRefreshAt = signedUntil ? Date.now() / 1000 + (value.refreshAfterSeconds || Math.max(1, signedUntil - Date.now() / 1000 - 60)) : 0
  window.clearTimeout(signedRefreshTimer)
  if (signedRefreshAt) {
    const key = attachedKey
    signedRefreshTimer = window.setTimeout(async () => {
      if (version !== generation || !playback || key !== attachedKey) return
      try {
        const fresh = await bounded(signal => feedApi.playback(playback!.playbackId, signal))
        if (version !== generation || key !== attachedKey) return
        playback = fresh; await attach(fresh, version)
      } catch { if (version === generation) void recover(version, 'manifest_refresh_failed') }
    }, Math.max(250, (signedRefreshAt - Date.now() / 1000) * 1000))
  }
  canSeek.value = value.sourceComplete
  duration.value = value.durationSeconds || duration.value
  if (!releaseOwner) releaseOwner = claimVideoPlayback(() => stop(), () => usePlayerStore().pause())
  adapter.attach(target, value.manifestUrl,
    code => { if (version === generation) void recover(version, code) },
    () => { if (version === generation) { status.value = '点击播放开始观看'; emit('autoplay_blocked') } },
    { position: oldPosition, autoplay })
  status.value = value.mode === 'cold' ? '首次观看，正在保存' : '已准备好'
}
async function descriptorChanged(value: Playback, version: number) {
  if (!['ready', 'streaming', 'preparing'].includes(value.status)) throw new Error(value.errorType || value.status)
  if (value.status === 'preparing') return
  const refreshed = signedUntil > 0 && Date.now() / 1000 >= signedRefreshAt
  if (generationKey(value) !== attachedKey || value.mode !== attachedMode || refreshed) await attach(value, version, !!attachedKey)
  canSeek.value = value.sourceComplete
}
async function beat(version: number) {
  if (!playback || version !== generation) return
  try {
    const value = await bounded(signal => feedApi.heartbeat(playback!.playbackId, signal))
    if (version !== generation) return
    playback = value; heartbeatFailures = 0
    await descriptorChanged(value, version); flushWatch()
    heartbeat = window.setTimeout(() => void beat(version), 5000)
  } catch (error) {
    if (version !== generation || !playback) return
    heartbeatFailures += 1
    const code = (error as { status?: number }).status
    const lease = playback.leaseExpiresAt || playback.expiresAt
    if ([401, 403, 404, 409, 410].includes(code || 0) || Date.now() / 1000 > lease - 1 || heartbeatFailures >= 4) {
      status.value = '连接中断，点击重试'; stop(); return
    }
    status.value = '连接恢复中…'
    heartbeat = window.setTimeout(() => void beat(version), Math.min(1000 * 2 ** (heartbeatFailures - 1), 3000))
  }
}
async function recover(version: number, code: string) {
  if (recovering || version !== generation || !playback) return
  recovering = true; onPause(); emit('playback_error', { errorType: code })
  try {
    while (recoveryCount < 3 && version === generation && playback) {
      recoveryCount += 1; status.value = `正在恢复播放（${recoveryCount}/3）…`
      await delay(500 * 2 ** (recoveryCount - 1))
      if (version !== generation || !playback) return
      try {
        const value = await bounded(signal => feedApi.heartbeat(playback!.playbackId, signal))
        if (version !== generation) return
        playback = value
        if (['ready', 'streaming'].includes(value.status)) { await attach(value, version); return }
      } catch { /* Retry within the playback lease. */ }
    }
    if (version === generation) { status.value = '播放失败，请重试'; stop() }
  } finally { if (version === generation) recovering = false }
}
async function begin(item?: FeedItem) {
  stop(); watchMs = 0; lastTime = 0; lastMediaTime = 0; started = false; attachedKey = ''; attachedMode = ''
  position.value = 0; duration.value = 0; canSeek.value = false
  exposureSince = 0; exposureSent = false; heartbeatFailures = 0; recoveryCount = 0; bufferingMs = 0; bufferingSince = 0
  if (!item || item.suppressed || document.hidden) return
  currentItem = item; beginAt = performance.now(); startupMs = 0
  const version = generation
  exposureTimer = window.setInterval(checkExposure, 100)
  status.value = '正在准备视频…'
  const key = crypto.randomUUID()
  try {
    const value = await bounded(signal => feedApi.start(item.contentId, key, signal))
    if (version !== generation) { void feedApi.release(value.playbackId).catch(() => undefined); return }
    playback = value
    heartbeat = window.setTimeout(() => void beat(version), 5000)
    const deadline = performance.now() + 120000
    while (playback?.status === 'preparing') {
      if (performance.now() > deadline) throw new Error('preparation_timeout')
      await delay(700)
      if (version !== generation || !playback) return
      try {
        const fresh = await bounded(signal => feedApi.playback(playback!.playbackId, signal))
        if (version !== generation) return
        playback = fresh
      } catch (error) {
        if ([401, 403, 404, 410].includes((error as { status?: number }).status || 0)) throw error
        await delay(1000)
      }
    }
    if (version !== generation || !playback) return
    await descriptorChanged(playback, version)
  } catch (error) {
    if (version === generation) {
      const unavailable = [403, 404, 410, 422].includes((error as { status?: number }).status || 0)
      status.value = unavailable ? '视频不可用，已跳过' : '播放失败，请重试'
      stop()
      if (unavailable) { item.suppressed = true; void next(false) }
    }
  }
}
function onPlaying(event?: Event) {
  if (event && event.target !== element()) return
  if (bufferingSince) { bufferingMs += performance.now() - bufferingSince; bufferingSince = 0 }
  playing.value = true; lastTime = performance.now(); lastMediaTime = element()?.currentTime || 0
  if (!started) { started = true; startupMs = performance.now() - beginAt; emit('play_started', { watchMs: 0 }) }
}
function onPause(event?: Event) { if (event && event.target !== element()) return; onTime(event); playing.value = false; lastTime = 0 }
function onSeeked(event?: Event) { onTime(event); if (!element()?.paused) onPlaying(event) }
function onWaiting(event?: Event) { onPause(event); if (!bufferingSince) bufferingSince = performance.now() }
function onTime(event?: Event) {
  if (event && event.target !== element()) return
  const target = element(), now = performance.now()
  if (!target) return
  const mediaDelta = target.currentTime - lastMediaTime
  if (playing.value && !target.seeking && !document.hidden && lastTime && mediaDelta > 0)
    watchMs += Math.min(now - lastTime, mediaDelta * 1000 / Math.max(.25, target.playbackRate), 2000)
  lastTime = now; lastMediaTime = target.currentTime; position.value = target.currentTime || 0
  if (Number.isFinite(target.duration)) duration.value = target.duration
}
async function onEnded(event?: Event) {
  if (event && event.target !== element()) return
  onPause(event)
  const version = generation, current = playback
  if (!current) return
  try {
    const value = await bounded(signal => feedApi.playback(current.playbackId, signal))
    if (version !== generation) return
    flushWatch()
    if (value.sourceComplete) {
      const total = value.durationSeconds || duration.value
      // Seeking to the end is navigation, not evidence that the content was watched.
      if (total > 0 && watchMs >= total * 900) emit('complete', { watchMs: Math.round(watchMs) })
      stop('complete'); await next(false)
    }
    else { emit('interrupt'); await recover(version, 'incomplete_end') }
  } catch { if (version === generation) await recover(version, 'end_verification_failed') }
}
function togglePlay() { if (!playback) { void begin(active.value); return }; const target = element(); if (!target) return; if (target.paused) void target.play().catch(() => { status.value = '点击播放开始观看' }); else target.pause() }
function onScroll() {
  checkExposure()
  if (!scroller.value) return
  activeIndex.value = Math.max(0, Math.min(feed.items.length - 1, Math.round(scroller.value.scrollTop / viewportHeight.value)))
}
function goTo(index: number) { pendingNext = false; activeIndex.value = index; scroller.value?.scrollTo({ top: index * viewportHeight.value, behavior: 'auto' }) }
async function next(navigation = true) {
  if (navigation) { flushWatch(); emit('swipe_next'); stop() }
  let index = feed.items.findIndex((item, n) => n > activeIndex.value && !item.suppressed)
  if (index < 0) { pendingNext = true; await feed.more(); index = feed.items.findIndex((item, n) => n > activeIndex.value && !item.suppressed) }
  if (index >= 0) goTo(index)
  else { status.value = feed.supplyState === 'replenishing' ? '正在发现更多视频…' : '已经看完，试试换一组'; pendingNext = true }
}
function seek(event: Event) { const target = element(); if (!target || !canSeek.value) return; flushWatch(); target.currentTime = Number((event.target as HTMLInputElement).value); lastMediaTime = target.currentTime; lastTime = performance.now() }
async function fullscreen() {
  const target = element(), card = target?.closest('.feed-card') as HTMLElement | null
  try { if (document.fullscreenElement) await document.exitFullscreen(); else if (card?.requestFullscreen) await card.requestFullscreen(); else (target as HTMLVideoElement & { webkitEnterFullscreen?: () => void })?.webkitEnterFullscreen?.() }
  catch { status.value = '当前浏览器暂不支持全屏' }
}
async function refresh() { pendingNext = false; stop(); activeIndex.value = 0; if (scroller.value) scroller.value.scrollTop = 0; await feed.reset() }
async function changeMode(mode: FeedMode) { if (feed.mode !== mode) { feed.mode = mode; await refresh() } }
async function followChanged(value: CreatorFollow) {
  if (feed.mode !== 'following') return
  stop(); pendingNext = false; activeIndex.value = 0
  if (scroller.value) scroller.value.scrollTop = 0
  await feed.followingChanged(value.externalId, value.following)
}
async function openContent(contentId: string) { stop(); pendingNext = false; activeIndex.value = 0; await feed.openContent(contentId); await nextTick(); if (scroller.value) scroller.value.scrollTop = 0 }
function dismiss(itemId: string) { const item = feed.items.find(value => value.itemId === itemId); if (item) item.suppressed = true; if (active.value?.itemId === itemId) { stop(); void next(false) } }
function restore(itemId: string) { const item = feed.items.find(value => value.itemId === itemId); if (item) item.suppressed = false; if (active.value?.itemId === itemId) void begin(item) }
function visibility() { if (document.hidden) stop(); else { feed.resumeSupply(); void begin(active.value) } }
function keys(e: KeyboardEvent) { if ((e.target as HTMLElement)?.matches('input,textarea,select,button') || (e.target as HTMLElement)?.isContentEditable) return; if (e.key === 'ArrowDown') { e.preventDefault(); void next() } else if (e.key === 'ArrowUp') { e.preventDefault(); let index = activeIndex.value - 1; while (index >= 0 && feed.items[index].suppressed) index -= 1; if (index >= 0) goTo(index) } }
watch(() => active.value?.itemId, (_id, previous) => {
  if (previous && currentItem && currentItem.itemId !== active.value?.itemId) { flushWatch(); emit('swipe_next') }
  if (active.value?.suppressed) { void next(false); return }
  void begin(active.value)
  if (activeIndex.value >= feed.items.length - 3) void feed.more()
})
watch(() => feed.items.length, () => { if (pendingNext) { const index = feed.items.findIndex((item, n) => n > activeIndex.value && !item.suppressed); if (index >= 0) goTo(index) } })
watch(scroller, node => { observer?.disconnect(); if (node) { observer = new ResizeObserver(entries => { viewportHeight.value = Math.max(240, entries[0].contentRect.height); node.scrollTop = activeIndex.value * viewportHeight.value }); observer.observe(node); viewportHeight.value = node.clientHeight } })
watch(() => route.query.content, value => { if (mounted && typeof value === 'string' && value) void openContent(value) })
onMounted(async () => {
  if (!(await fetchSettings()).feedEnabled) { feed.error = '视频功能尚未开启'; return }
  mounted = true
  document.addEventListener('visibilitychange', visibility); window.addEventListener('keydown', keys); window.addEventListener('pagehide', pageHide)
  if (typeof route.query.content === 'string') await openContent(route.query.content); else await refresh()
})
function pageHide() { stop(); events.close() }
onBeforeUnmount(() => { mounted = false; stop(); events.close(); feed.stop(); observer?.disconnect(); document.removeEventListener('visibilitychange', visibility); window.removeEventListener('keydown', keys); window.removeEventListener('pagehide', pageHide) })
</script>

<style scoped>
.feed-page { height:100%; min-height:0; display:flex; flex-direction:column; padding:20px 24px; gap:12px; }
.feed-header { display:flex; align-items:center; justify-content:space-between; gap:16px; }
h1 { font-size:24px; margin:0 0 6px; }.feed-header p { margin:0; color:var(--color-text-secondary); font-size:13px; }
.feed-tabs,.feed-condition,.feed-controls { display:flex; gap:8px; align-items:center; }.feed-tabs { flex-wrap:wrap; }.following-context { display:flex; gap:8px; flex-wrap:wrap; align-items:center; font-size:12px; color:var(--color-text-secondary); }.feed-condition input { flex:1; min-width:0; padding:10px 14px; border:1px solid var(--color-border); border-radius:10px; background:var(--color-bg-app); color:inherit; }
button { border:1px solid var(--color-border); border-radius:9px; padding:8px 12px; color:inherit; background:var(--color-bg-sidebar); cursor:pointer; }button.selected { color:var(--color-accent); border-color:currentColor; }button:disabled { opacity:.5; cursor:default; }
.feed-scroller { flex:1; min-height:240px; overscroll-behavior:contain; touch-action:pan-y; overflow-y:auto; scroll-snap-type:y mandatory; position:relative; scrollbar-width:thin; }.feed-slide { scroll-snap-align:start; box-sizing:border-box; padding:0 0 12px; }
.feed-card { height:100%; position:relative; border-radius:16px; overflow:hidden; background:#101114; color:#fff; }.feed-card video,.feed-card>img { width:100%; height:100%; object-fit:contain; }
.feed-caption { position:absolute; bottom:120px; left:20px; right:20px; display:grid; gap:5px; text-shadow:0 1px 6px #000; pointer-events:none; }.feed-caption span { font-size:13px; opacity:.8; }.feed-controls { position:absolute; left:0; right:0; bottom:0; padding:14px 18px; background:linear-gradient(transparent,rgba(0,0,0,.85)); flex-wrap:wrap; }.feed-controls span { flex:1; font-size:12px; }.feed-controls button { background:rgba(30,30,35,.85); border-color:#555; }
.feed-empty { flex:1; display:flex; align-items:center; justify-content:center; gap:16px; color:var(--color-text-secondary); }.feed-error { margin:0; color:#c44; }.feed-footer { text-align:center; padding:8px; font-size:12px; }
@media(max-width:700px) { .feed-page { padding:12px; }.feed-header { align-items:flex-start; flex-direction:column; }.feed-header p { display:none; }.feed-controls { padding:10px; gap:5px; }.feed-controls button { padding:7px 8px; } }
.feed-progress { width:100%; display:flex; align-items:center; gap:8px; font-size:12px; }.feed-progress input { flex:1; min-width:40px; accent-color:var(--color-accent); }.feed-progress small { font-size:10px; opacity:.7; }.feed-card:fullscreen { border-radius:0; width:100vw; height:100dvh; }.feed-controls { padding-bottom:max(14px,env(safe-area-inset-bottom)); }
@media (orientation:landscape) and (max-height:600px) { .feed-header p,.feed-condition { display:none; }.feed-page { padding:4px 12px; gap:4px; }.feed-header h1 { font-size:16px; }.feed-header { flex-direction:row; }.feed-scroller { min-height:120px; } }
</style>
