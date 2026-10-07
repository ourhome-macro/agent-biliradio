<template>
  <div class="feed-page">
    <header class="feed-header">
      <div><h1>音乐视频</h1><p>从一段现场、一次翻唱，找到此刻想听的声音。</p></div>
      <div class="feed-tabs">
        <button :class="{ selected: feed.mode === 'personal' }" @click="changeMode('personal')">为你推荐</button>
        <button :class="{ selected: feed.mode === 'new' }" @click="changeMode('new')">新发现</button>
      </div>
    </header>
    <form class="feed-condition" @submit.prevent="refresh">
      <input v-model="feed.requestText" maxlength="2000" placeholder="想看什么？例如日语摇滚，不要 Rap" aria-label="当前视频偏好" />
      <button :disabled="feed.loading">换一组</button>
    </form>
    <p v-if="feed.error" class="feed-error" role="alert">{{ feed.error }}</p>
    <div v-if="!feed.items.length" class="feed-empty" role="status">
      {{ feed.loading ? '正在准备推荐…' : feed.supplyState === 'replenishing' ? '正在发现新的音乐视频，稍后可刷新。' : '暂时没有合适的视频，试试调整条件。' }}
      <button v-if="!feed.loading" @click="refresh">刷新</button>
    </div>
    <div v-else ref="scroller" class="feed-scroller" @scroll.passive="onScroll">
      <div :style="{ height: `${rangeStart * viewportHeight}px` }" aria-hidden="true" />
      <article v-for="entry in windowItems" :key="entry.item.itemId" :data-feed-item="entry.item.itemId" class="feed-slide" :style="{ height: `${viewportHeight}px` }">
        <div v-if="entry.item.suppressed" class="feed-empty">这条内容已不可用</div>
        <div v-else class="feed-card">
          <video v-if="entry.index === activeIndex" ref="video" :muted="muted" playsinline preload="none"
            :poster="mediaUrl(entry.item.coverUrl || entry.item.track?.cover)" @playing="onPlaying" @pause="onPause"
            @waiting="onPause" @ended="onEnded" @timeupdate="onTime" />
          <img v-else :src="mediaUrl(entry.item.coverUrl || entry.item.track?.cover)" :alt="entry.item.track?.title" loading="lazy" />
          <div class="feed-caption"><strong>{{ entry.item.track?.title }}</strong><span>{{ entry.item.track?.owner }}</span></div>
          <div v-if="entry.index === activeIndex" class="feed-controls">
            <span role="status">{{ status }}</span>
            <button @click="togglePlay">{{ playing ? '暂停' : '播放' }}</button>
            <button @click="muted = !muted">{{ muted ? '开启声音' : '静音' }}</button>
            <button :disabled="reactionBusy" :class="{ selected: entry.item.reaction === 'like' }" @click="react('like')">喜欢 {{ entry.item.counts?.likes || 0 }}</button>
            <button :disabled="reactionBusy" @click="react('dislike')">不感兴趣</button>
            <button v-if="status === '播放失败，请重试'" @click="begin(entry.item)">重试</button>
            <button @click="next">下一个 ↓</button>
          </div>
        </div>
      </article>
      <div :style="{ height: `${Math.max(0, feed.items.length - rangeEnd) * viewportHeight}px` }" aria-hidden="true" />
      <div class="feed-footer">{{ feed.loading ? '正在加载…' : feed.hasMore ? '' : '这一组已经看完' }}<button v-if="feed.error" @click="feed.more">重试加载</button></div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useFeedStore } from '@/stores/feedStore'
import { feedApi, type FeedEvent, type FeedItem, type Playback, type Reaction } from '@/api/feed'
import { fetchSettings, mediaUrl } from '@/api/client'
import { VideoPlayerAdapter } from '@/video/VideoPlayerAdapter'
import { claimVideoPlayback } from '@/media/PlaybackCoordinator'
import { usePlayerStore } from '@/stores/playerStore'

const feed = useFeedStore()
const scroller = ref<HTMLDivElement | null>(null)
const video = ref<HTMLVideoElement[] | HTMLVideoElement | null>(null)
const activeIndex = ref(0), viewportHeight = ref(600), muted = ref(true), playing = ref(false)
const status = ref(''), reactionBusy = ref(false)
const rangeStart = computed(() => Math.max(0, activeIndex.value - 1))
const rangeEnd = computed(() => Math.min(feed.items.length, activeIndex.value + 2))
const windowItems = computed(() => feed.items.slice(rangeStart.value, rangeEnd.value).map((item, n) => ({ item, index: rangeStart.value + n })))
const active = computed(() => feed.items[activeIndex.value])
const adapter = new VideoPlayerAdapter()
let generation = 0, playback: Playback | null = null, heartbeat: number | undefined, exposure: number | undefined
let releaseOwner: (() => void) | null = null, watchMs = 0, lastTime = 0, started = false
let observer: ResizeObserver | null = null
const element = () => Array.isArray(video.value) ? video.value[0] : video.value
function emit(type: string, extra: Partial<FeedEvent> = {}) {
  const item = active.value
  if (!item || item.suppressed) return
  void feedApi.events([{ eventId: crypto.randomUUID(), itemId: item.itemId, type,
    ...(playback ? { playbackId: playback.playbackId } : {}), ...extra }]).catch(() => undefined)
}
function stop() {
  generation += 1; window.clearInterval(heartbeat); window.clearTimeout(exposure)
  adapter.stop(); playing.value = false
  if (playback) { void feedApi.release(playback.playbackId).catch(() => undefined); playback = null }
  releaseOwner?.(); releaseOwner = null
}
async function begin(item?: FeedItem) {
  stop(); watchMs = 0; lastTime = 0; started = false
  if (!item || item.suppressed || document.hidden) return
  const version = generation
  exposure = window.setTimeout(() => {
    const card = scroller.value?.querySelector(`[data-feed-item="${item.itemId}"]`)
    if (version !== generation || document.hidden || !card || !scroller.value) return
    const rect = card.getBoundingClientRect(), viewport = scroller.value.getBoundingClientRect()
    const ratio = Math.max(0, Math.min(rect.bottom, viewport.bottom) - Math.max(rect.top, viewport.top)) / rect.height
    if (ratio >= .5) emit('impression', { visibleRatio: Math.min(1, ratio), visibleMs: 1000 })
  }, 1000)
  status.value = '正在准备视频…'
  const key = crypto.randomUUID()
  try {
    const value = await feedApi.start(item.contentId, key)
    if (version !== generation) { void feedApi.release(value.playbackId); return }
    playback = value
    heartbeat = window.setInterval(() => {
      if (!playback || version !== generation) return
      void feedApi.heartbeat(playback.playbackId).then(v => {
        if (version === generation) playback = v
      }).catch(() => { if (version === generation) { status.value = '播放已中断，请重试'; stop() } })
      if (started) emit('watch_progress', { watchMs: Math.round(watchMs) })
    }, 5000)
    while (value.status === 'preparing' || playback?.status === 'preparing') {
      await new Promise(resolve => window.setTimeout(resolve, 500))
      if (version !== generation || !playback) return
      playback = await feedApi.playback(playback.playbackId)
      if (playback.status !== 'preparing') break
    }
    if (version !== generation || !playback) return
    if (!playback.manifestUrl || !['streaming', 'ready'].includes(playback.status)) throw new Error('unavailable')
    await nextTick()
    const target = element()
    if (!target || version !== generation) return
    releaseOwner = claimVideoPlayback(stop, () => usePlayerStore().pause())
    adapter.attach(target, playback.manifestUrl,
      code => { if (version === generation) { status.value = '播放失败，请重试'; emit('playback_error', { errorType: code }); stop() } },
      () => { if (version === generation) { status.value = '点击播放开始观看'; emit('autoplay_blocked') } })
    status.value = playback.mode === 'cold' ? '首次观看，正在保存' : '已准备好'
  } catch { if (version === generation) { status.value = '播放失败，请重试'; stop() } }
}
function onPlaying(event?: Event) { if (event && event.target !== element()) return; playing.value = true; lastTime = performance.now(); if (!started) { started = true; emit('play_started', { watchMs: 0 }) } }
function onPause(event?: Event) { if (event && event.target !== element()) return; onTime(event); playing.value = false; lastTime = 0 }
function onTime(event?: Event) {
  if (event && event.target !== element()) return
  const now = performance.now()
  if (playing.value && !document.hidden && lastTime) watchMs += Math.min(now - lastTime, 2000)
  lastTime = now
}
async function onEnded(event?: Event) {
  if (event && event.target !== element()) return
  onPause(event)
  const version = generation, current = playback
  if (!current) return
  try { const value = await feedApi.playback(current.playbackId); if (version !== generation) return
    if (value.sourceComplete) emit('complete', { watchMs: Math.round(watchMs) }); else emit('interrupt')
  } catch { if (version === generation) emit('interrupt') }
}
function togglePlay() { if (!playback) { void begin(active.value); return }; const target = element(); if (!target) return; if (target.paused) void target.play().catch(() => { status.value = '点击播放开始观看' }); else target.pause() }
function onScroll() { if (!scroller.value) return; activeIndex.value = Math.max(0, Math.min(feed.items.length - 1, Math.round(scroller.value.scrollTop / viewportHeight.value))) }
function next() { scroller.value?.scrollTo({ top: Math.min(activeIndex.value + 1, feed.items.length - 1) * viewportHeight.value, behavior: 'smooth' }) }
async function react(state: Reaction) {
  const item = active.value
  if (!item || reactionBusy.value) return
  reactionBusy.value = true
  try { const result = await feedApi.react(item, item.reaction === state ? 'neutral' : state); item.reaction = result.state; item.relationVersion = result.relationVersion; if (state === 'dislike') { item.suppressed = true; stop(); next() } }
  catch (e) { status.value = e instanceof Error ? e.message : '反馈失败，请重试' }
  finally { reactionBusy.value = false }
}
async function refresh() { stop(); activeIndex.value = 0; if (scroller.value) scroller.value.scrollTop = 0; await feed.reset() }
async function changeMode(mode: 'personal' | 'new') { if (feed.mode !== mode) { feed.mode = mode; await refresh() } }
function visibility() { if (document.hidden) { emit('interrupt'); stop() } else void begin(active.value) }
function keys(e: KeyboardEvent) { if ((e.target as HTMLElement)?.matches('input,textarea')) return; if (e.key === 'ArrowDown') { e.preventDefault(); next() } else if (e.key === 'ArrowUp') { e.preventDefault(); scroller.value?.scrollTo({ top: Math.max(0, activeIndex.value - 1) * viewportHeight.value, behavior: 'smooth' }) } }
watch(() => active.value?.itemId, () => { void begin(active.value); if (activeIndex.value >= feed.items.length - 3) void feed.more() })
onMounted(async () => {
  if (!(await fetchSettings()).feedEnabled) { feed.error = '视频功能尚未开启'; return }
  document.addEventListener('visibilitychange', visibility); window.addEventListener('keydown', keys)
  await refresh(); await nextTick()
  if (scroller.value) { observer = new ResizeObserver(entries => { viewportHeight.value = Math.max(300, entries[0].contentRect.height) }); observer.observe(scroller.value); viewportHeight.value = scroller.value.clientHeight }
})
onBeforeUnmount(() => { stop(); feed.stop(); observer?.disconnect(); document.removeEventListener('visibilitychange', visibility); window.removeEventListener('keydown', keys) })
</script>

<style scoped>
.feed-page { height:100%; min-height:0; display:flex; flex-direction:column; padding:20px 24px; gap:12px; }
.feed-header { display:flex; align-items:center; justify-content:space-between; gap:16px; }
h1 { font-size:24px; margin:0 0 6px; }.feed-header p { margin:0; color:var(--color-text-secondary); font-size:13px; }
.feed-tabs,.feed-condition,.feed-controls { display:flex; gap:8px; align-items:center; }.feed-condition input { flex:1; min-width:0; padding:10px 14px; border:1px solid var(--color-border); border-radius:10px; background:var(--color-bg-app); color:inherit; }
button { border:1px solid var(--color-border); border-radius:9px; padding:8px 12px; color:inherit; background:var(--color-bg-sidebar); cursor:pointer; }button.selected { color:var(--color-accent); border-color:currentColor; }button:disabled { opacity:.5; cursor:default; }
.feed-scroller { flex:1; min-height:300px; overflow-y:auto; scroll-snap-type:y mandatory; position:relative; scrollbar-width:thin; }.feed-slide { scroll-snap-align:start; box-sizing:border-box; padding:0 0 12px; }
.feed-card { height:100%; position:relative; border-radius:16px; overflow:hidden; background:#101114; color:#fff; }.feed-card video,.feed-card>img { width:100%; height:100%; object-fit:contain; }
.feed-caption { position:absolute; bottom:76px; left:20px; right:20px; display:grid; gap:5px; text-shadow:0 1px 6px #000; pointer-events:none; }.feed-caption span { font-size:13px; opacity:.8; }.feed-controls { position:absolute; left:0; right:0; bottom:0; padding:14px 18px; background:linear-gradient(transparent,rgba(0,0,0,.85)); flex-wrap:wrap; }.feed-controls span { flex:1; font-size:12px; }.feed-controls button { background:rgba(30,30,35,.85); border-color:#555; }
.feed-empty { flex:1; display:flex; align-items:center; justify-content:center; gap:16px; color:var(--color-text-secondary); }.feed-error { margin:0; color:#c44; }.feed-footer { text-align:center; padding:8px; font-size:12px; }
@media(max-width:700px) { .feed-page { padding:12px; }.feed-header { align-items:flex-start; flex-direction:column; }.feed-header p { display:none; }.feed-controls { padding:10px; gap:5px; }.feed-controls button { padding:7px 8px; } }
</style>
