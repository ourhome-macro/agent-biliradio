<template><div class="ugc-video"><video ref="video" controls playsinline preload="metadata" @play="claim" /><p v-if="message" role="status">{{ message }}</p><button v-if="failed" @click="start">重试播放</button></div></template>
<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { feedApi, type Playback } from '@/api/feed'
import { VideoPlayerAdapter } from '@/video/VideoPlayerAdapter'
import { claimVideoPlayback } from '@/media/PlaybackCoordinator'
import { usePlayerStore } from '@/stores/playerStore'
const props = defineProps<{ contentId: string }>()
const video = ref<HTMLVideoElement>(), message = ref(''), failed = ref(false)
const adapter = new VideoPlayerAdapter()
let alive = true, generation = 0, playback: Playback | null = null, timer: ReturnType<typeof setTimeout> | undefined
let refreshAt = 0, retries = 0, releaseOwner: (() => void) | null = null
function claim() { if (!releaseOwner) releaseOwner = claimVideoPlayback(() => { stop(); failed.value = true; message.value = '已切换到音乐播放，可重新打开视频' }, () => usePlayerStore().pause()) }
function stop() {
  generation += 1; clearTimeout(timer); adapter.stop(); releaseOwner?.(); releaseOwner = null
  if (playback) void feedApi.release(playback.playbackId).catch(() => undefined)
  playback = null
}
function attach(value: Playback, version: number, preserve = false) {
  if (!video.value || !value.manifestUrl || version !== generation) return
  const position = preserve ? video.value.currentTime : 0, autoplay = preserve && !video.value.paused
  refreshAt = Date.now() + (value.refreshAfterSeconds || 3600) * 1000
  adapter.attach(video.value, value.manifestUrl, () => { if (version === generation) { failed.value = true; message.value = '视频暂时无法播放，请重试'; stop() } }, () => { message.value = '点击播放开始观看' }, { position, autoplay })
}
async function beat(version: number) {
  if (!playback || version !== generation || !alive) return
  const controller = new AbortController(), deadline = setTimeout(() => controller.abort(), 3500)
  try {
    const value = await feedApi.heartbeat(playback.playbackId, controller.signal)
    if (version !== generation) return
    if (!value.sourceComplete || value.status !== 'ready') throw new Error('Media unavailable')
    playback = value; retries = 0
    if (Date.now() >= refreshAt) attach(value, version, true)
    timer = setTimeout(() => void beat(version), Math.min(5000, Math.max(250, refreshAt - Date.now())))
  } catch {
    if (version !== generation) return
    retries += 1
    if (retries >= 3 || Date.now() / 1000 >= (playback?.leaseExpiresAt || playback?.expiresAt || 0) - 1) { failed.value = true; message.value = '连接已中断，请重试'; stop() }
    else timer = setTimeout(() => void beat(version), retries * 1000)
  } finally { clearTimeout(deadline) }
}
async function start() {
  stop(); const version = generation; failed.value = false; message.value = '正在加载视频…'; retries = 0
  const controller = new AbortController(), timeout = setTimeout(() => controller.abort(), 8000)
  try {
    const value = await feedApi.start(props.contentId, crypto.randomUUID(), controller.signal)
    if (!alive || version !== generation) { void feedApi.release(value.playbackId); return }
    playback = value
    if (!value.sourceComplete || value.status !== 'ready') throw new Error('Media is not available')
    attach(value, version); message.value = ''
    timer = setTimeout(() => void beat(version), Math.min(5000, Math.max(250, refreshAt - Date.now())))
  } catch { if (alive && version === generation) { failed.value = true; message.value = '视频不可用，或当前账号没有观看权限'; stop() } }
  finally { clearTimeout(timeout) }
}
watch(() => props.contentId, start, { immediate: true, flush: 'post' })
onBeforeUnmount(() => { alive = false; stop() })
</script>
<style scoped>.ugc-video{margin:20px 0}.ugc-video video{width:100%;max-height:65vh;background:#151515;border-radius:12px}.ugc-video p{color:var(--color-text-secondary)}button{padding:8px 12px;border:1px solid var(--color-border);border-radius:8px;background:var(--color-bg-sidebar);color:inherit}</style>
