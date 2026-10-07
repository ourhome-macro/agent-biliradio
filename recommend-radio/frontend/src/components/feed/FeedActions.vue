<template>
  <section class="feed-actions" aria-label="视频操作">
    <template v-if="item && !item.suppressed">
      <button :disabled="busy" :class="{ selected: item.reaction === 'like' }" @click="react(item, item.reaction === 'like' ? 'neutral' : 'like')">喜欢 {{ item.counts?.likes || 0 }}</button>
      <button :disabled="busy" @click="dislike">不感兴趣</button>
      <button @click="showPlaylists">加入歌单</button>
      <button @click="showDetail">详情 / 分享</button>
    </template>
    <span v-if="undo" class="undo" role="status">已减少此类推荐 <button :disabled="busy" @click="restore">撤销</button></span>
    <span v-if="message" role="status">{{ message }}</span>
    <span v-if="pending" aria-live="polite">计数同步中</span>
    <button v-if="pending && item && !item.suppressed" @click="refreshCounts(item)">刷新计数</button>
    <dialog ref="dialog" @close="panel = null" @click="closeBackdrop">
      <header><strong>{{ panel === 'playlist' ? '加入歌单' : '视频详情' }}</strong><button aria-label="关闭" @click="dialog?.close()">关闭</button></header>
      <template v-if="panel === 'playlist'">
        <p v-if="!playlists.length">还没有歌单，可先到“我的歌单”创建。</p>
        <button v-for="list in playlists" :key="list.id" class="list-button" :disabled="adding" @click="addTo(list.id)">{{ list.name }}</button>
      </template>
      <template v-if="panel === 'detail' && detail">
        <h3>{{ detail.track.title }}</h3><p>{{ detail.track.owner }} · {{ Math.round(detail.track.duration) }} 秒</p>
        <p class="description">{{ detail.track.description || '暂无简介' }}</p>
        <button v-if="detail.sharePath" @click="share">分享此视频</button>
        <p v-else>此视频仅对当前账号可见。</p>
        <label v-if="shareUrl">分享链接 <input readonly :value="shareUrl" @focus="($event.target as HTMLInputElement).select()" /></label>
      </template>
    </dialog>
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import type { FeedItem, Reaction } from '@/api/feed'
import { feedProductApi, type ContentDetail } from '@/api/feedProduct'
import { addPlaylistItemsRemote, fetchPlaylists } from '@/api/client'
import type { Playlist, Track } from '@/types'

const props = defineProps<{ item?: FeedItem }>()
const emit = defineEmits<{
  dismiss: [itemId: string]; restore: [itemId: string]; status: [message: string]
  reaction: [value: { itemId: string; contentId: string; state: Reaction; relationVersion: number; counts?: ContentDetail['counts'] }]
}>()
const busy = ref(false), pending = ref(false), message = ref(''), dialog = ref<HTMLDialogElement>()
const panel = ref<'playlist' | 'detail' | null>(null), detail = ref<ContentDetail | null>(null)
const playlists = ref<Playlist[]>([]), adding = ref(false), shareUrl = ref('')
const undo = ref<{ item: FeedItem; state: Reaction } | null>(null)
let pollVersion = 0, alive = true, panelVersion = 0
let playlistTrack: Track | undefined
function status(value: string) { if (alive) { message.value = value; emit('status', value) } }
function publish(item: FeedItem) {
  emit('reaction', { itemId: item.itemId, contentId: item.contentId, state: item.reaction || 'neutral', relationVersion: item.relationVersion || 0, counts: item.counts })
}
async function refreshCounts(item: FeedItem) {
  const version = ++pollVersion
  for (const delay of [0, 400, 800, 1600, 3200]) {
    if (delay) await new Promise(resolve => setTimeout(resolve, delay))
    if (!alive || version !== pollVersion) return
    try {
      const value = await feedProductApi.detail(item.contentId)
      if (!alive || version !== pollVersion) return
      if (value.relationVersion < (item.relationVersion || 0)) continue
      item.reaction = value.reaction; item.relationVersion = value.relationVersion
      if (!value.projectionPending) {
        item.counts = value.counts; pending.value = false; publish(item); return
      }
      pending.value = true
    } catch { /* The committed reaction is preserved; the user can refresh later. */ }
  }
  if (alive && version === pollVersion) status('反馈已保存，计数稍后更新')
}
async function react(item: FeedItem, state: Reaction): Promise<boolean> {
  if (busy.value) return false
  busy.value = true; ++pollVersion
  const previous = item.reaction || 'neutral', key = crypto.randomUUID()
  try {
    let result
    try { result = await feedProductApi.react(item, state, key) }
    catch (error) {
      const code = (error as { status?: number }).status
      if (!(error instanceof TypeError) && (!code || code < 500)) throw error
      result = await feedProductApi.react(item, state, key)
    }
    if (!alive) return false
    item.reaction = result.state; item.relationVersion = result.relationVersion
    const counts = item.counts || { likes: 0, dislikes: 0, version: 0 }
    item.counts = { ...counts,
      likes: Math.max(0, counts.likes + Number(state === 'like') - Number(previous === 'like')),
      dislikes: Math.max(0, counts.dislikes + Number(state === 'dislike') - Number(previous === 'dislike')) }
    pending.value = true; publish(item); status('反馈已保存')
    void refreshCounts(item)
    return true
  } catch (error) {
    if ((error as { status?: number }).status === 409) {
      status('反馈已在其他页面更新，已刷新当前状态，请重新选择')
      void refreshCounts(item)
    } else status('反馈未确认，请重试')
    return false
  } finally { busy.value = false }
}
async function dislike() {
  const item = props.item
  if (!item) return
  const previous = item.reaction || 'neutral'
  if (await react(item, 'dislike')) { undo.value = { item, state: previous }; emit('dismiss', item.itemId) }
}
async function restore() {
  const value = undo.value
  if (value && await react(value.item, value.state)) { emit('restore', value.item.itemId); undo.value = null; status('已撤销不感兴趣') }
}
async function showPlaylists() {
  const item = props.item, version = ++panelVersion
  if (!item?.track) return
  try {
    const lists = await fetchPlaylists()
    if (!alive || version !== panelVersion) return
    playlists.value = lists; playlistTrack = item.track; panel.value = 'playlist'; dialog.value?.showModal()
  } catch { status('歌单加载失败，请重试') }
}
async function addTo(id: string) {
  if (!playlistTrack || adding.value) return
  adding.value = true
  try { await addPlaylistItemsRemote(id, [playlistTrack]); dialog.value?.close(); status('已加入歌单') }
  catch { status('加入歌单失败，请重试') }
  finally { adding.value = false }
}
async function showDetail() {
  const item = props.item, version = ++panelVersion
  if (!item) return
  try {
    const value = await feedProductApi.detail(item.contentId)
    if (!alive || version !== panelVersion) return
    detail.value = value; panel.value = 'detail'; shareUrl.value = ''; dialog.value?.showModal()
  } catch { status('内容已不可用或详情加载失败') }
}
async function share() {
  if (!detail.value?.sharePath) return
  shareUrl.value = new URL(detail.value.sharePath, location.origin).href
  try {
    if (navigator.share) await navigator.share({ title: detail.value.track.title, url: shareUrl.value })
    else if (navigator.clipboard) { await navigator.clipboard.writeText(shareUrl.value); status('分享链接已复制') }
  } catch { /* Link remains visible for manual copy, including permission denial. */ }
}
function closeBackdrop(event: MouseEvent) { if (event.target === dialog.value) { const box = dialog.value.getBoundingClientRect(); if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) dialog.value.close() } }
watch(() => props.item?.itemId, () => { ++pollVersion; ++panelVersion; pending.value = false; message.value = ''; dialog.value?.close() })
onBeforeUnmount(() => { alive = false; ++pollVersion; ++panelVersion })
</script>

<style scoped>
.feed-actions { display:flex; flex-wrap:wrap; align-items:center; gap:8px; font-size:13px; }
button { border:1px solid var(--color-border); border-radius:8px; padding:7px 10px; background:var(--color-bg-sidebar); color:inherit; cursor:pointer; }button:disabled { opacity:.5; }.selected { color:var(--color-accent); border-color:currentColor; }.undo { display:flex; align-items:center; gap:6px; }
dialog { width:min(480px,calc(100vw - 48px)); max-height:75vh; overflow:auto; border:1px solid var(--color-border); border-radius:14px; background:var(--color-bg-app); color:var(--color-text-primary); padding:20px; }dialog::backdrop { background:#0009; }header { display:flex; align-items:center; justify-content:space-between; }.list-button { display:block; width:100%; text-align:left; margin-top:10px; }.description { white-space:pre-wrap; line-height:1.6; }label,input { display:block; width:100%; margin-top:10px; }input { box-sizing:border-box; padding:8px; }
</style>
