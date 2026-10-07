<template>
  <section class="follow-author" aria-label="作者关注">
    <template v-if="creator?.supported && item && !item.suppressed">
      <span>{{ creator.name || item?.track?.owner }}</span>
      <button v-if="creator.syncStatus !== 'unbound'" :disabled="busy" :class="{ selected: creator.following }" @click="setFollow(creator, !creator.following)">{{ creator.following ? '取消关注' : '关注作者' }}</button>
      <span v-if="creator.syncStatus === 'unbound'">绑定 B 站账号后可关注</span>
      <span v-else-if="creator.syncStatus === 'pending' || creator.syncStatus === 'unknown'" role="status">正在同步到 B 站…</span>
      <span v-else-if="creator.syncStatus === 'failed'" role="status">B 站同步失败 <button :disabled="busy" @click="retry(creator)">重试同步</button></span>
      <span v-else class="hint">{{ creator.following ? '已同步 B 站关注' : '操作会同步到 B 站' }}</span>
    </template>
    <span v-if="error" role="status">{{ error }} <button :disabled="busy" @click="load">刷新状态</button></span>
    <span v-if="lastAction && (!item || item.suppressed || lastAction.creatorId !== creator?.creatorId)" role="status">
      {{ lastAction.name }}：{{ actionText(lastAction) }}
      <button v-if="lastAction.syncStatus === 'failed'" :disabled="busy" @click="retry(lastAction)">重试同步</button>
    </span>
    <button v-if="pollPaused" @click="resumePoll">继续查询同步结果</button>
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import type { FeedItem } from '@/api/feed'
import { followingApi, type CreatorFollow } from '@/api/following'
const props = defineProps<{ item?: FeedItem }>()
const emit = defineEmits<{ changed: [value: CreatorFollow] }>()
const creator = ref<CreatorFollow | null>(null), lastAction = ref<CreatorFollow | null>(null)
const busy = ref(false), error = ref(''), pollPaused = ref(false)
let alive = true, generation = 0, controller: AbortController | null = null
let timer: ReturnType<typeof setTimeout> | undefined, pollVersion = 0
const actionText = (value: CreatorFollow) => value.syncStatus === 'synced'
  ? value.following ? '已关注' : '已取消关注' : value.syncStatus === 'failed' ? '同步失败' : '正在同步到 B 站…'
function apply(value: CreatorFollow) {
  if (creator.value?.creatorId === value.creatorId && value.relationVersion >= creator.value.relationVersion) creator.value = value
  if (lastAction.value?.creatorId === value.creatorId && value.relationVersion >= lastAction.value.relationVersion) lastAction.value = value
}
function poll(value: CreatorFollow, attempt = 0, version = ++pollVersion) {
  clearTimeout(timer)
  if (!alive || !['pending', 'unknown'].includes(value.syncStatus)) { pollPaused.value = false; return }
  const delays = [2000, 4000, 8000, 12000, 15000]
  if (attempt >= delays.length) { pollPaused.value = true; return }
  timer = setTimeout(async () => {
    try {
      const fresh = await followingApi.state(value.creatorId)
      if (!alive || version !== pollVersion) return
      apply(fresh)
      if (fresh.following && fresh.confirmedFollowing && fresh.syncStatus === 'synced' && value.syncStatus !== 'synced') emit('changed', fresh)
      poll(fresh, attempt + 1, version)
    } catch { if (alive && version === pollVersion) poll(value, attempt + 1, version) }
  }, delays[attempt])
}
function resumePoll() { const value = lastAction.value || creator.value; if (value) { pollPaused.value = false; poll(value) } }
async function load() {
  controller?.abort(); controller = new AbortController()
  const version = ++generation, item = props.item
  creator.value = null; error.value = ''
  if (!item || item.suppressed) return
  try {
    const value = await followingApi.creator(item.contentId, controller.signal)
    if (!alive || version !== generation) return
    creator.value = value
    if (!lastAction.value || lastAction.value.syncStatus === 'synced') poll(value)
  } catch (cause) { if (alive && version === generation && !(cause instanceof DOMException && cause.name === 'AbortError')) error.value = '作者关注状态暂不可用' }
}
async function setFollow(value: CreatorFollow, following: boolean) {
  if (busy.value) return
  busy.value = true; error.value = ''
  const key = crypto.randomUUID()
  try {
    let result: CreatorFollow
    try { result = await followingApi.set(value, following, key) }
    catch (cause) {
      const status = (cause as { status?: number }).status
      if (status && status < 500) throw cause
      result = await followingApi.set(value, following, key)
    }
    if (!alive) return
    lastAction.value = result; apply(result); emit('changed', result); poll(result)
  } catch (cause) {
    if (!alive) return
    const status = (cause as { status?: number }).status
    if (status === 409) {
      error.value = '关注状态已变化，请确认最新状态后重试'
      try { apply(await followingApi.state(value.creatorId)) } catch { /* Explicit refresh remains available. */ }
    } else if (status === 403) error.value = '请先绑定有效的 B 站账号，再操作关注'
    else error.value = '关注操作未确认，请重试'
  } finally { busy.value = false }
}
async function retry(value: CreatorFollow) {
  try {
    const fresh = await followingApi.state(value.creatorId)
    if (!alive) return
    apply(fresh); await setFollow(fresh, value.following)
  } catch { error.value = '读取关注状态失败，请刷新后重试' }
}
watch(() => [props.item?.contentId, props.item?.suppressed], load, { immediate: true })
onBeforeUnmount(() => { alive = false; generation += 1; pollVersion += 1; controller?.abort(); clearTimeout(timer) })
</script>
<style scoped>
.follow-author { display:flex; align-items:center; gap:8px; flex-wrap:wrap; font-size:12px; min-width:0; }
button { border:1px solid var(--color-border); border-radius:8px; padding:6px 10px; background:var(--color-bg-sidebar); color:inherit; cursor:pointer; }
button.selected { color:var(--color-accent); } button:disabled { opacity:.5; } .hint { color:var(--color-text-secondary); }
</style>
