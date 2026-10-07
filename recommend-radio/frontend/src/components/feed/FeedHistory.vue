<template>
  <button class="history-button" @click="open">观看历史</button>
  <dialog ref="dialog" aria-label="观看历史">
    <header><strong>观看历史</strong><button @click="dialog?.close()">关闭</button></header>
    <p v-if="error" role="alert">{{ error }}</p>
    <p v-if="!entries.length && !loading">还没有有效观看记录。</p>
    <button v-for="entry in entries" :key="entry.contentId" class="history-entry" @click="select(entry.contentId)">
      <strong>{{ entry.track.title }}</strong><span>{{ entry.track.owner }} · {{ Math.round(entry.watchMs / 1000) }} 秒{{ entry.completed ? ' · 已看完' : '' }}</span>
      <time>{{ new Date(entry.watchedAt * 1000).toLocaleString() }}</time>
    </button>
    <button v-if="cursor || error" :disabled="loading" @click="load">{{ loading ? '正在加载…' : '加载更多' }}</button>
    <p v-else-if="loading" role="status">正在加载…</p>
  </dialog>
</template>
<script setup lang="ts">
import { onBeforeUnmount, ref } from 'vue'
import { feedProductApi, type HistoryEntry } from '@/api/feedProduct'
const emit = defineEmits<{ select: [contentId: string] }>()
const dialog = ref<HTMLDialogElement>(), entries = ref<HistoryEntry[]>([]), cursor = ref<string | null>(null)
const loading = ref(false), error = ref('')
let generation = 0
async function open() { ++generation; entries.value = []; cursor.value = null; loading.value = false; dialog.value?.showModal(); await load() }
async function load() {
  if (loading.value) return
  const version = generation; loading.value = true; error.value = ''
  try { const result = await feedProductApi.history(cursor.value || undefined); if (version !== generation) return
    const seen = new Set(entries.value.map(item => item.contentId)); entries.value.push(...result.items.filter(item => !seen.has(item.contentId))); cursor.value = result.nextCursor
  } catch { if (version === generation) error.value = '历史加载失败，请重试' }
  finally { if (version === generation) loading.value = false }
}
function select(id: string) { dialog.value?.close(); emit('select', id) }
onBeforeUnmount(() => { ++generation })
</script>
<style scoped>
button { border:1px solid var(--color-border); background:var(--color-bg-sidebar); color:inherit; border-radius:8px; padding:8px 12px; cursor:pointer; }dialog { width:min(480px,calc(100vw - 48px)); max-height:75vh; overflow:auto; border:1px solid var(--color-border); border-radius:14px; background:var(--color-bg-app); color:var(--color-text-primary); padding:20px; }dialog::backdrop { background:#0009; }header { display:flex; justify-content:space-between; align-items:center; }.history-entry { display:grid; width:100%; gap:6px; text-align:left; margin-top:10px; }.history-entry span,.history-entry time { font-size:12px; opacity:.7; }
</style>
