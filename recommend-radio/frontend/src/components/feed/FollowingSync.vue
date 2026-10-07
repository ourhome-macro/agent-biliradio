<template>
  <div class="following-sync">
    <button :disabled="busy" @click="sync">{{ busy ? '同步关注中…' : '同步 B 站关注' }}</button>
    <span v-if="message" role="status">{{ message }}</span>
    <button v-if="paused" @click="check">查询同步结果</button>
  </div>
</template>
<script setup lang="ts">
import { onBeforeUnmount, ref } from 'vue'
import { followingApi } from '@/api/following'
const emit = defineEmits<{ updated: [] }>()
const busy = ref(false), message = ref(''), paused = ref(false)
let alive = true, attempts = 0, timer: ReturnType<typeof setTimeout> | undefined
async function check() {
  clearTimeout(timer)
  try {
    const value = await followingApi.list()
    if (!alive) return
    if (!value.accountMid) { busy.value = false; message.value = '请先绑定 B 站账号'; return }
    if (value.sync.errorType || value.sync.status === 'failed') { busy.value = false; paused.value = false; message.value = '同步失败，可重新同步'; return }
    if (!['pending', 'queued', 'running', 'syncing'].includes(value.sync.status)) {
      busy.value = false; paused.value = false; message.value = `关注列表已更新（${value.items.filter(item => item.following).length} 位）`; emit('updated'); return
    }
    message.value = '正在读取 B 站关注列表…'
  } catch { if (!alive) return; message.value = '暂时无法查询同步进度' }
  attempts += 1
  if (attempts >= 8) { paused.value = true; busy.value = false; return }
  timer = setTimeout(() => void check(), Math.min(2000 * attempts, 10000))
}
async function sync() {
  if (busy.value) return
  busy.value = true; paused.value = false; message.value = ''; attempts = 0
  const key = crypto.randomUUID()
  try {
    await followingApi.sync(key)
    if (!alive) return
    message.value = '同步任务已提交'
    timer = setTimeout(() => void check(), 1000)
  } catch (cause) {
    if (!alive) return
    busy.value = false
    message.value = (cause as { status?: number }).status === 403 ? '请先绑定有效的 B 站账号' : '同步未确认，请重试'
  }
}
onBeforeUnmount(() => { alive = false; clearTimeout(timer) })
</script>
<style scoped>
.following-sync { display:flex; align-items:center; gap:8px; flex-wrap:wrap; font-size:12px; }
button { border:1px solid var(--color-border); border-radius:8px; padding:6px 10px; background:var(--color-bg-sidebar); color:inherit; cursor:pointer; }
button:disabled { opacity:.5; }
</style>
