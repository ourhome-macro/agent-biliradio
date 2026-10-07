<template>
  <main class="operations">
    <header><div><h1>视频运营</h1><p>内容状态、媒体任务和播放质量</p></div><button :disabled="loading" @click="load">{{ loading ? '刷新中…' : '刷新' }}</button></header>
    <p v-if="error" class="error" role="alert">{{ error }}</p>
    <p v-if="message" role="status">{{ message }}</p>
    <p v-if="updatedAt" class="muted">更新于 {{ updatedAt }} · 前台每 15 秒刷新</p>
    <template v-if="snapshot">
      <section aria-label="告警"><p v-for="alert in snapshot.alerts || []" :key="alert.code" :class="['notice', alert.level]" role="status">{{ alert.message }}</p></section>
      <section class="capacity" aria-label="存储容量">
        <article><h2>已留存媒体</h2><strong>{{ bytes(snapshot.inventory.retainedBytes) }} / {{ bytes(snapshot.inventory.storageBudgetBytes) }}</strong><meter min="0" :max="snapshot.inventory.storageBudgetBytes || 1" :value="snapshot.inventory.retainedBytes || 0" aria-label="媒体存储使用量" /></article>
        <article><h2>临时工作目录</h2><strong>{{ bytes(snapshot.inventory.scratchBytes) }} / {{ bytes(snapshot.inventory.scratchBudgetBytes) }}</strong><meter min="0" :max="snapshot.inventory.scratchBudgetBytes || 1" :value="snapshot.inventory.scratchBytes || 0" aria-label="临时存储使用量" /></article>
      </section>
      <section aria-label="播放指标"><h2>播放质量</h2><div class="metric-grid">
        <article v-for="mode in ['cold', 'stored'] as const" :key="mode"><h3>{{ mode === 'cold' ? '冷首播' : '存储播放' }}</h3><p>P50 {{ milliseconds(snapshot.metrics.startupMs?.[mode]?.p50) }} · P95 {{ milliseconds(snapshot.metrics.startupMs?.[mode]?.p95) }}</p><span>{{ snapshot.metrics.startupMs?.[mode]?.count || 0 }} 个样本</span></article>
        <article><h3>缓冲占比</h3><p>{{ percent(snapshot.metrics.bufferingRatio) }}</p></article><article><h3>播放错误率</h3><p>{{ percent(snapshot.metrics.playbackErrorRate) }}</p></article><article><h3>滑动次数</h3><p>{{ snapshot.metrics.swipeCount || 0 }}</p></article>
      </div></section>
      <section><div class="section-head"><h2>内容目录</h2><input v-model="query" type="search" placeholder="搜索当前列表标题或 ID" aria-label="搜索内容目录" /></div>
        <p class="muted">{{ statusSummary(snapshot.inventory.contents) }}。此处展示近期内容；上下架会立即影响后续授权读取。</p>
        <div class="table-wrap"><table><thead><tr><th scope="col">内容</th><th scope="col">状态</th><th scope="col">媒体</th><th scope="col">操作</th></tr></thead><tbody>
          <tr v-for="content in visibleContents" :key="content.contentId"><td><strong>{{ content.title }}</strong><small>{{ content.contentId }}</small></td><td>{{ content.status }}</td><td>{{ content.assetStatus || 'empty' }}</td><td><button v-if="content.status === 'admitted'" @click="prepare({ kind: 'status', id: content.contentId, title: content.title, status: 'retired' })">下架</button><button v-else-if="content.status === 'retired'" @click="prepare({ kind: 'status', id: content.contentId, title: content.title, status: 'admitted' })">重新上架</button><span v-else>待音乐准入</span></td></tr>
        </tbody></table><p v-if="!visibleContents.length">暂无匹配内容。</p></div>
      </section>
      <section><h2>近期导入任务</h2><div class="table-wrap"><table><thead><tr><th scope="col">任务</th><th scope="col">状态 / 阶段</th><th scope="col">错误</th><th scope="col">操作</th></tr></thead><tbody>
        <tr v-for="job in snapshot.inventory.imports || []" :key="job.importId"><td><small>{{ job.importId }}</small><span>第 {{ job.attempt }} 次</span></td><td>{{ job.status }} / {{ job.stage }}</td><td>{{ job.errorType || '—' }}</td><td><button v-if="job.status === 'failed'" @click="prepare({ kind: 'retry', id: job.importId, title: job.importId })">重试</button><span v-else>—</span></td></tr>
      </tbody></table><p v-if="!snapshot.inventory.imports?.length">暂无导入任务。</p></div></section>
      <p class="muted">备份与恢复通过服务器运维命令执行。恢复前停止写入并核对数据库与媒体清单，页面不提供文件路径或覆盖恢复操作。</p>
    </template>
    <dialog ref="dialog" @close="operation = null">
      <form @submit.prevent="commit"><h2>{{ operation?.kind === 'retry' ? '重试媒体任务' : operation?.status === 'retired' ? '下架内容' : '重新上架内容' }}</h2><p>{{ operation?.title }}</p>
        <label>操作原因<textarea v-model="reason" required minlength="3" maxlength="500" rows="3" placeholder="记录本次操作的原因" /></label><p v-if="commandError" class="error" role="alert">{{ commandError }}</p>
        <div class="dialog-actions"><button type="button" :disabled="saving" @click="dialog?.close()">取消</button><button :disabled="saving || reason.trim().length < 3">{{ saving ? '提交中…' : '确认操作' }}</button></div>
      </form>
    </dialog>
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { apiRequest } from '@/api/client'
interface Content { contentId: string; title: string; status: string; assetStatus?: string }
interface Import { importId: string; assetId: string; status: string; stage: string; attempt: number; errorType?: string; updatedAt: number }
interface Distribution { p50?: number | null; p95?: number | null; count?: number }
interface Snapshot {
  inventory: { contents: Record<string, number>; assets: Record<string, number>; contentsList?: Content[]; imports?: Import[]; retainedBytes: number; storageBudgetBytes: number; scratchBytes: number; scratchBudgetBytes: number }
  metrics: { startupMs?: { cold?: Distribution; stored?: Distribution }; bufferingRatio?: number | null; playbackErrorRate?: number | null; swipeCount?: number }
  alerts?: { level: string; code: string; message: string }[]
}
type Operation = { kind: 'retry' | 'status'; id: string; title: string; status?: 'admitted' | 'retired' }
const snapshot = ref<Snapshot | null>(null), loading = ref(false), error = ref(''), message = ref(''), updatedAt = ref(''), query = ref('')
const dialog = ref<HTMLDialogElement>(), operation = ref<Operation | null>(null), reason = ref(''), saving = ref(false), commandError = ref('')
let alive = true, timer: ReturnType<typeof setInterval> | undefined, request: AbortController | null = null
const visibleContents = computed(() => (snapshot.value?.inventory.contentsList || []).filter(row => `${row.title} ${row.contentId}`.toLocaleLowerCase().includes(query.value.trim().toLocaleLowerCase())))
const bytes = (value?: number) => value == null ? '—' : value >= 1073741824 ? `${(value / 1073741824).toFixed(2)} GiB` : `${(value / 1048576).toFixed(1)} MiB`
const milliseconds = (value?: number | null) => value == null ? '暂无样本' : `${(value / 1000).toFixed(2)} 秒`
const percent = (value?: number | null) => value == null ? '暂无样本' : `${(value * 100).toFixed(2)}%`
const statusSummary = (values: Record<string, number>) => Object.entries(values || {}).map(([key, count]) => `${key}: ${count}`).join(' · ') || '暂无内容'
async function load() {
  if (loading.value || !alive) return
  loading.value = true; error.value = ''; request = new AbortController()
  try { const value = await apiRequest<Snapshot>('/api/admin/feed/operations', { signal: request.signal }); if (alive) { snapshot.value = value; updatedAt.value = new Date().toLocaleTimeString() } }
  catch (value) { if (alive) error.value = value instanceof Error ? value.message : '运营数据加载失败' }
  finally { if (alive) loading.value = false }
}
function prepare(value: Operation) { operation.value = value; reason.value = ''; commandError.value = ''; dialog.value?.showModal() }
async function commit() {
  const value = operation.value
  if (!value || saving.value || reason.value.trim().length < 3) return
  saving.value = true; commandError.value = ''
  try {
    const path = value.kind === 'retry' ? `/api/admin/feed/imports/${encodeURIComponent(value.id)}/retry` : `/api/admin/feed/content/${encodeURIComponent(value.id)}/status`
    const result = await apiRequest<{ requiresViewer?: boolean }>(path, { method: value.kind === 'retry' ? 'POST' : 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ reason: reason.value.trim(), ...(value.status ? { status: value.status } : {}) }) })
    if (!alive) return
    dialog.value?.close(); message.value = result?.requiresViewer ? '重试已排队；尚未获取完整的素材需要观看者保持播放' : '操作已完成'; await load()
  } catch (value) { if (alive) commandError.value = value instanceof Error ? value.message : '操作未确认，请刷新后重试' }
  finally { saving.value = false }
}
onMounted(() => { void load(); timer = setInterval(() => { if (!document.hidden && !saving.value) void load() }, 15000) })
onBeforeUnmount(() => { alive = false; clearInterval(timer); request?.abort() })
</script>

<style scoped>
.operations { padding:24px; overflow:auto; height:100%; box-sizing:border-box; }.operations>header,.section-head { display:flex; justify-content:space-between; align-items:center; gap:12px; }h1 { margin:0; font-size:26px; }h2 { font-size:18px; }h3 { margin:0; font-size:14px; }header p,.muted { color:var(--color-text-secondary); font-size:13px; }section { margin-top:24px; }.capacity,.metric-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }article { padding:16px; border:1px solid var(--color-border); border-radius:12px; }meter { width:100%; display:block; margin-top:12px; }button,input,textarea { padding:8px 12px; border:1px solid var(--color-border); border-radius:8px; background:var(--color-bg-sidebar); color:inherit; }button { cursor:pointer; }button:disabled { opacity:.5; }.table-wrap { overflow:auto; }table { border-collapse:collapse; width:100%; font-size:13px; }th,td { text-align:left; padding:12px 8px; border-bottom:1px solid var(--color-border); }th { white-space:nowrap; }td small { display:block; margin:5px 0; overflow-wrap:anywhere; }.error { color:#c44; }.notice { padding:12px; border-left:3px solid #ca8b1a; background:#ca8b1a15; }.notice.critical,.notice.error { border-color:#c44; }dialog { width:min(440px,calc(100vw - 48px)); border:1px solid var(--color-border); border-radius:14px; background:var(--color-bg-app); color:var(--color-text-primary); padding:20px; }dialog::backdrop { background:#0009; }dialog p { overflow-wrap:anywhere; }label,textarea { display:block; width:100%; box-sizing:border-box; }textarea { margin-top:8px; }.dialog-actions { display:flex; justify-content:flex-end; gap:8px; margin-top:16px; }@media(max-width:700px) { .operations { padding:14px; }.section-head { align-items:stretch; flex-direction:column; } }
</style>
