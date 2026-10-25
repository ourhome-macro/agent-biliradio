<template>
  <main class="studio-page">
    <header><div><h1>创作中心</h1><p>记录音乐感受，或引用已入库的视频。</p></div><button :disabled="busy || !enabled" @click="newDraft">新建草稿</button></header>
    <p v-if="notice" role="status">{{ notice }}</p><p v-if="error" class="error" role="alert">{{ error }}</p>
    <div v-if="enabled" class="studio-layout">
      <aside aria-label="我的内容"><button :disabled="busy" @click="loadList">刷新列表</button><p v-if="!posts.length">还没有内容，先写一篇草稿。</p>
        <button v-for="post in posts" :key="post.contentId" class="post-row" :class="{ selected: current?.contentId === post.contentId }" :disabled="busy" @click="open(post.contentId)"><strong>{{ post.title || '未命名草稿' }}</strong><small>{{ post.status === 'draft' ? '草稿' : '已发布' }} · {{ post.visibility === 'public' ? '公开' : '仅自己' }}</small></button>
      </aside>
      <form class="editor" @submit.prevent="save">
        <h2>{{ current ? current.status === 'draft' ? '编辑草稿' : '编辑内容' : '新的草稿' }}</h2>
        <label>内容形式<select v-model="form.kind" aria-label="内容形式" :disabled="busy || !!current"><option value="text">文字</option><option value="video">引用视频</option><option value="audio">上传音频</option></select></label>
        <label>标题<input v-model="form.title" maxlength="200" :disabled="busy" placeholder="给这段分享起个标题" required /></label>
        <label>正文<textarea v-model="form.body" rows="12" maxlength="50000" :disabled="busy" placeholder="写下你的音乐感受…" /></label>
        <label v-if="form.kind === 'video'">已入库视频<select v-model="form.mediaSourceContentId" aria-label="已入库视频" :disabled="busy"><option value="">请选择可引用的视频</option><option v-for="media in mediaCandidates" :key="media.contentId" :value="media.contentId">{{ media.title }}{{ media.visibility === 'private' ? '（私有）' : '' }}</option></select><small>视频必须已完成入库；私有来源的可见范围由服务端校验。</small></label>
        <label v-if="form.kind === 'audio'">音频文件<input type="file" accept="audio/*,.flac,.wav,.m4a" :disabled="busy || current?.status === 'published' || !form.title.trim()" @change="uploadAudio" /><small>最大 64 MB。上传后自动校验处理，完成后可发布。</small><span role="status">{{ audioStatus }}</span><button v-if="current?.audioUploadId && !busy" type="button" @click="checkAudio">刷新音频状态</button></label>
        <label>可见范围<select v-model="form.visibility" aria-label="可见范围" :disabled="busy"><option value="private">仅自己</option><option value="public">公开</option></select></label>
        <p class="muted">{{ current?.status === 'published' ? '保存会更新已发布内容；改为“仅自己”后公开链接将不再对其他人可见。' : '保存为草稿不会对外发布，发布前请确认可见范围。' }}</p>
        <div class="actions"><button class="primary" :disabled="busy || !form.title.trim()">{{ busy ? '正在保存…' : '保存' }}</button><button v-if="current?.status === 'draft'" type="button" :disabled="busy || dirty" @click="publish">发布</button><RouterLink v-if="current" :to="`/content/${current.contentId}`">查看详情</RouterLink><button v-if="current" type="button" :disabled="busy" @click="reload">重新加载</button></div>
        <small v-if="dirty">有尚未保存的修改{{ current?.status === 'draft' ? '，保存后才能发布' : '' }}。</small>
        <div v-if="current" class="delete-row"><button v-if="!confirmDelete" type="button" :disabled="busy" @click="confirmDelete = true">删除内容</button><template v-else><span>删除后分享入口将失效。</span><button type="button" :disabled="busy" @click="remove">确认删除</button><button type="button" @click="confirmDelete = false">取消</button></template></div>
      </form>
    </div>
  </main>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { ugcApi, type UGCPost, type PostInput, type MediaCandidate } from '@/api/ugc'
const route = useRoute(), posts = ref<UGCPost[]>([]), current = ref<UGCPost | null>(null), mediaCandidates = ref<MediaCandidate[]>([])
const enabled = ref(false), busy = ref(false), error = ref(''), notice = ref(''), confirmDelete = ref(false)
const audioStatus = ref('')
const empty = (): PostInput => ({ kind: 'text', title: '', body: '', visibility: 'private', mediaSourceContentId: '', audioUploadId: null })
const form = reactive<PostInput>(empty())
let saved = JSON.stringify(empty()), alive = true, generation = 0, pending: AbortController | null = null
const savedSnapshot = ref(saved)
const dirty = computed(() => JSON.stringify(form) !== savedSnapshot.value)
function apply(post: UGCPost | null) {
  if (current.value?.contentId !== post?.contentId) audioStatus.value = ''
  current.value = post
  Object.assign(form, post ? { kind: post.kind, title: post.title, body: post.body, visibility: post.visibility, mediaSourceContentId: post.mediaSourceContentId || '', audioUploadId: post.audioUploadId || null } : empty())
  saved = JSON.stringify(form); savedSnapshot.value = saved; confirmDelete.value = false
}
function newDraft() { if (dirty.value && !window.confirm('当前修改尚未保存，确定新建草稿？')) return; generation += 1; pending?.abort(); apply(null); error.value = ''; notice.value = '' }
async function loadList() { try { const value = await ugcApi.list(); if (alive) posts.value = value.items.filter(post => post.status !== 'deleted') } catch { if (alive) error.value = '内容列表加载失败，请重试' } }
async function open(id: string) {
  if (dirty.value && !window.confirm('当前修改尚未保存，确定加载其他内容？')) return
  pending?.abort(); pending = new AbortController(); const version = ++generation
  error.value = ''; notice.value = ''; apply(null); busy.value = true
  try { const value = await ugcApi.detail(id, pending.signal); if (alive && version === generation) { if (!value.canEdit) throw new Error('无权编辑此内容'); apply(value) } }
  catch (cause) { if (alive && version === generation) error.value = cause instanceof Error ? cause.message : '内容加载失败' }
  finally { if (alive && version === generation) busy.value = false }
}
function reload() { if (current.value) void open(current.value.contentId) }
async function mutate(action: () => Promise<UGCPost>, success: string) {
  if (busy.value) return
  const version = generation; busy.value = true; error.value = ''; notice.value = ''
  try { const value = await action(); if (!alive || version !== generation) return; apply(value.status === 'deleted' ? null : value); notice.value = success; await loadList() }
  catch (cause) { if (alive && version === generation) error.value = (cause as { status?: number }).status === 409 ? '内容已在其他窗口更新。当前输入已保留，请重新加载最新版本再编辑。' : cause instanceof Error ? cause.message : '操作失败，请重试' }
  finally { if (alive) busy.value = false }
}
async function save() {
  if (form.kind === 'video' && !form.mediaSourceContentId) { error.value = '请选择已入库的视频'; return }
  const value = { ...form, mediaSourceContentId: form.kind === 'video' ? form.mediaSourceContentId : null }
  const post = current.value
  await mutate(() => post ? ugcApi.update(post.contentId, { title: value.title, body: value.body, visibility: value.visibility, mediaSourceContentId: value.mediaSourceContentId, audioUploadId: value.kind === 'audio' ? value.audioUploadId : null, expectedVersion: post.version }) : ugcApi.create(value), '已保存')
}
async function checkAudio() {
  const post = current.value
  const version = generation
  if (!post?.audioUploadId) return
  try {
    const state = await ugcApi.uploadStatus(post.audioUploadId)
    if (current.value?.contentId !== post.contentId || !alive) return
    audioStatus.value = ({ awaiting: '等待上传', queued: '等待处理', processing: '正在校验音频', ready: '音频已就绪，可以发布', failed: '音频校验失败，请重新上传', expired: '上传已过期，请重新上传' })[state.status]
    if (state.status === 'ready' && !dirty.value) {
      const fresh = await ugcApi.detail(post.contentId)
      if (alive && version === generation && current.value?.contentId === post.contentId && !dirty.value) apply(fresh)
    }
  } catch { audioStatus.value = '暂时无法获取音频状态，请重试' }
}
async function uploadAudio(event: Event) {
  const input = event.target as HTMLInputElement, file = input.files?.[0]
  if (!file || busy.value) return
  if (!file.size || file.size > 64 * 1024 * 1024) { error.value = '请选择不超过 64 MB 的音频文件'; return }
  const version = generation, controller = new AbortController(); pending?.abort(); pending = controller
  const deadline = setTimeout(() => controller.abort(), 240000)
  busy.value = true; error.value = ''; audioStatus.value = '正在准备音频…'
  try {
    let post = current.value
    if (!post) post = await ugcApi.create({ ...form, mediaSourceContentId: null, audioUploadId: null })
    else if (dirty.value) post = await ugcApi.update(post.contentId, { title: form.title, body: form.body, visibility: form.visibility, audioUploadId: form.audioUploadId, expectedVersion: post.version })
    if (!alive || version !== generation) return
    apply(post)
    const digest = await crypto.subtle.digest('SHA-256', await file.arrayBuffer())
    if (!alive || version !== generation || controller.signal.aborted) return
    const sha = Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, '0')).join('')
    const mime = file.type || ({ wav: 'audio/wav', flac: 'audio/flac', m4a: 'audio/mp4', mp3: 'audio/mpeg', ogg: 'audio/ogg', aac: 'audio/aac' } as Record<string,string>)[file.name.split('.').pop()?.toLowerCase() || ''] || 'application/octet-stream'
    const session = await ugcApi.initiateUpload(post.contentId, file.size, sha, mime)
    if (!alive || version !== generation || controller.signal.aborted) return
    if (!session.upload) throw new Error('无法创建上传会话')
    post = await ugcApi.update(post.contentId, { audioUploadId: session.uploadId, expectedVersion: post.version })
    if (!alive || version !== generation) return
    apply(post); audioStatus.value = '正在上传音频…'
    const body = new FormData()
    for (const [key, value] of Object.entries(session.upload.fields)) body.append(key, value)
    body.append('file', file)
    const response = await fetch(session.upload.url, { method: 'POST', body, credentials: 'omit', signal: controller.signal })
    if (!response.ok) throw new Error('音频上传失败，请重新选择文件')
    await ugcApi.completeUpload(session.uploadId)
    for (let attempt = 0; attempt < 90; attempt++) {
      if (!alive || version !== generation || controller.signal.aborted) return
      const state = await ugcApi.uploadStatus(session.uploadId, controller.signal)
      if (state.status === 'ready') { const fresh = await ugcApi.detail(post.contentId); if (!alive || version !== generation) return; apply(fresh); audioStatus.value = '音频已就绪，可以发布'; await loadList(); return }
      if (['failed', 'expired'].includes(state.status)) throw new Error('音频校验失败，请重新上传')
      audioStatus.value = state.status === 'processing' ? '正在校验处理音频…' : '音频已上传，等待处理…'
      await new Promise(resolve => setTimeout(resolve, 1500))
    }
    audioStatus.value = '音频仍在后台处理，可稍后刷新状态'
  } catch (cause) { if (alive && version === generation) error.value = cause instanceof Error ? cause.message : '上传失败，请重试' }
  finally { clearTimeout(deadline); if (alive && version === generation) busy.value = false; input.value = '' }
}
function publish() { const post = current.value; if (post && !dirty.value) void mutate(() => ugcApi.publish(post), '已发布') }
function remove() { const post = current.value; if (post) void mutate(() => ugcApi.remove(post), '已删除') }
function beforeUnload(event: BeforeUnloadEvent) { if (dirty.value) { event.preventDefault(); event.returnValue = '' } }
watch(() => route.query.edit, id => { if (enabled.value && typeof id === 'string') void open(id) })
onMounted(async () => {
  window.addEventListener('beforeunload', beforeUnload)
  try {
    const capabilities = await ugcApi.capabilities(); if (!alive) return
    enabled.value = capabilities.ugcEnabled
    if (!enabled.value) { notice.value = '创作功能尚未开启'; return }
    await loadList()
    const media = await ugcApi.media(); if (alive) mediaCandidates.value = media.items
    if (alive && typeof route.query.edit === 'string') await open(route.query.edit)
  } catch { if (alive) error.value = '创作服务暂不可用，请稍后刷新' }
})
onBeforeUnmount(() => { alive = false; generation += 1; pending?.abort(); window.removeEventListener('beforeunload', beforeUnload) })
</script>
<style scoped>
.studio-page{padding:28px;max-width:1200px;margin:auto}.studio-page header{display:flex;align-items:center;justify-content:space-between;gap:16px}h1{margin:0}h2{font-size:20px}header p,.muted,small{color:var(--color-text-secondary)}.studio-layout{display:grid;grid-template-columns:250px minmax(0,1fr);gap:24px;margin-top:24px}aside{display:flex;flex-direction:column;gap:8px}.post-row{text-align:left;display:grid;gap:6px}.post-row.selected{border-color:var(--color-accent)}.editor{display:grid;gap:14px}label{display:grid;gap:7px}button,input,textarea,select{padding:10px 12px;border:1px solid var(--color-border);border-radius:9px;background:var(--color-bg-sidebar);color:inherit;font:inherit}textarea{resize:vertical;min-height:160px}button{cursor:pointer}button:disabled{opacity:.5;cursor:default}.primary{background:var(--color-accent);color:#fff}.actions,.delete-row{display:flex;flex-wrap:wrap;align-items:center;gap:10px}.delete-row{margin-top:18px}.error{color:#bd3535}.actions a{color:var(--color-accent)}@media(max-width:700px){.studio-page{padding:16px}.studio-layout{grid-template-columns:1fr}aside{max-height:180px;overflow:auto}}
</style>
