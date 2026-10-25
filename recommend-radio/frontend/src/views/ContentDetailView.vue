<template>
  <main class="content-detail">
    <RouterLink to="/content-search">← 内容搜索</RouterLink>
    <p v-if="loading" role="status">正在读取内容…</p><p v-if="error" role="alert">{{ error }}</p>
    <article v-if="post">
      <header><span class="badge">{{ post.kind === 'audio' ? '音频分享' : post.kind === 'video' ? '视频分享' : '文字分享' }} · {{ post.status === 'draft' ? '草稿' : post.visibility === 'private' ? '仅自己' : '公开' }}</span><h1>{{ post.title }}</h1><p>{{ post.author.name || '创作者' }}</p></header>
      <UGCVideo v-if="['video', 'audio'].includes(post.kind) && post.status === 'published'" :key="`${post.contentId}:${post.permissionVersion}`" :content-id="post.contentId" />
      <p v-else-if="post.kind === 'video'" class="hint">发布后可以从详情页观看引用的视频。</p>
      <div class="body">{{ post.body }}</div>
      <footer><RouterLink v-if="post.canEdit" :to="{ path: '/studio', query: { edit: post.contentId } }">编辑内容</RouterLink><button v-if="post.sharePath && post.visibility === 'public' && post.status === 'published'" @click="share">分享</button><span v-else>此内容没有公开分享入口</span></footer>
      <p v-if="shareMessage" role="status">{{ shareMessage }}</p><input v-if="shareUrl" readonly :value="shareUrl" aria-label="分享链接" @focus="($event.target as HTMLInputElement).select()" />
    </article>
    <button v-if="error" @click="load">重新读取</button>
  </main>
</template>
<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { ugcApi, type UGCPost } from '@/api/ugc'
import { contentRevision } from '@/content/contentChanges'
import UGCVideo from '@/components/content/UGCVideo.vue'
const route = useRoute(), post = ref<UGCPost | null>(null), loading = ref(false), error = ref(''), shareUrl = ref(''), shareMessage = ref('')
let generation = 0, alive = true, controller: AbortController | null = null
async function load() {
  controller?.abort(); controller = new AbortController(); const version = ++generation
  post.value = null; shareUrl.value = ''; shareMessage.value = ''; error.value = ''; loading.value = true
  try {
    const value = await ugcApi.detail(String(route.params.id), controller.signal)
    if (alive && version === generation) { if (value.status === 'deleted') throw new Error('内容已删除'); post.value = value }
  } catch (cause) { if (alive && version === generation) error.value = [403, 404, 410].includes((cause as { status?: number }).status || 0) ? '内容不存在，或当前账号没有查看权限。' : '内容暂时无法读取，请重试。' }
  finally { if (version === generation) loading.value = false }
}
async function share() {
  const current = post.value, version = generation
  if (!current?.sharePath || current.visibility !== 'public') return
  // Reauthorize immediately before revealing a URL: an old detail response does
  // not authorize sharing after another tab changed visibility.
  try {
    const fresh = await ugcApi.detail(current.contentId)
    if (!alive || version !== generation) return
    if (!fresh.sharePath || fresh.visibility !== 'public' || fresh.status !== 'published') { await load(); return }
    const url = new URL(fresh.sharePath, window.location.origin).href
    shareUrl.value = url
    try { await navigator.clipboard.writeText(url); shareMessage.value = '分享链接已复制' } catch { shareMessage.value = '可复制下方链接分享' }
  } catch { if (alive && version === generation) await load() }
}
function visible() { if (!document.hidden) void load() }
watch(() => route.params.id, load, { immediate: true })
watch(contentRevision, load)
onMounted(() => { window.addEventListener('focus', visible); document.addEventListener('visibilitychange', visible) })
onBeforeUnmount(() => { alive = false; generation += 1; controller?.abort(); window.removeEventListener('focus', visible); document.removeEventListener('visibilitychange', visible) })
</script>
<style scoped>
.content-detail{max-width:900px;margin:auto;padding:32px 28px}.content-detail>a,footer a{color:var(--color-accent)}article{margin-top:28px}h1{font-size:32px;line-height:1.35;overflow-wrap:anywhere}.badge,header p,.hint,footer span{font-size:13px;color:var(--color-text-secondary)}.body{white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.9;font-size:16px;margin:26px 0}footer{border-top:1px solid var(--color-border);padding-top:20px;display:flex;align-items:center;gap:18px}button,input{padding:9px 12px;border:1px solid var(--color-border);border-radius:9px;background:var(--color-bg-sidebar);color:inherit}input{width:100%}@media(max-width:600px){.content-detail{padding:20px 16px}h1{font-size:26px}}
</style>
