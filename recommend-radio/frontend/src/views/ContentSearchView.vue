<template>
  <main class="content-search">
    <header><h1>内容搜索</h1><p>查找平台里的音乐分享与视频。</p><RouterLink to="/search">搜索 B 站上游视频 →</RouterLink></header>
    <form class="search-form" @submit.prevent="search(false)">
      <div class="suggestion-input"><input v-model="query" maxlength="200" autocomplete="off" placeholder="输入标题或内容关键词" aria-label="平台内容关键词" role="combobox" aria-autocomplete="list" :aria-expanded="suggestions.length > 0" aria-controls="content-suggestions" :aria-activedescendant="activeSuggestion >= 0 ? `suggestion-${activeSuggestion}` : undefined" @keydown="keys" />
        <ul v-if="suggestions.length" id="content-suggestions" role="listbox"><li v-for="(item,index) in suggestions" :id="`suggestion-${index}`" :key="item.contentId" role="option" :aria-selected="activeSuggestion === index"><button type="button" @click="open(item)">{{ item.title }}</button></li></ul>
      </div><button :disabled="loading || !enabled || !query.trim()">搜索</button>
    </form>
    <p v-if="error" role="alert">{{ error }}</p><p v-if="loading" role="status">正在查询…</p><p v-if="searched && !loading && !items.length && !error">没有找到当前可查看的内容。</p>
    <section class="results" aria-label="内容搜索结果"><article v-for="item in items" :key="item.contentId"><button class="result-title" @click="open(item)">{{ item.title }}</button><p>{{ item.snippet || item.body?.slice(0,180) || '' }}</p><small>{{ item.author?.name || (item.source === 'bili' ? 'B 站视频' : '平台分享') }}{{ item.visibility === 'private' ? ' · 仅自己' : '' }}</small></article></section>
    <button v-if="hasMore" :disabled="loading" @click="search(true)">加载更多</button>
  </main>
</template>
<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ugcApi } from '@/api/ugc'
import { contentSearchApi, type ContentSearchItem } from '@/api/contentSearch'
import { contentRevision } from '@/content/contentChanges'
const route = useRoute(), router = useRouter()
const query = ref(typeof route.query.q === 'string' ? route.query.q : ''), items = ref<ContentSearchItem[]>([]), suggestions = ref<ContentSearchItem[]>([])
const enabled = ref(false), loading = ref(false), error = ref(''), hasMore = ref(false), searched = ref(false), activeSuggestion = ref(-1)
let alive = true, generation = 0, suggestGeneration = 0, searchController: AbortController | null = null, suggestController: AbortController | null = null
let timer: ReturnType<typeof setTimeout> | undefined, cursor: string | null = null, committedQuery = ''
function clearRequests() { generation += 1; suggestGeneration += 1; searchController?.abort(); suggestController?.abort(); clearTimeout(timer); loading.value = false; suggestions.value = []; activeSuggestion.value = -1 }
async function search(append: boolean): Promise<void> {
  if (!enabled.value || !query.value.trim() || append && loading.value) return
  if (!append) { clearRequests(); items.value = []; cursor = null; hasMore.value = false; committedQuery = query.value.trim() }
  if (append && committedQuery !== query.value.trim()) return search(false)
  const version = ++generation, controller = searchController = new AbortController()
  loading.value = true; error.value = ''; searched.value = true; suggestions.value = []
  try {
    const value = await contentSearchApi.search(committedQuery, append ? cursor : null, controller.signal)
    if (!alive || version !== generation) return
    const old = new Set(append ? items.value.map(item => item.contentId) : [])
    const next = value.items.filter(item => !old.has(item.contentId))
    items.value = append ? [...items.value, ...next] : next; cursor = value.nextCursor || null; hasMore.value = value.hasMore
  } catch (cause) {
    if (alive && version === generation) {
      const status = (cause as {status?:number}).status || 0
      if (status === 410 && append) { await search(false); return }
      error.value = [401,403].includes(status) ? '请登录后查询可查看的内容。' : '内容搜索暂不可用，请稍后重试。'
    }
  }
  finally { if (version === generation) loading.value = false }
}
function suggest() {
  suggestGeneration += 1; suggestController?.abort(); clearTimeout(timer); suggestions.value = []; activeSuggestion.value = -1
  if (!enabled.value || query.value.trim().length < 2) return
  const version = suggestGeneration, text = query.value.trim()
  timer = setTimeout(async () => {
    const controller = suggestController = new AbortController()
    try { const value = await contentSearchApi.suggest(text, controller.signal); if (alive && version === suggestGeneration && query.value.trim() === text) suggestions.value = value.items }
    catch { /* A suggestion failure does not replace explicit search results. */ }
  }, 250)
}
function open(item: ContentSearchItem) {
  clearRequests()
  void router.push(item.source === 'bili' ? { path:'/feed', query:{ content:item.contentId } } : { path:`/content/${encodeURIComponent(item.contentId)}` })
}
function keys(event: KeyboardEvent) {
  if (event.key === 'Escape') { suggestGeneration += 1; suggestController?.abort(); suggestions.value = []; activeSuggestion.value = -1 }
  else if (event.key === 'ArrowDown' && suggestions.value.length) { event.preventDefault(); activeSuggestion.value = Math.min(activeSuggestion.value + 1, suggestions.value.length - 1) }
  else if (event.key === 'ArrowUp' && suggestions.value.length) { event.preventDefault(); activeSuggestion.value = Math.max(0, activeSuggestion.value - 1) }
  else if (event.key === 'Enter' && activeSuggestion.value >= 0 && suggestions.value[activeSuggestion.value]) { event.preventDefault(); open(suggestions.value[activeSuggestion.value]) }
}
function invalidate() { clearRequests(); items.value = []; hasMore.value = false; cursor = null; if (query.value.trim()) void search(false) }
function visible() { if (!document.hidden && searched.value) invalidate() }
watch(query, () => { generation += 1; searchController?.abort(); loading.value = false; items.value = []; hasMore.value = false; searched.value = false; suggest() })
watch(contentRevision, invalidate)
onMounted(async () => {
  window.addEventListener('focus', visible); document.addEventListener('visibilitychange', visible)
  try { const value = await ugcApi.capabilities(); if (!alive) return; enabled.value = value.contentSearchEnabled; if (!enabled.value) error.value = '内容搜索尚未开启'; else if (query.value.trim()) await search(false) }
  catch { if (alive) error.value = '内容搜索暂不可用' }
})
onBeforeUnmount(() => { alive = false; clearRequests(); window.removeEventListener('focus', visible); document.removeEventListener('visibilitychange', visible) })
</script>
<style scoped>
.content-search{max-width:1000px;margin:auto;padding:30px}h1{margin-top:0}header p,small{color:var(--color-text-secondary)}header a{color:var(--color-accent)}.search-form{display:flex;gap:12px;margin:24px 0}.suggestion-input{position:relative;flex:1;min-width:0}input{width:100%}button,input{padding:11px 14px;border:1px solid var(--color-border);border-radius:9px;background:var(--color-bg-sidebar);color:inherit;font:inherit}button{cursor:pointer}button:disabled{opacity:.5}.suggestion-input ul{position:absolute;z-index:10;top:100%;left:0;right:0;padding:5px;margin:5px 0;list-style:none;border:1px solid var(--color-border);border-radius:10px;background:var(--color-bg-app);box-shadow:0 8px 20px #0002}.suggestion-input li button{width:100%;text-align:left;border:0;background:transparent}.suggestion-input li[aria-selected=true]{background:var(--color-bg-sidebar)}.results{display:grid;gap:16px}.results article{padding:20px;border:1px solid var(--color-border);border-radius:12px}.result-title{padding:0;border:0;background:transparent;text-align:left;font-size:19px;font-weight:600}.results p{line-height:1.7;white-space:pre-wrap;overflow-wrap:anywhere}button:focus-visible,input:focus-visible{outline:2px solid var(--color-accent)}@media(max-width:600px){.content-search{padding:18px 14px}}
</style>
