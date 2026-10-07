import { defineStore } from 'pinia'
import { ref } from 'vue'
import { feedApi, type FeedItem } from '@/api/feed'

export const useFeedStore = defineStore('videoFeed', () => {
  const items = ref<FeedItem[]>([])
  const sessionId = ref('')
  const mode = ref<'personal' | 'new'>('personal')
  const requestText = ref('')
  const loading = ref(false)
  const error = ref('')
  const hasMore = ref(false)
  const supplyState = ref('')
  const nextCursor = ref('')
  const generation = ref(0)
  let pending: AbortController | null = null
  function stop() { generation.value += 1; pending?.abort(); pending = null; loading.value = false }
  async function reset() {
    stop()
    const version = generation.value
    items.value = []; error.value = ''; sessionId.value = ''; hasMore.value = false
    const controller = pending = new AbortController()
    loading.value = true
    try {
      const page = await feedApi.create(mode.value, requestText.value, controller.signal)
      if (version !== generation.value) return
      sessionId.value = page.sessionId; items.value = page.items
      nextCursor.value = page.nextCursor; hasMore.value = page.hasMore; supplyState.value = page.supplyState
    } catch (e) {
      if (version === generation.value) error.value = e instanceof Error ? e.message : String(e)
    } finally { if (version === generation.value) loading.value = false }
  }
  async function more() {
    if (loading.value || !hasMore.value || !sessionId.value) return
    const version = generation.value
    const controller = pending = new AbortController()
    loading.value = true
    try {
      const page = await feedApi.page(sessionId.value, nextCursor.value, controller.signal)
      if (version !== generation.value) return
      const ids = new Set(items.value.map(i => i.itemId))
      items.value.push(...page.items.filter(i => !ids.has(i.itemId)))
      nextCursor.value = page.nextCursor; hasMore.value = page.hasMore; supplyState.value = page.supplyState
    } catch (e) {
      if (version === generation.value) error.value = e instanceof Error ? e.message : String(e)
    } finally { if (version === generation.value) loading.value = false }
  }
  return { items, sessionId, mode, requestText, loading, error, hasMore, supplyState, generation, reset, more, stop }
})
