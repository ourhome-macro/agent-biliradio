import { defineStore } from 'pinia'
import { ref } from 'vue'
import { feedApi, type FeedItem, type FeedPage, type FeedMode, type FollowingState } from '@/api/feed'
import { apiRequest } from '@/api/client'

const SUPPLY_DELAYS = [2000, 4000, 8000, 12000, 15000, 15000]
const isHidden = () => typeof document !== 'undefined' && document.hidden
const statusOf = (error: unknown) => typeof error === 'object' && error !== null && 'status' in error
  ? Number(error.status) : 0

export const useFeedStore = defineStore('videoFeed', () => {
  const items = ref<FeedItem[]>([])
  const sessionId = ref('')
  const mode = ref<FeedMode>('personal')
  const revision = ref('')
  const followingState = ref<FollowingState | undefined>()
  const requestText = ref('')
  const loading = ref(false)
  const error = ref('')
  const hasMore = ref(false)
  const supplyState = ref('')
  const nextCursor = ref('')
  const generation = ref(0)
  const expiresAt = ref(0)
  const renewalCount = ref(0)
  const supplyPolling = ref(false)
  const supplyPaused = ref(false)
  let pending: AbortController | null = null
  let timer: ReturnType<typeof setTimeout> | undefined
  let attempts = 0
  let supplyDemand = false
  let stopped = false
  let sessionMode: FeedMode = 'personal'
  let sessionText = ''

  function cancelTimer() {
    if (timer !== undefined) clearTimeout(timer)
    timer = undefined
    supplyPolling.value = false
  }

  function stop() {
    stopped = true
    generation.value += 1
    pending?.abort()
    pending = null
    loading.value = false
    supplyDemand = false
    cancelTimer()
  }

  function scheduleSupply() {
    cancelTimer()
    if (stopped || !supplyDemand || !hasMore.value || loading.value) return
    if (attempts >= SUPPLY_DELAYS.length) { supplyPaused.value = true; return }
    if (isHidden()) return
    const version = generation.value
    supplyPolling.value = true
    timer = setTimeout(() => {
      timer = undefined
      supplyPolling.value = false
      if (version !== generation.value || stopped || isHidden()) return
      attempts += 1
      void loadMore(true)
    }, SUPPLY_DELAYS[attempts])
  }

  function accept(page: FeedPage, append: boolean) {
    // Renewed sessions can return fresh item IDs for old content. Retain the old
    // item identity so active playback events still reference their issuing page.
    const ids = new Set(append ? items.value.map(item => item.contentId) : [])
    const additions = page.items.filter(item => {
      if (ids.has(item.contentId)) return false
      ids.add(item.contentId)
      return true
    })
    if (append) items.value.push(...additions)
    else items.value = additions
    sessionId.value = page.sessionId
    revision.value = page.revision
    followingState.value = page.followingState
    expiresAt.value = page.expiresAt
    nextCursor.value = page.nextCursor
    hasMore.value = page.hasMore
    supplyState.value = page.supplyState
    if (additions.some(item => !item.suppressed)) {
      attempts = 0
      supplyPaused.value = false
      supplyDemand = false
    } else if (hasMore.value) supplyDemand = true
    if (!hasMore.value) supplyDemand = false
  }

  async function startSession(create: (signal: AbortSignal) => Promise<FeedPage>) {
    stop()
    stopped = false
    attempts = 0
    supplyPaused.value = false
    renewalCount.value = 0
    const version = generation.value
    items.value = []
    error.value = ''
    sessionId.value = ''
    revision.value = ''
    followingState.value = undefined
    nextCursor.value = ''
    expiresAt.value = 0
    hasMore.value = false
    supplyState.value = ''
    const controller = pending = new AbortController()
    loading.value = true
    try {
      const page = await create(controller.signal)
      if (version !== generation.value) return
      accept(page, false)
    } catch (e) {
      if (version === generation.value && !controller.signal.aborted)
        error.value = e instanceof Error ? e.message : String(e)
    } finally {
      if (version === generation.value) {
        pending = null
        loading.value = false
        scheduleSupply()
      }
    }
  }

  async function reset() {
    const currentMode = mode.value, currentText = requestText.value
    sessionMode = currentMode
    sessionText = currentText
    await startSession(signal => feedApi.create(currentMode, currentText, signal))
  }

  async function openContent(contentId: string) {
    await startSession(signal => apiRequest<FeedPage>(
      '/api/feed/content/' + encodeURIComponent(contentId) + '/open', { method: 'POST', signal }))
  }

  async function followingChanged(externalId: string, following: boolean) {
    if (mode.value !== 'following') return
    if (!following) {
      for (const item of items.value)
        if (String(item.track?.ownerMid || '') === String(externalId)) item.suppressed = true
    }
    // Reset synchronously aborts the old page and increments generation before
    // awaiting network I/O, so an in-flight page cannot restore an unfollowed UP.
    await startSession(signal => feedApi.create(sessionMode, sessionText, signal))
  }

  async function loadMore(automatic = false) {
    if (stopped || loading.value || !hasMore.value || !sessionId.value) return
    cancelTimer()
    const version = generation.value
    const controller = pending = new AbortController()
    const currentMode = sessionMode, currentText = sessionText
    loading.value = true
    error.value = ''
    try {
      let page: FeedPage
      const renew = async () => {
        const fresh = await feedApi.create(currentMode, currentText, controller.signal)
        if (version === generation.value) renewalCount.value += 1
        return fresh
      }
      if (expiresAt.value > 0 && expiresAt.value * 1000 <= Date.now()) page = await renew()
      else {
        try { page = await feedApi.page(sessionId.value, nextCursor.value, controller.signal) }
        catch (e) {
          if (statusOf(e) !== 410) throw e
          page = await renew()
        }
      }
      if (version !== generation.value) return
      accept(page, true)
    } catch (e) {
      if (version === generation.value && !controller.signal.aborted) {
        error.value = e instanceof Error ? e.message : String(e)
        const status = statusOf(e)
        supplyDemand = status === 0 || status === 429 || status >= 500
        if (!automatic) attempts = Math.max(attempts, 1)
      }
    } finally {
      if (version === generation.value) {
        pending = null
        loading.value = false
        scheduleSupply()
      }
    }
  }

  async function more() {
    // Tail scroll callbacks must not bypass the bounded replenishment backoff.
    if (supplyPolling.value || supplyPaused.value) return
    await loadMore()
  }

  async function retrySupply() {
    if (stopped || loading.value) return
    attempts = 0
    supplyPaused.value = false
    cancelTimer()
    if (!sessionId.value) await reset()
    else await loadMore()
  }

  function resumeSupply() {
    if (isHidden()) { cancelTimer(); return }
    scheduleSupply()
  }

  return {
    items, sessionId, mode, requestText, loading, error, hasMore, supplyState, revision, followingState,
    generation, expiresAt, renewalCount, supplyPolling, supplyPaused,
    reset, more, stop, retrySupply, resumeSupply, openContent, followingChanged,
  }
})
