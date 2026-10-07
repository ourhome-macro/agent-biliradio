import { apiRequest } from './client'

export interface CreatorFollow {
  creatorId: string; provider: string; externalId: string; name: string
  following: boolean; confirmedFollowing: boolean | null
  syncStatus: 'pending' | 'synced' | 'unknown' | 'failed' | 'unbound'
  relationVersion: number; accountMid: number | string | null; errorType: string | null; supported: boolean
}
export interface FollowingList {
  items: CreatorFollow[]; accountMid: string | number | null; relationRevision: number
  sync: { status: string; lastSyncedAt: number | null; errorType: string | null }
}
const command = (method: string, body: unknown, key: string): RequestInit => ({
  method, headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body),
})
async function request<T>(path: string, init: RequestInit = {}) {
  const controller = new AbortController(), external = init.signal
  const abort = () => controller.abort()
  if (external?.aborted) abort()
  external?.addEventListener('abort', abort, { once: true })
  const timer = setTimeout(abort, 8000)
  try { return await apiRequest<T>(path, { ...init, signal: controller.signal }) }
  finally { clearTimeout(timer); external?.removeEventListener('abort', abort) }
}
export const followingApi = {
  creator: (contentId: string, signal?: AbortSignal) => request<CreatorFollow>(`/api/content/${encodeURIComponent(contentId)}/creator`, { signal }),
  state: (creatorId: string, signal?: AbortSignal) => request<CreatorFollow>(`/api/creators/${encodeURIComponent(creatorId)}/follow`, { signal }),
  set: (creator: CreatorFollow, following: boolean, key: string) => request<CreatorFollow>(
    `/api/creators/${encodeURIComponent(creator.creatorId)}/follow`, command('PUT', { following, expectedVersion: creator.relationVersion }, key)),
  list: (signal?: AbortSignal) => request<FollowingList>('/api/following', { signal }),
  sync: (key: string) => request<{ syncRunId: string; status: string }>('/api/following/sync', command('POST', {}, key)),
}
