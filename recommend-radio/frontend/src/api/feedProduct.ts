import { apiRequest } from './client'
import type { FeedItem, Reaction } from './feed'
import type { Track } from '@/types'

export interface ContentDetail {
  contentId: string; track: Track & { description?: string }; assetStatus: string
  reaction: Reaction; relationVersion: number; projectionPending: boolean
  counts: { likes: number; dislikes: number; version: number }; sharePath: string | null
}
export interface HistoryEntry { contentId: string; track: Track; watchMs: number; watchedAt: number; completed: boolean }
export const feedProductApi = {
  detail: (id: string) => apiRequest<ContentDetail>(`/api/content/${encodeURIComponent(id)}`),
  history: (cursor?: string) => apiRequest<{ items: HistoryEntry[]; nextCursor: string | null }>(
    `/api/feed/history${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ''}`),
  react: (item: FeedItem, state: Reaction, key: string) => apiRequest<{
    state: Reaction; relationVersion: number; counts: ContentDetail['counts']
  }>(`/api/content/${encodeURIComponent(item.contentId)}/reaction`, {
    method: 'PUT', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify({ state, expectedVersion: item.relationVersion ?? 0 }),
  }),
}
