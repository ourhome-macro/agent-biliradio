import { apiRequest } from './client'
import type { Track } from '@/types'

export type Reaction = 'like' | 'dislike' | 'neutral'
export interface FeedItem {
  itemId: string; contentId: string; rank: number; suppressed: boolean
  track?: Track; assetStatus?: string; reaction?: Reaction; relationVersion?: number
  counts?: { likes: number; dislikes: number; version: number }; decisionTraceId?: string
  coverUrl?: string
}
export interface FeedPage {
  sessionId: string; revision: string; mode: 'personal' | 'new'; items: FeedItem[]
  nextCursor: string; hasMore: boolean; supplyState: string; expiresAt: number
}
export interface Playback {
  playbackId: string; assetId: string; mode: 'cold' | 'stored'; status: string
  manifestUrl: string | null; expiresAt: number; sourceComplete: boolean; generation: number
  errorType?: string
}
export interface FeedEvent {
  eventId: string; itemId: string; type: string; playbackId?: string
  watchMs?: number; visibleRatio?: number; visibleMs?: number; errorType?: string
}
const json = (method: string, body: unknown, key?: string, signal?: AbortSignal): RequestInit => ({
  method, signal, headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
  body: JSON.stringify(body),
})
export const feedApi = {
  create: (mode: 'personal' | 'new', requestText: string, signal?: AbortSignal) =>
    apiRequest<FeedPage>('/api/feed/sessions', json('POST', { mode, requestText }, crypto.randomUUID(), signal)),
  page: (sessionId: string, cursor: string, signal?: AbortSignal) =>
    apiRequest<FeedPage>(`/api/feed/sessions/${sessionId}/items?cursor=${encodeURIComponent(cursor)}`, { signal }),
  start: (contentId: string, key: string, signal?: AbortSignal) =>
    apiRequest<Playback>('/api/media/playbacks', json('POST', { contentId }, key, signal)),
  playback: (id: string) => apiRequest<Playback>(`/api/media/playbacks/${id}`),
  heartbeat: (id: string) => apiRequest<Playback>(`/api/media/playbacks/${id}/heartbeat`, json('POST', {})),
  release: (id: string) => apiRequest(`/api/media/playbacks/${id}`, { method: 'DELETE', keepalive: true }),
  events: (events: FeedEvent[]) => apiRequest('/api/feed/events', { ...json('POST', { events }), keepalive: true }),
  react: (item: FeedItem, state: Reaction) => apiRequest<{ state: Reaction; relationVersion: number }>(
    `/api/content/${item.contentId}/reaction`, json('PUT', { state, expectedVersion: item.relationVersion }, crypto.randomUUID())),
}
