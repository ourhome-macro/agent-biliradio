import { apiRequest } from './client'
import type { Track } from '@/types'

export type Reaction = 'like' | 'dislike' | 'neutral'
export type FeedMode = 'personal' | 'new' | 'following'
export interface FollowingState {
  reason: 'no_follows' | 'syncing' | 'no_music' | 'available' | 'account_changed' | 'login_required' | 'upstream_error'
  snapshotVersion: number; hasUpdates: boolean; lastSyncedAt: number | null
  errorType?: string | null
}
export interface FeedItem {
  itemId: string; contentId: string; rank: number; suppressed: boolean
  track?: Track; assetStatus?: string; reaction?: Reaction; relationVersion?: number
  counts?: { likes: number; dislikes: number; version: number }; decisionTraceId?: string
  coverUrl?: string
}
export interface FeedPage {
  sessionId: string; revision: string; mode: FeedMode; items: FeedItem[]
  nextCursor: string; hasMore: boolean; supplyState: string; expiresAt: number
  followingState?: FollowingState
}
export interface Playback {
  playbackId: string; assetId: string; mode: 'cold' | 'stored'; status: string
  manifestUrl: string | null; expiresAt: number; sourceComplete: boolean; generation: number
  leaseExpiresAt?: number; manifestExpiresAt?: number | null; refreshAfterSeconds?: number
  generationKey?: string; durationSeconds?: number; seekable?: boolean
  errorType?: string
}
export interface FeedEvent {
  eventId: string; itemId: string; type: string; playbackId?: string
  watchMs?: number; visibleRatio?: number; visibleMs?: number; errorType?: string
  bufferingMs?: number; startupMs?: number; positionMs?: number; reason?: string
}
const json = (method: string, body: unknown, key?: string, signal?: AbortSignal): RequestInit => ({
  method, signal, headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
  body: JSON.stringify(body),
})
export const feedApi = {
  create: (mode: FeedMode, requestText: string, signal?: AbortSignal) =>
    apiRequest<FeedPage>('/api/feed/sessions', json('POST', { mode, requestText }, crypto.randomUUID(), signal)),
  page: (sessionId: string, cursor: string, signal?: AbortSignal) =>
    apiRequest<FeedPage>(`/api/feed/sessions/${sessionId}/items?cursor=${encodeURIComponent(cursor)}`, { signal }),
  start: (contentId: string, key: string, signal?: AbortSignal) =>
    apiRequest<Playback>('/api/media/playbacks', json('POST', { contentId }, key, signal)),
  playback: (id: string, signal?: AbortSignal) => apiRequest<Playback>(`/api/media/playbacks/${id}`, { signal }),
  heartbeat: (id: string, signal?: AbortSignal) => apiRequest<Playback>(`/api/media/playbacks/${id}/heartbeat`, json('POST', {}, undefined, signal)),
  release: (id: string) => apiRequest(`/api/media/playbacks/${id}`, { method: 'DELETE', keepalive: true }),
  events: (events: FeedEvent[], signal?: AbortSignal) => apiRequest('/api/feed/events', { ...json('POST', { events }, undefined, signal), keepalive: true }),
  react: (item: FeedItem, state: Reaction) => apiRequest<{ state: Reaction; relationVersion: number }>(
    `/api/content/${item.contentId}/reaction`, json('PUT', { state, expectedVersion: item.relationVersion }, crypto.randomUUID())),
}
