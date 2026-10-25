import { apiRequest } from './client'
import { contentChanged } from '@/content/contentChanges'

export interface UGCPost {
  id: string; contentId: string; version: number; permissionVersion: number
  kind: 'text' | 'video' | 'audio'; status: 'draft' | 'published' | 'deleted'; visibility: 'public' | 'private'
  title: string; body: string; author: { id: string; name: string }; ownerId?: string; isOwner?: boolean
  mediaSourceContentId: string | null; assetId: string | null; createdAt: number; updatedAt: number
  audioUploadId?: string | null
  publishedAt: number | null; sharePath: string | null; canEdit: boolean
}
export interface PostInput { kind: 'text' | 'video' | 'audio'; title: string; body: string; visibility: 'public' | 'private'; mediaSourceContentId?: string | null; audioUploadId?: string | null }
export interface AudioUpload { uploadId: string; contentId: string; status: 'awaiting' | 'queued' | 'processing' | 'ready' | 'failed' | 'expired'; expiresAt: number; errorType?: string; upload?: { url: string; fields: Record<string, string>; method: string } }
export interface MediaCandidate { contentId: string; title: string; assetId: string; visibility: string }
export interface ContentCapabilities { ugcEnabled: boolean; contentSearchEnabled: boolean }
async function bounded<T>(path: string, init: RequestInit = {}) {
  const controller = new AbortController(), parent = init.signal
  const abort = () => controller.abort()
  if (parent?.aborted) abort()
  parent?.addEventListener('abort', abort, { once: true })
  const timer = setTimeout(abort, 12000)
  try { return await apiRequest<T>(path, { ...init, signal: controller.signal }) }
  finally { clearTimeout(timer); parent?.removeEventListener('abort', abort) }
}
async function command(path: string, method: string, body: unknown, key: string) {
  const init = { method, headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) }
  let result: UGCPost
  try { result = await bounded<UGCPost>(path, init) }
  catch (cause) {
    const status = (cause as { status?: number }).status
    if (status && status < 500) throw cause
    result = await bounded<UGCPost>(path, init)
  }
  contentChanged()
  return result
}
export const ugcApi = {
  initiateUpload: (id: string, bytes: number, sha256: string, contentType: string) => bounded<AudioUpload>(`/api/ugc/posts/${encodeURIComponent(id)}/uploads`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ bytes, sha256, contentType }),
  }),
  completeUpload: (id: string) => bounded<AudioUpload>(`/api/ugc/uploads/${id}/complete`, { method: 'POST' }),
  uploadStatus: (id: string, signal?: AbortSignal) => bounded<AudioUpload>(`/api/ugc/uploads/${id}`, { signal }),
  capabilities: (signal?: AbortSignal) => bounded<ContentCapabilities>('/api/content/capabilities', { signal }),
  list: (signal?: AbortSignal) => bounded<{ items: UGCPost[] }>('/api/ugc/posts', { signal }),
  detail: (id: string, signal?: AbortSignal) => bounded<UGCPost>(`/api/ugc/posts/${encodeURIComponent(id)}`, { signal }),
  media: (signal?: AbortSignal) => bounded<{ items: MediaCandidate[] }>('/api/ugc/media-candidates', { signal }),
  create: (input: PostInput, key = crypto.randomUUID()) => command('/api/ugc/posts', 'POST', input, key),
  update: (id: string, input: Partial<Omit<PostInput, 'kind'>> & { expectedVersion: number }, key = crypto.randomUUID()) => command(`/api/ugc/posts/${encodeURIComponent(id)}`, 'PATCH', input, key),
  publish: (post: UGCPost, key = crypto.randomUUID()) => command(`/api/ugc/posts/${encodeURIComponent(post.contentId)}/publish`, 'POST', { expectedVersion: post.version }, key),
  remove: (post: UGCPost, key = crypto.randomUUID()) => command(`/api/ugc/posts/${encodeURIComponent(post.contentId)}`, 'DELETE', { expectedVersion: post.version }, key),
}
