import { apiRequest } from './client'
import type { UGCPost } from './ugc'

export type ContentSearchItem = Partial<UGCPost> & { contentId: string; title: string; source?: string; snippet?: string }
export interface ContentSearchPage { items: ContentSearchItem[]; nextCursor?: string | null; hasMore: boolean; query: string }
async function read<T>(path: string, signal?: AbortSignal) {
  const controller = new AbortController(), abort = () => controller.abort()
  if (signal?.aborted) abort()
  signal?.addEventListener('abort', abort, { once:true })
  const timer = setTimeout(abort, 10000)
  try { return await apiRequest<T>(path, { signal:controller.signal }) }
  finally { clearTimeout(timer); signal?.removeEventListener('abort', abort) }
}
export const contentSearchApi = {
  search: (q: string, cursor?: string | null, signal?: AbortSignal) => {
    const params = new URLSearchParams({ q, limit: '20' }); if (cursor) params.set('cursor', cursor)
    return read<ContentSearchPage>(`/api/content/search?${params}`, signal)
  },
  suggest: (q: string, signal?: AbortSignal) => read<{ items: ContentSearchItem[] }>(`/api/content/suggest?${new URLSearchParams({ q, limit: '6' })}`, signal),
}
