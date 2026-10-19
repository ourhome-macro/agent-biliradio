import Hls from 'hls.js'
import { apiUrl } from '@/api/client'

export class VideoPlayerAdapter {
  private hls: Hls | null = null
  private video: HTMLVideoElement | null = null
  private generation = 0
  private cleanupNative: (() => void) | null = null
  private signedFiles = new Map<string, string>()
  private refreshing: AbortController | null = null
  attach(video: HTMLVideoElement, manifest: string, onError: (type: string) => void, onBlocked: () => void,
    options: { position?: number; autoplay?: boolean; shouldAutoplay?: () => boolean } = {}) {
    this.stop()
    this.video = video
    const version = this.generation
    let restored = !options.position
    const restorePosition = () => {
      if (version !== this.generation || restored || !video.readyState) return
      if (options.position && Number.isFinite(options.position)) {
        try { video.currentTime = options.position; restored = true } catch { /* Metadata may not be ready. */ }
      }
    }
    const play = () => {
      if (version !== this.generation) return
      restorePosition()
      if (options.autoplay === false || options.shouldAutoplay?.() === false) return
      void video.play().catch(() => { if (version === this.generation) onBlocked() })
    }
    video.addEventListener('loadedmetadata', restorePosition)
    this.cleanupNative = () => video.removeEventListener('loadedmetadata', restorePosition)
    if (Hls.isSupported()) {
      const apiOrigin = new URL(apiUrl('/'), window.location.href).origin
      const signedFiles = this.signedFiles
      this.hls = new Hls({
        maxBufferLength: 16, backBufferLength: 8, maxMaxBufferLength: 24,
        startPosition: options.position || 0,
        xhrSetup(xhr, url) {
          const name = new URL(url, window.location.href).pathname.split('/').pop() || ''
          const fresh = signedFiles.get(name) || url
          // Renew only future requests. Keep the existing MediaSource and buffers.
          if (fresh !== url) xhr.open('GET', fresh, true)
          xhr.withCredentials = new URL(fresh, window.location.href).origin === apiOrigin
        },
      })
      this.hls.on(Hls.Events.MANIFEST_PARSED, play)
      this.hls.on(Hls.Events.ERROR, (_event, data) => {
        if (data.fatal && version === this.generation) onError(data.type)
      })
      this.hls.loadSource(apiUrl(manifest)); this.hls.attachMedia(video)
    } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
      const error = () => { if (version === this.generation) onError('native_media_error') }
      video.addEventListener('loadedmetadata', play, { once: true })
      video.addEventListener('error', error)
      this.cleanupNative = () => { video.removeEventListener('loadedmetadata', restorePosition); video.removeEventListener('loadedmetadata', play); video.removeEventListener('error', error) }
      video.src = apiUrl(manifest); video.load()
    } else onError('unsupported_browser')
  }
  async refreshManifest(manifest: string): Promise<boolean> {
    if (!this.hls) return false
    const version = this.generation, controller = new AbortController()
    this.refreshing?.abort(); this.refreshing = controller
    const timer = window.setTimeout(() => controller.abort(), 3500)
    try {
      const url = new URL(apiUrl(manifest), window.location.href)
      const response = await fetch(url, { credentials: 'include', cache: 'no-store', signal: controller.signal })
      if (!response.ok) throw new Error(`manifest_refresh_${response.status}`)
      const text = await response.text()
      if (!text.startsWith('#EXTM3U')) throw new Error('invalid_manifest')
      const files = new Map<string, string>()
      for (const line of text.split(/\r?\n/)) {
        const reference = line.startsWith('#EXT-X-MAP:') ? /URI="([^"]+)"/.exec(line)?.[1]
          : line && !line.startsWith('#') ? line.trim() : undefined
        if (!reference) continue
        const target = new URL(reference, url)
        const name = target.pathname.split('/').pop() || ''
        if (/^(init\.mp4|seg_\d{6}\.m4s)$/.test(name) && ['http:', 'https:'].includes(target.protocol))
          files.set(name, target.href)
      }
      if (!files.size) throw new Error('empty_manifest')
      if (version !== this.generation) return true
      this.signedFiles.clear()
      for (const [name, target] of files) this.signedFiles.set(name, target)
      return true
    } finally {
      clearTimeout(timer)
      if (this.refreshing === controller) this.refreshing = null
    }
  }
  resumeLoading() { this.hls?.startLoad(-1) }
  stop() {
    this.generation += 1
    this.refreshing?.abort(); this.refreshing = null; this.signedFiles.clear()
    this.cleanupNative?.(); this.cleanupNative = null
    this.hls?.destroy(); this.hls = null
    if (this.video) { this.video.pause(); this.video.removeAttribute('src'); this.video.load() }
    this.video = null
  }
}
