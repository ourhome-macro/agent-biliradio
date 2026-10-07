import Hls from 'hls.js'
import { apiUrl } from '@/api/client'

export class VideoPlayerAdapter {
  private hls: Hls | null = null
  private video: HTMLVideoElement | null = null
  private generation = 0
  private cleanupNative: (() => void) | null = null
  attach(video: HTMLVideoElement, manifest: string, onError: (type: string) => void, onBlocked: () => void,
    options: { position?: number; autoplay?: boolean } = {}) {
    this.stop()
    this.video = video
    const version = this.generation
    const play = () => {
      if (version !== this.generation) return
      if (options.position && Number.isFinite(options.position)) {
        try { video.currentTime = options.position } catch { /* Wait for metadata. */ }
      }
      if (options.autoplay === false) return
      void video.play().catch(() => { if (version === this.generation) onBlocked() })
    }
    if (Hls.isSupported()) {
      const apiOrigin = new URL(apiUrl('/'), window.location.href).origin
      this.hls = new Hls({
        maxBufferLength: 16, backBufferLength: 8, maxMaxBufferLength: 24,
        startPosition: options.position || 0,
        xhrSetup(xhr, url) { xhr.withCredentials = new URL(url, window.location.href).origin === apiOrigin },
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
      this.cleanupNative = () => { video.removeEventListener('loadedmetadata', play); video.removeEventListener('error', error) }
      video.src = apiUrl(manifest); video.load()
    } else onError('unsupported_browser')
  }
  stop() {
    this.generation += 1
    this.cleanupNative?.(); this.cleanupNative = null
    this.hls?.destroy(); this.hls = null
    if (this.video) { this.video.pause(); this.video.removeAttribute('src'); this.video.load() }
    this.video = null
  }
}
