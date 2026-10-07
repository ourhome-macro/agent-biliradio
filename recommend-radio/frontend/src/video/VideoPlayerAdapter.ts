import Hls from 'hls.js'
import { apiUrl } from '@/api/client'

export class VideoPlayerAdapter {
  private hls: Hls | null = null
  private video: HTMLVideoElement | null = null
  private generation = 0
  attach(video: HTMLVideoElement, manifest: string, onError: (type: string) => void, onBlocked: () => void) {
    this.stop()
    this.video = video
    const version = this.generation
    const play = () => {
      if (version !== this.generation) return
      void video.play().catch(() => { if (version === this.generation) onBlocked() })
    }
    if (Hls.isSupported()) {
      const apiOrigin = new URL(apiUrl('/'), window.location.href).origin
      this.hls = new Hls({
        maxBufferLength: 16, backBufferLength: 8, maxMaxBufferLength: 24,
        xhrSetup(xhr, url) { xhr.withCredentials = new URL(url, window.location.href).origin === apiOrigin },
      })
      this.hls.on(Hls.Events.MANIFEST_PARSED, play)
      this.hls.on(Hls.Events.ERROR, (_event, data) => {
        if (data.fatal && version === this.generation) onError(data.type)
      })
      this.hls.loadSource(apiUrl(manifest)); this.hls.attachMedia(video)
    } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
      video.src = apiUrl(manifest); play()
    } else onError('unsupported_browser')
  }
  stop() {
    this.generation += 1
    this.hls?.destroy(); this.hls = null
    if (this.video) { this.video.pause(); this.video.removeAttribute('src'); this.video.load() }
    this.video = null
  }
}
