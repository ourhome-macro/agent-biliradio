import { feedApi, type FeedEvent } from '@/api/feed'

/** Per-view ordered delivery: retries retain IDs and never overtake play_started. */
export class FeedEventQueue {
  private queue: { event: FeedEvent; attempts: number }[] = []
  private running = false
  private inFlightCount = 0
  private isolate = false
  private timer: ReturnType<typeof setTimeout> | undefined
  enqueue(event: FeedEvent) {
    // Bound memory under a prolonged outage; retain transition events preferentially.
    if (this.queue.length >= 256) {
      const progress = this.queue.findIndex((row, index) => index >= this.inFlightCount && row.event.type === 'watch_progress')
      if (progress >= 0) this.queue.splice(progress, 1)
      else return
    }
    this.queue.push({ event: structuredClone(event), attempts: 0 })
    void this.flush()
  }
  async flush() {
    if (this.running || !this.queue.length) return
    clearTimeout(this.timer)
    this.running = true
    const batch = this.queue.slice(0, this.isolate ? 1 : 20)
    this.inFlightCount = batch.length
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 5000)
    try {
      await feedApi.events(batch.map(row => row.event), controller.signal)
      this.queue.splice(0, batch.length)
    } catch (error) {
      const status = (error as { status?: number }).status
      const permanent = status !== undefined && status >= 400 && status < 500 && ![408, 429].includes(status)
      // The endpoint commits a batch atomically. Isolate a rejected batch before
      // discarding anything: its invalid member need not be the first member.
      if (status === 401 || status === 403) this.queue = []
      else if (permanent && batch.length > 1) this.isolate = true
      else if (permanent) this.queue.shift()
      else {
        batch.forEach(row => { row.attempts += 1 })
        if (batch[0].attempts >= 6) this.queue.shift()
      }
      this.running = false
      this.inFlightCount = 0
      if (this.queue.length) this.timer = setTimeout(() => void this.flush(), permanent ? 0 : Math.min(1000 * 2 ** batch[0].attempts, 15000))
      else this.isolate = false
      return
    } finally { clearTimeout(timeout) }
    this.running = false
    this.inFlightCount = 0
    if (this.queue.length) this.timer = setTimeout(() => void this.flush(), 0)
    else this.isolate = false
  }
  close() {
    void this.flush()
  }
  get pending() { return this.queue.length }
}
