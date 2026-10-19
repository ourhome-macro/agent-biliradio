import { ref } from 'vue'

// Payloads carry no title/body/private metadata. Other views must ask the API to
// authorize current material instead of restoring stale search or detail data.
export const contentRevision = ref(0)
const channel = typeof window === 'undefined' || typeof BroadcastChannel === 'undefined' ? null : new BroadcastChannel('radio-content-changed')
if (channel) channel.onmessage = () => { contentRevision.value += 1 }
export function contentChanged() {
  contentRevision.value += 1
  channel?.postMessage({ invalidate: true })
}
