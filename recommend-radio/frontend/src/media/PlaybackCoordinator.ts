let currentOwner: (() => void) | null = null
export function claimVideoPlayback(stop: () => void, pauseAudio: () => void): () => void {
  if (currentOwner && currentOwner !== stop) currentOwner()
  pauseAudio()
  currentOwner = stop
  return () => { if (currentOwner === stop) currentOwner = null }
}

export function claimAudioPlayback(): void {
  const stop = currentOwner
  currentOwner = null
  stop?.()
}
