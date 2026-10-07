export interface VideoInfo {
  trackId?: string
  bvid: string
  cid?: number | null
  title: string
  duration: number
  owner: string
  ownerMid?: number | null
  cover: string
  playCount?: number
  recentPlayCount?: number
  positionMs?: number
  listenMs?: number
  completed?: boolean
  publishedAt?: string | null
}

export interface AudioStreamInfo {
  url: string
  relativeUrl?: string
  duration: number
  bitrate: number
  sampleRate?: number
  sample_rate: number
  channels: number
  quality?: string
  actualQuality?: string
  codec?: string
  fallback?: boolean
  bvid?: string
  cid?: number
  availableAudioQualities?: AudioQualityPreference[]
}

export type AudioQualityPreference = 'auto' | '64k' | '132k' | '192k' | 'dolby' | 'hires'

/** 播放队列中的一条曲目，也用于最近播放、收藏、歌单 */
export interface Track {
  trackId?: string
  bvid: string
  cid?: number | null
  title: string
  owner: string
  ownerMid?: number | null
  cover: string
  duration: number
  playCount?: number
  recentPlayCount?: number
  positionMs?: number
  listenMs?: number
  completed?: boolean
  publishedAt?: string | null
  page?: number | null
  pageTitle?: string | null
  pageCount?: number | null
  isMultipart?: boolean | null
  source?: string
}

export interface BiliUserProfile {
  mid: number
  name: string
  face: string
  level?: number
  vipType?: number
}

export type AppUserRole = 'user' | 'admin'

export interface AppUser {
  id: string
  displayName: string
  role: AppUserRole
}

export interface AppSession {
  authenticated: boolean
  user: AppUser | null
  csrfToken: string | null
  oidcEnabled: boolean
  biliConnected: boolean
}

export interface AuthStatus {
  qrLoginEnabled: boolean
  isLoggedIn: boolean
  user: BiliUserProfile | null
  cookieUpdatedAt?: string | null
}

export interface AuthQrCode {
  qrcodeKey: string
  url: string
  expiresAt: string
  pollIntervalMs: number
}

export interface AuthQrStatus {
  qrcodeKey: string
  status: 'waiting' | 'scanned' | 'confirmed' | 'expired' | 'unknown'
  code: number
  message: string
  isLoggedIn: boolean
  user: BiliUserProfile | null
}

export interface AdminStatsSummary {
  range: string
  generatedAt: string
  monitoringUrl?: string | null
  users: {
    total: number
    active: number
    newUsers: number
    admins: number
  }
  traffic: {
    requests: number | null
    errorRate: number | null
    p95LatencyMs: number | null
  }
  playback: {
    plays: number
    skips: number
    listenSeconds: number
  }
}

export interface AdminUser {
  id: string
  displayName: string
  email?: string | null
  role: AppUserRole
  status: 'active' | 'disabled'
  createdAt: string
  lastLoginAt?: string | null
}

export interface AdminUsersPage {
  items: AdminUser[]
  total: number
  page: number
  pageSize: number
}

export interface AdminRoleToggleResult {
  id: string
  role: AppUserRole
}

export interface FavoriteFolder {
  mediaId: number
  id: number
  fid?: number | null
  mid: number
  title: string
  cover: string
  mediaCount: number
  attr: number
  favoriteState: number
}

export interface TrackCoverInfo {
  bvid: string
  cid?: number | null
  cover: string
  videoCover: string
  pageCover?: string | null
  ownerFace?: string
  pages: Array<{
    cid: number
    page: number
    pageTitle: string
    cover: string
    firstFrame?: string | null
  }>
}

export interface PlayerQueueSnapshot {
  queue: Track[]
  currentIndex: number
  playMode: PlayMode
  updatedAt?: string | null
}

export interface TrackIntro {
  bvid: string
  cid?: number | null
  title: string
  description: string
  dynamic: string
  owner: {
    mid: number
    name: string
    face: string
  }
  publishedAt?: string | null
  stats: {
    view: number
    danmaku: number
    reply: number
    favorite: number
    coin: number
    share: number
    like: number
  }
  pages: Array<{
    cid: number
    page: number
    title: string
    duration: number
  }>
}

export interface TrackSubtitleInfo {
  id: number
  lan: string
  lanDoc: string
  url: string
  authorMid: number
  type: number
}

export interface TrackSubtitleLine {
  from: number
  to: number
  text: string
}

export interface TrackSubtitles {
  bvid: string
  cid: number
  needLogin: boolean
  subtitles: TrackSubtitleInfo[]
  activeSubtitleId?: number | null
  lines: TrackSubtitleLine[]
}

export interface TrackChapter {
  from: number
  to: number
  title: string
  cover: string
}

export interface TrackChapters {
  bvid: string
  cid: number
  chapters: TrackChapter[]
}

export interface TrackComment {
  id: string
  author: {
    mid: number
    name: string
    avatar: string
  }
  message: string
  like: number
  replyCount: number
  createdAt?: string | null
}

export interface TrackComments {
  bvid: string
  aid: number
  page: number
  pageSize: number
  total: number
  hasMore: boolean
  comments: TrackComment[]
}

export interface TrackReview {
  trackId: string
  bvid: string
  cid?: number | null
  rating: number
  mood: string
  note: string
  visibility: 'private'
  createdAt: string
  updatedAt: string
}

export interface AppSettings {
  feedEnabled?: boolean
  audioQualityPreference: AudioQualityPreference
  playbackSpeed: number
}

export interface DeepSeekKeyStatus {
  configured: boolean
  fallbackConfigured: boolean
  provider: string
}

export interface BiliUpProfile {
  mid: number
  name: string
  face: string
  sign: string
  level?: number
}

export interface BiliUpTracksResult {
  mid: number
  page: number
  pageSize: number
  order: 'pubdate' | 'click'
  total: number
  hasMore: boolean
  profile: BiliUpProfile
  tracks: Track[]
}

export interface RecommendationItem {
  track: Track
  score: number
  source: string
  reason: string
  llmReason?: string
  profileSignals?: string[]
  agentTraceId?: string
  scoreSignals?: Record<string, number>
  matchedPreferences?: string[]
  evidence?: string[]
  penalties?: string[]
  sourceKeywordIds?: string[]
  sourceKeywordFamilyIds?: string[]
  sourceDiscoveryJobIds?: string[]
  recommendationTraceId?: string
}

export interface MusicProfile {
  positive_topics: Record<string, number>
  negative_topics: Record<string, number>
  mbti?: string
  music_persona?: string
  current_music_phase?: string
  core_traits?: string[]
  psychological_needs?: string[]
  persona_evidence?: string[]
  persona_confidence?: number
  preferred_uploaders: Record<string, number>
  avoid_uploaders: Record<string, number>
  blocked_uploaders: Record<string, number>
  mood_weights: Record<string, number>
  recent_intents: string[]
  same_uploader_limit: number
  exploration_ratio: number
  evidence_memory_ids: string[]
  confidence: number
  source: string
}

export interface RecommendationsResult {
  scene: string
  items: RecommendationItem[]
  profile?: MusicProfile
  profileTraceId?: string
  profileVersion?: string
  agentTraceId?: string
  debugTraceId?: string
  discoveryJobId?: string | null
  timing?: Record<string, unknown>
}

export interface RecommendationDiscoveryStatus {
  jobId: string
  available: boolean
  status?: 'queued' | 'running' | 'completed' | 'failed' | 'needs_reconciliation'
  result?: Record<string, unknown>
  error?: string
}

export interface RelevantMemoryTrace {
  memory_id: string
  content: string
  layer: string
  memory_type: string
  tags: string[]
  salience: number
  confidence: number
}

export interface MusicProfileAnalysis {
  scene: string
  profile: MusicProfile
  profileTraceId: string
  memories: RelevantMemoryTrace[]
  summary: {
    topPositiveTopics: Array<{ name: string; weight: number }>
    topNegativeTopics: Array<{ name: string; weight: number }>
    topUploaders: Array<{ name: string; weight: number }>
    topMoods: Array<{ name: string; weight: number }>
    strategy: {
      sameUploaderLimit: number
      explorationRatio: number
      confidence: number
      source: string
    }
    evidenceMemoryCount: number
  }
}

export type AgentDialogueCardAction = 'confirm' | 'reject' | 'discuss' | 'later' | 'accurate' | 'inaccurate'

export type AgentDialogueCardStatus =
  | 'pending'
  | 'confirming'
  | 'confirmed'
  | 'rejected'
  | 'deferred'
  | 'discussing'
  | 'failed'

export type AgentDialogueCardKind =
  | 'interest_probe'
  | 'avoid_probe'
  | 'pending_confirmation'
  | 'recommendation_carousel'
  | 'memory_recall'

export interface AgentDialogueContext {
  cardId: string
  kind: AgentDialogueCardKind
  statement: string
  sourceText: string
  topic: string
  polarity: 'positive' | 'negative' | 'neutral'
  trackId?: string
}

export interface AgentDialogueMessage {
  id: string
  role: 'assistant' | 'user' | 'system'
  content: string
  cardId?: string | null
  createdAt: string
  quotedContext?: AgentDialogueContext | null
  confirmedContext?: AgentDialogueContext | null
  card?: AgentDialogueCard | null
}

export interface AgentDialogueCard {
  cardId: string
  kind: AgentDialogueCardKind
  status: AgentDialogueCardStatus
  title: string
  prompt: string
  statement: string
  topic: string
  polarity: 'positive' | 'negative' | 'neutral'
  sourceText: string
  createdAt: string
  updatedAt: string
  actions: AgentDialogueCardAction[]
  memoryIds: string[]
  eventId?: string | null
  error?: string | null
  note?: string
  recommendations?: RecommendationItem[]
  discoveryJobId?: string | null
  discoveryStatus?: 'queued' | 'running' | 'completed' | 'failed' | 'needs_reconciliation' | null
  tracks?: Track[]
}

export interface AgentDialogueSession {
  sessionId: string
  state: string
  focus: string
  createdAt: string
  updatedAt: string
  pendingContext?: AgentDialogueContext | Record<string, never>
  messages: AgentDialogueMessage[]
  cards: AgentDialogueCard[]
  analysis: MusicProfileAnalysis
}

export interface AgentDialogueSessionSummary {
  sessionId: string
  title: string
  preview: string
  state: string
  focus: string
  createdAt: string
  updatedAt: string
  messageCount: number
}

export interface AgentDialogueSessionsResult {
  items: AgentDialogueSessionSummary[]
}

export interface AgentDialogueResult extends AgentDialogueSession {
  memoryIds?: string[]
  eventId?: string | null
}

export interface AgentDialogueTaskAccepted {
  taskId: string
  sessionId: string
  status: 'queued'
}

export interface AgentDialogueTaskStatus {
  taskId: string
  status: 'queued' | 'running' | 'completed' | 'failed' | 'needs_reconciliation'
  result?: AgentDialogueResult | null
  error?: string | null
}

export interface AgentDialogueStreamEvent {
  eventId: number
  taskId: string
  sessionId: string
  userId: string
  type: 'task' | 'progress' | 'session' | 'discovery' | 'done' | 'error'
  status?: string
  payload: {
    stage?: string
    label?: string
    message?: string
    session?: AgentDialogueResult
    discoveryPending?: boolean
    [key: string]: unknown
  }
  createdAt: string
}

export interface AgentDialogueUndoResult extends AgentDialogueResult {
  undone: boolean
  message?: string
}

export interface ProfileStatementResult {
  description: string
  profile: MusicProfile
  source: string
  eventId?: string | null
  memoryIds: string[]
  analysis: MusicProfileAnalysis
}

export interface RecommendationTraceCandidate {
  trackId?: string
  bvid?: string
  cid?: number | null
  title?: string
  owner?: string
  ownerMid?: number | null
  score?: number
  source?: string
  reason?: string
  tags?: string[]
  llmReason?: string
  profileSignals?: string[]
  agentTraceId?: string | null
  scoreSignals?: Record<string, number>
  matchedPreferences?: string[]
  evidence?: string[]
  sourceKeywordIds?: string[]
  sourceKeywordFamilyIds?: string[]
  sourceDiscoveryJobIds?: string[]
  recommendationTraceId?: string
  penalties?: string[]
}

export interface RecommendationDebugTrace {
  traceId: string | null
  scene: string
  available: boolean
  message?: string
  profileTraceId?: string
  profileVersion?: string
  agentTraceId?: string
  createdAt?: string
  memoryRetrieval?: {
    count: number
    memories: RelevantMemoryTrace[]
  }
  profileSnapshot?: {
    traceId: string
    version: string
    profile: MusicProfile
  }
  musicProfile?: MusicProfile
  agent?: {
    searchQueries: string[]
    localCandidateCount: number
    agentCandidateCount: number
    agentCandidates: RecommendationTraceCandidate[]
  }
  rerankedCandidates?: RecommendationTraceCandidate[]
  finalResults?: RecommendationTraceCandidate[]
}

export type PlayerStatus = 'idle' | 'loading' | 'playing' | 'paused' | 'error'

/** 播放模式：顺序播放 / 列表循环 / 单曲循环 / 随机 */
export type PlayMode = 'order' | 'loop' | 'single' | 'shuffle'

export interface PlayerState {
  status: PlayerStatus
  currentTime: number
  duration: number
  volume: number
  isMuted: boolean
  bufferLevel: number
  videoInfo: VideoInfo | null
  errorMessage: string | null
}

export interface DownloadProgress {
  downloaded_bytes: number
  total_bytes: number
  speed: number
  state: string
  error: string | null
}

export interface PlaybackProgress {
  current_time: number
  duration: number
  buffer_level: number
  state: string
  error: string | null
}

export interface AudioDataPacket {
  data: string
  sample_rate: number
  channels: number
}

export interface BufferStats {
  size: number
  max_size: number
  fill_ratio: number
  chunk_count: number
  state: string
  total_written: number
  total_read: number
}

/** 本地歌单 */
export interface Playlist {
  id: string
  name: string
  cover: string | null
  sourceType?: 'user-created' | 'bilibili-multipage' | 'bilibili-favorite'
  sourceBvid?: string | null
  tracks: Track[]
  createdAt: number | string
  updatedAt?: string
}
