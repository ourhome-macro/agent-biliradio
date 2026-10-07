<template>
  <div class="sidebar-root">
  <aside class="sidebar">
    <div class="brand">
      <img class="brand-logo" :src="iconUrl" alt="logo" />
      <div class="brand-name">
        <span class="brand-title">B 站电台</span>
        <span class="brand-sub">bilibili radio</span>
      </div>
    </div>

    <nav class="nav">
      <RouterLink
        v-for="item in navItems"
        :key="item.to"
        :to="item.to"
        class="nav-item"
        :class="{ active: isActive(item.to) }"
        @click="emit('navigate')"
      >
        <span class="indicator" />
        <AppIcon :name="item.icon" :size="18" />
        <span class="nav-label">{{ item.label }}</span>
      </RouterLink>
    </nav>

    <div class="nav-section-title">我的音乐</div>
    <nav class="nav">
      <RouterLink to="/likes" class="nav-item" :class="{ active: isActive('/likes') }" @click="emit('navigate')">
        <span class="indicator" />
        <AppIcon name="heart" :size="18" />
        <span class="nav-label">我喜欢</span>
      </RouterLink>
      <RouterLink to="/recent" class="nav-item" :class="{ active: isActive('/recent') }" @click="emit('navigate')">
        <span class="indicator" />
        <AppIcon name="clock" :size="18" />
        <span class="nav-label">最近播放</span>
      </RouterLink>
    </nav>

    <div class="nav-section-title playlists-title">
      <span>本地歌单</span>
      <button class="add-playlist" title="新建歌单" @click="openPlaylistDialog">
        <AppIcon name="plus" :size="16" />
      </button>
    </div>
    <nav class="nav playlists">
      <RouterLink
        v-for="playlist in library.playlists"
        :key="playlist.id"
        :to="`/playlist/${playlist.id}`"
        class="nav-item"
        :class="{ active: isActive(`/playlist/${playlist.id}`) }"
        @click="emit('navigate')"
      >
        <span class="indicator" />
        <AppIcon name="list" :size="18" />
        <span class="nav-label">{{ playlist.name }}</span>
      </RouterLink>
      <p v-if="library.playlists.length === 0" class="no-playlist">还没有歌单，点上方 + 新建</p>
    </nav>

  </aside>

  <Teleport to="body">
    <Transition name="dialog">
      <div v-if="playlistDialogOpen" class="dialog-backdrop" @click.self="closePlaylistDialog">
        <form class="playlist-dialog" @submit.prevent="confirmCreatePlaylist" @keydown.esc="closePlaylistDialog">
          <div class="dialog-mark">♪</div>
          <div class="dialog-head">
            <h2>新建歌单</h2>
            <p>给这份电台收藏起一个名字</p>
          </div>
          <label class="dialog-field">
            <span>歌单名称</span>
            <input
              ref="playlistNameInput"
              v-model="playlistName"
              maxlength="32"
              placeholder="我的歌单"
            />
          </label>
          <p v-if="playlistError" class="dialog-error">{{ playlistError }}</p>
          <div class="dialog-actions">
            <button class="ghost-btn" type="button" @click="closePlaylistDialog">取消</button>
            <button class="primary-btn" type="submit">确定</button>
          </div>
        </form>
      </div>
    </Transition>
  </Teleport>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, ref } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { useLibraryStore } from '@/stores/libraryStore'
import { useAuthStore } from '@/stores/authStore'
import AppIcon from '@/components/base/AppIcon.vue'
import iconUrl from '@/assets/icon.png'

const route = useRoute()
const library = useLibraryStore()
const auth = useAuthStore()
const emit = defineEmits<{ navigate: [] }>()

const playlistDialogOpen = ref(false)
const playlistName = ref('我的歌单')
const playlistError = ref('')
const playlistNameInput = ref<HTMLInputElement | null>(null)

import { fetchSettings } from '@/api/client'
const feedEnabled = ref(false)
onMounted(() => { void fetchSettings().then(value => { feedEnabled.value = !!value.feedEnabled }).catch(() => undefined) })
const navItems = computed(() => [
  { to: '/', label: '发现', icon: 'home' },
  ...(feedEnabled.value ? [{ to: '/feed', label: '音乐视频', icon: 'play' }] : []),
  ...(feedEnabled.value && auth.isAdmin ? [{ to: '/admin/feed', label: '视频运营', icon: 'list' }] : []),
  { to: '/search', label: '搜索', icon: 'search' },
  { to: '/favorites', label: 'B 站收藏夹', icon: 'star' },
  { to: '/agent', label: '音乐助手', icon: 'message' },
])

function isActive(path: string): boolean {
  if (path === '/') return route.path === '/'
  return route.path === path || route.path.startsWith(path + '/')
}

async function openPlaylistDialog() {
  playlistName.value = '我的歌单'
  playlistError.value = ''
  playlistDialogOpen.value = true
  await nextTick()
  playlistNameInput.value?.focus()
  playlistNameInput.value?.select()
}

function closePlaylistDialog() {
  playlistDialogOpen.value = false
}

function confirmCreatePlaylist() {
  const name = playlistName.value.trim()
  if (!name) {
    playlistError.value = '歌单名称不能为空'
    return
  }
  library.createPlaylist(name)
  closePlaylistDialog()
}
</script>

<style scoped>
.sidebar-root {
  width: 100%;
  height: 100%;
  min-width: 0;
  min-height: 0;
}

.sidebar {
  width: 100%;
  height: 100%;
  background: var(--color-bg-sidebar);
  border-right: 1px solid var(--color-border);
  display: flex;
  flex-direction: column;
  padding: 16px 12px;
  overflow-y: auto;
}

.brand {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 4px 8px 20px;
}

.brand-logo {
  width: 36px;
  height: 36px;
  border-radius: 10px;
  object-fit: cover;
}

.brand-name {
  display: flex;
  flex-direction: column;
  line-height: 1.2;
}

.brand-title {
  font-size: 15px;
  font-weight: 700;
  color: var(--color-text-primary);
}

.brand-sub {
  font-size: 11px;
  color: var(--color-text-tertiary);
}

.nav {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.nav-item {
  position: relative;
  display: flex;
  align-items: center;
  gap: 12px;
  height: 40px;
  padding: 0 12px;
  border-radius: var(--radius-small);
  color: var(--color-text-secondary);
  text-decoration: none;
  transition: background 160ms ease, color 160ms ease;
}

.nav-item:hover {
  background: var(--color-bg-hover);
  color: var(--color-text-primary);
}

.nav-item.active {
  background: var(--color-primary-soft);
  color: var(--color-primary);
}

.indicator {
  position: absolute;
  left: 0;
  top: 50%;
  transform: translateY(-50%);
  width: 3px;
  height: 0;
  border-radius: 0 2px 2px 0;
  background: var(--color-primary);
  transition: height 160ms ease;
}

.nav-item.active .indicator {
  height: 18px;
}

.nav-label {
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.nav-section-title {
  font-size: 12px;
  color: var(--color-text-tertiary);
  padding: 20px 12px 8px;
  font-weight: 500;
}

.playlists-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.add-playlist {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border: none;
  background: transparent;
  color: var(--color-text-tertiary);
  border-radius: 6px;
  cursor: pointer;
  transition: background 160ms ease, color 160ms ease;
}

.add-playlist:hover {
  background: var(--color-bg-hover);
  color: var(--color-primary);
}

.playlists {
  flex: 1;
}

.no-playlist {
  font-size: 12px;
  color: var(--color-text-tertiary);
  padding: 8px 12px;
  line-height: 1.5;
}

.dialog-backdrop {
  position: fixed;
  inset: 0;
  z-index: 300;
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(12, 12, 16, 0.58);
  backdrop-filter: blur(10px);
}

.playlist-dialog {
  position: relative;
  width: min(360px, 100%);
  padding: 24px;
  border: 1px solid var(--color-border);
  border-radius: var(--radius-medium);
  background: var(--color-bg-content);
  box-shadow: var(--shadow-popup);
  overflow: hidden;
}

.playlist-dialog::before {
  content: '';
  position: absolute;
  inset: 0 0 auto;
  height: 3px;
  background: linear-gradient(90deg, var(--color-primary), #23ade5);
}

.dialog-mark {
  width: 38px;
  height: 38px;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: var(--color-primary-soft);
  color: var(--color-primary);
  font-size: 20px;
  font-weight: 700;
}

.dialog-head {
  margin-top: 14px;
}

.dialog-head h2 {
  font-size: 20px;
  line-height: 1.2;
  color: var(--color-text-primary);
}

.dialog-head p {
  margin-top: 6px;
  font-size: 13px;
  color: var(--color-text-secondary);
}

.dialog-field {
  margin-top: 18px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.dialog-field span {
  font-size: 12px;
  color: var(--color-text-secondary);
}

.dialog-field input {
  height: 38px;
  padding: 0 12px;
  border: 1px solid var(--color-border);
  border-radius: var(--radius-small);
  background: var(--color-bg-app);
  outline: none;
  color: var(--color-text-primary);
  transition: border-color 160ms ease, background 160ms ease;
}

.dialog-field input:focus {
  border-color: var(--color-primary);
  background: var(--color-bg-content);
}

.dialog-error {
  margin-top: 10px;
  color: var(--color-primary);
  font-size: 12px;
}

.dialog-actions {
  margin-top: 22px;
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

.primary-btn,
.ghost-btn {
  height: 34px;
  padding: 0 16px;
  border-radius: var(--radius-small);
  font-size: 13px;
}

.primary-btn {
  background: var(--color-primary);
  color: #fff;
}

.primary-btn:hover {
  background: var(--color-primary-hover);
}

.ghost-btn {
  border: 1px solid var(--color-border);
  color: var(--color-text-secondary);
}

.ghost-btn:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

.dialog-enter-active,
.dialog-leave-active {
  transition: opacity 160ms ease;
}

.dialog-enter-active .playlist-dialog,
.dialog-leave-active .playlist-dialog {
  transition: transform 160ms ease, opacity 160ms ease;
}

.dialog-enter-from,
.dialog-leave-to {
  opacity: 0;
}

.dialog-enter-from .playlist-dialog,
.dialog-leave-to .playlist-dialog {
  opacity: 0;
  transform: translateY(12px) scale(0.98);
}
</style>
