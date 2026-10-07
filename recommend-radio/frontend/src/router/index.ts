import { createRouter, createWebHashHistory } from 'vue-router'
import { useAuthStore } from '@/stores/authStore'

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    {
      path: '/session-unavailable',
      name: 'session-unavailable',
      component: () => import('@/views/SessionUnavailableView.vue'),
      meta: { layout: 'auth', sessionRecovery: true },
    },
    {
      path: '/login',
      name: 'login',
      component: () => import('@/views/LoginView.vue'),
      meta: { layout: 'auth' },
    },
    {
      path: '/',
      name: 'home',
      component: () => import('@/views/HomeView.vue'),
    },
    {
      path: '/search',
      name: 'search',
      component: () => import('@/views/SearchView.vue'),
    },
    {
      path: '/feed',
      name: 'video-feed',
      component: () => import('@/views/FeedView.vue'),
    },
    {
      path: '/admin/feed',
      name: 'feed-operations',
      component: () => import('@/views/FeedOperations.vue'),
      meta: { requiresAdmin: true },
    },
    {
      path: '/favorites',
      name: 'favorites',
      component: () => import('@/views/FavoritesView.vue'),
    },
    {
      path: '/agent',
      name: 'agent-dialogue',
      component: () => import('@/views/AgentDialogueView.vue'),
    },
    {
      path: '/settings',
      name: 'settings',
      component: () => import('@/views/SettingsView.vue'),
    },
    {
      path: '/playlist/:id',
      name: 'playlist',
      component: () => import('@/views/PlaylistDetailView.vue'),
    },
    {
      path: '/up/resolve/:bvid',
      name: 'up-resolve',
      component: () => import('@/views/UpProfileView.vue'),
    },
    {
      path: '/up/:mid',
      name: 'up',
      component: () => import('@/views/UpProfileView.vue'),
    },
    {
      path: '/recent',
      name: 'recent',
      component: () => import('@/views/RecentView.vue'),
    },
    {
      path: '/likes',
      name: 'likes',
      component: () => import('@/views/LikesView.vue'),
    },
    {
      path: '/:pathMatch(.*)*',
      redirect: '/',
    },
  ],
})

router.beforeEach(async (to) => {
  if (to.meta.sessionRecovery) return true

  const auth = useAuthStore()
  try {
    await auth.initializeSession()
  } catch {
    return {
      name: 'session-unavailable',
      query: { redirect: to.fullPath },
    }
  }

  if (!auth.appAuthenticated) {
    auth.loginWithOidc()
    return false
  }

  if (to.meta.requiresAdmin && !auth.isAdmin) return { name: 'home' }

  return true
})

export default router
