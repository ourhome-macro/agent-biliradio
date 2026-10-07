from __future__ import annotations

from datetime import datetime, timedelta, timezone

from database import get_connection
from music_profile import MusicProfile
from recommendation_contracts import (
    NEGATIVE_OWNER_SUPPRESSION_THRESHOLD,
    RECENT_LISTEN_DAYS,
    RECENT_RECOMMEND_DAYS,
    UserProfile,
    _json_loads,
)


class UserProfileReader:
    def __init__(self, db_path, user_id: str):
        self.db_path = db_path
        self.user_id = user_id

    def _load_user_profile(self) -> UserProfile:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=RECENT_LISTEN_DAYS)).isoformat()
        recommend_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=RECENT_RECOMMEND_DAYS)
        ).isoformat()
        profile = UserProfile()

        with get_connection(self.db_path) as conn:
            profile.frequent_owner_mids = {
                int(row["owner_mid"])
                for row in conn.execute(
                    """
                    SELECT t.owner_mid, COUNT(*) + COALESCE(SUM(r.play_count), 0) AS weight
                    FROM tracks t
                    LEFT JOIN recent r ON r.user_id = ? AND r.track_id = t.track_id
                    LEFT JOIN playback_recent pr ON pr.user_id = ? AND pr.track_id = t.track_id
                    WHERE t.owner_mid IS NOT NULL
                      AND (r.track_id IS NOT NULL OR pr.track_id IS NOT NULL)
                    GROUP BY t.owner_mid
                    ORDER BY weight DESC
                    LIMIT 10
                    """,
                    (self.user_id, self.user_id),
                ).fetchall()
                if row["owner_mid"]
            }
            profile.liked_owner_mids = {
                int(row["owner_mid"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT t.owner_mid
                    FROM likes l
                    JOIN tracks t ON t.track_id = l.track_id
                    WHERE l.user_id = ? AND t.owner_mid IS NOT NULL
                    LIMIT 20
                    """,
                    (self.user_id,),
                ).fetchall()
                if row["owner_mid"]
            }
            profile.common_tags = {
                str(row["mood"]).strip()
                for row in conn.execute(
                    """
                    SELECT mood, COUNT(*) AS weight
                    FROM track_reviews
                    WHERE user_id = ? AND TRIM(mood) <> ''
                    GROUP BY mood
                    ORDER BY weight DESC, MAX(updated_at) DESC
                    LIMIT 10
                    """,
                    (self.user_id,),
                ).fetchall()
                if str(row["mood"]).strip()
            }
            profile.repeated_owner_mids = {
                int(row["owner_mid"])
                for row in conn.execute(
                    """
                    SELECT t.owner_mid
                    FROM recent r
                    JOIN tracks t ON t.track_id = r.track_id
                    WHERE r.user_id = ? AND r.play_count >= 2 AND t.owner_mid IS NOT NULL
                    GROUP BY t.owner_mid
                    LIMIT 10
                    """,
                    (self.user_id,),
                ).fetchall()
                if row["owner_mid"]
            }
            profile.completed_owner_mids = {
                int(row["owner_mid"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT t.owner_mid
                    FROM tracks t
                    LEFT JOIN recent r ON r.user_id = ? AND r.track_id = t.track_id
                    LEFT JOIN playback_recent pr ON pr.user_id = ? AND pr.track_id = t.track_id
                    WHERE t.owner_mid IS NOT NULL
                      AND (COALESCE(r.completed, 0) = 1 OR COALESCE(pr.completed, 0) = 1)
                    LIMIT 20
                    """,
                    (self.user_id, self.user_id),
                ).fetchall()
                if row["owner_mid"]
            }
            profile.recently_heard_track_ids = {
                str(row["track_id"])
                for row in conn.execute(
                    """
                    SELECT track_id FROM recent
                    WHERE user_id = ? AND last_played_at >= ?
                    UNION
                    SELECT track_id FROM playback_recent
                    WHERE user_id = ? AND last_played_at >= ?
                    """,
                    (self.user_id, cutoff, self.user_id, cutoff),
                ).fetchall()
            }
            profile.recently_recommended_track_ids = {
                str(row["track_id"])
                for row in conn.execute(
                    """
                    SELECT track_id FROM recommendation_history
                    WHERE user_id = ? AND recommended_at >= ?
                    UNION
                    SELECT track_id FROM recommendation_events
                    WHERE user_id = ? AND event = 'shown' AND created_at >= ?
                    """,
                    (self.user_id, recommend_cutoff, self.user_id, recommend_cutoff),
                ).fetchall()
            }
            profile.skipped_track_ids = {
                str(row["track_id"])
                for row in conn.execute(
                    """
                    SELECT track_id FROM playback_recent
                    WHERE user_id = ? AND skipped = 1
                    UNION
                    SELECT track_id FROM recommendation_history
                    WHERE user_id = ? AND skipped = 1
                    UNION
                    SELECT track_id FROM recommendation_events
                    WHERE user_id = ? AND event IN ('skipped', 'dismissed', 'dislike')
                    """,
                    (self.user_id, self.user_id, self.user_id),
                ).fetchall()
            }
            profile.recently_heard_recording_ids = {
                str(row["recording_id"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT t.recording_id
                    FROM tracks t
                    WHERE t.recording_id IS NOT NULL AND t.track_id IN (
                        SELECT track_id FROM recent
                        WHERE user_id=? AND last_played_at>=?
                        UNION
                        SELECT track_id FROM playback_recent
                        WHERE user_id=? AND last_played_at>=?
                    )
                    """,
                    (self.user_id, cutoff, self.user_id, cutoff),
                ).fetchall()
                if row["recording_id"]
            }
            profile.recently_recommended_work_ids = {
                str(row["work_id"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT t.work_id
                    FROM tracks t
                    WHERE t.work_id IS NOT NULL AND t.track_id IN (
                        SELECT track_id FROM recommendation_history
                        WHERE user_id=? AND recommended_at>=?
                        UNION
                        SELECT track_id FROM recommendation_events
                        WHERE user_id=? AND event='shown' AND created_at>=?
                    )
                    """,
                    (self.user_id, recommend_cutoff, self.user_id, recommend_cutoff),
                ).fetchall()
                if row["work_id"]
            }
            profile.skipped_recording_ids = {
                str(row["recording_id"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT t.recording_id
                    FROM tracks t
                    WHERE t.recording_id IS NOT NULL AND t.track_id IN (
                        SELECT track_id FROM playback_recent
                        WHERE user_id=? AND skipped=1
                        UNION
                        SELECT track_id FROM recommendation_history
                        WHERE user_id=? AND skipped=1
                        UNION
                        SELECT track_id FROM recommendation_events
                        WHERE user_id=? AND event IN ('skipped', 'dismissed', 'dislike')
                    )
                    """,
                    (self.user_id, self.user_id, self.user_id),
                ).fetchall()
                if row["recording_id"]
            }
            profile.negative_owner_mids = {
                int(row["owner_mid"])
                for row in conn.execute(
                    """
                    SELECT owner_mid
                    FROM (
                        SELECT t.owner_mid AS owner_mid
                        FROM recommendation_events e
                        JOIN tracks t ON t.track_id = e.track_id
                        WHERE e.user_id = ?
                          AND e.event IN ('skipped', 'dismissed', 'dislike')
                          AND t.owner_mid IS NOT NULL
                        UNION ALL
                        SELECT t.owner_mid AS owner_mid
                        FROM recommendation_history h
                        JOIN tracks t ON t.track_id = h.track_id
                        WHERE h.user_id = ?
                          AND h.skipped = 1
                          AND t.owner_mid IS NOT NULL
                    )
                    AS negative_evidence GROUP BY owner_mid
                    HAVING COUNT(*) >= ?
                    LIMIT 50
                    """,
                    (
                        self.user_id,
                        self.user_id,
                        NEGATIVE_OWNER_SUPPRESSION_THRESHOLD,
                    ),
                ).fetchall()
                if row["owner_mid"]
            }

        return profile

    def _fallback_music_profile(self, profile: UserProfile) -> MusicProfile:
        fallback = MusicProfile(
            positive_topics={tag: 0.72 for tag in profile.common_tags},
            preferred_uploaders={
                str(mid): 0.75 for mid in (profile.liked_owner_mids | profile.frequent_owner_mids)
            },
            confidence=0.45 if profile.common_tags or profile.liked_owner_mids else 0.0,
            source="fallback",
        )
        with get_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT profile_json FROM music_profile_snapshots WHERE user_id = ?",
                (self.user_id,),
            ).fetchone()
        if row is None:
            return fallback
        stored = _json_loads(row["profile_json"])
        snapshot = MusicProfile.from_dict(stored, source="profile_snapshot")
        for name in (
            "positive_topics",
            "negative_topics",
            "preferred_uploaders",
            "avoid_uploaders",
            "blocked_uploaders",
            "mood_weights",
        ):
            getattr(fallback, name).update(getattr(snapshot, name))
        for name in (
            "mbti",
            "music_persona",
            "current_music_phase",
            "core_traits",
            "psychological_needs",
            "persona_evidence",
            "persona_confidence",
        ):
            value = getattr(snapshot, name)
            if value:
                setattr(fallback, name, value)
        fallback.recent_intents = list(snapshot.recent_intents)
        fallback.positive_interest_texts = list(snapshot.positive_interest_texts)
        fallback.negative_interest_texts = list(snapshot.negative_interest_texts)
        fallback.same_uploader_limit = snapshot.same_uploader_limit
        fallback.exploration_ratio = snapshot.exploration_ratio
        fallback.confidence = max(fallback.confidence, snapshot.confidence)
        fallback.source = "profile_snapshot"
        return fallback
