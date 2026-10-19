from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from auth_service import AuthService
from bili_client import BiliClient
from database import get_connection
from error_code import APIError
from feed_runtime.repository import FeedRepository
from following.service import FollowingService
from following.sync import execute_sync, observe, start_sync
from following.worker import execute_follow
from models import BiliUserProfile, Track


def make_following(tmp_path, monkeypatch, *, db_path=None):
    monkeypatch.setenv("RABBITMQ_ENABLED", "false")
    repo = FeedRepository(db_path or tmp_path / "following.sqlite3")
    with get_connection(repo.db_path) as conn:
        for user in ("alice", "bob"):
            conn.execute(
                "INSERT INTO app_users(id,created_at,updated_at) VALUES (?,?,?)",
                (user, "2026-10-08", "2026-10-08"),
            )
    auth = AuthService(repo.db_path, user_id="alice")
    auth._save_auth(
        "SESSDATA=fixture; bili_jct=fixture-csrf; DedeUserID=101",
        None,
        BiliUserProfile(mid=101, name="Fixture"),
    )
    remote = {
        "following": set(),
        "requests": [],
        "lost_response": False,
        "on_post": None,
        "pages": None,
        "fail_page": None,
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            path = urlparse(self.path)
            query = parse_qs(path.query)
            remote["requests"].append(("GET", path.path, query))
            if path.path == "/x/relation":
                data = {"attribute": 2 if int(query["fid"][0]) in remote["following"] else 0}
            elif path.path == "/x/relation/followings":
                page = int(query["pn"][0])
                if page == remote["fail_page"]:
                    self.send_response(503)
                    self.end_headers()
                    return
                data = remote["pages"][page - 1] if remote["pages"] else {"list": [], "total": 0}
            else:
                raise AssertionError(path.path)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"code": 0, "data": data}).encode())

        def do_POST(self):
            body = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
            assert body["csrf"] == ["fixture-csrf"]
            assert "DedeUserID=101" in self.headers["Cookie"]
            remote["requests"].append(("POST", self.path, {"fid": body["fid"], "act": body["act"]}))
            mid = int(body["fid"][0])
            remote["following"].add(mid) if body["act"] == ["1"] else remote["following"].discard(
                mid
            )
            callback, remote["on_post"] = remote["on_post"], None
            if callback:
                callback()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"code":0,"data":{}}')

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    class Session(requests.Session):
        def request(self, method, url, **kwargs):
            parsed = urlparse(url)
            assert parsed.hostname == "api.bilibili.com", "Unexpected external endpoint in fixture"
            result = super().request(method, base + parsed.path, **kwargs)
            if method.upper() == "POST" and remote["lost_response"]:
                remote["lost_response"] = False
                raise requests.Timeout("fixture drops already committed response")
            return result

    class Client(BiliClient):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.auth_session.close()
            self.auth_session = Session()

    service = FollowingService(repo.db_path, client_factory=Client)
    with get_connection(repo.db_path) as conn:
        creator = service.creator(conn, 202, "Fixture creator")
    yield service, auth, remote, creator
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


@pytest.fixture
def following(tmp_path, monkeypatch):
    yield from make_following(tmp_path, monkeypatch)


def job_for(service, kind, *, latest=True):
    with get_connection(service.db_path) as conn:
        direction = "DESC" if latest else "ASC"
        row = conn.execute(
            f"SELECT * FROM durable_jobs WHERE kind=? ORDER BY created_at {direction} LIMIT 1",
            (kind,),
        ).fetchone()
    job = dict(row)
    job["attempts"] = 1
    return job, json.loads(job["payload_json"])


def test_follow_command_queries_then_mutates_with_csrf_and_confirms(following):
    service, _, remote, creator = following
    first = service.set("alice", creator, True, "one", expected_version=0)
    assert first == service.set("alice", creator, True, "one", expected_version=0)
    assert not remote["requests"] and not service.snapshot("alice")["creatorIds"]
    job, payload = job_for(service, "creator_follow")
    assert execute_follow(service, job, payload)["status"] == "synced"
    assert service.snapshot("alice")["creatorIds"] == [creator]
    assert [r[0] for r in remote["requests"]] == ["GET", "POST", "GET"]
    with pytest.raises(APIError):
        service.set("alice", creator, False, "one", expected_version=1)
    with pytest.raises(APIError):
        service.set("alice", creator, False, "stale", expected_version=0)


def test_timeout_after_remote_commit_is_confirmed_without_second_write(following):
    service, _, remote, creator = following
    remote["lost_response"] = True
    service.set("alice", creator, True, "lost", expected_version=0)
    job, payload = job_for(service, "creator_follow")
    assert execute_follow(service, job, payload)["status"] == "synced"
    assert len([r for r in remote["requests"] if r[0] == "POST"]) == 1
    assert service.state("alice", creator)["confirmedFollowing"] is True


def test_old_task_and_inflight_ack_cannot_override_new_desired_state(following):
    service, _, remote, creator = following
    service.set("alice", creator, True, "follow", expected_version=0)
    old_job, old_payload = job_for(service, "creator_follow")
    remote["on_post"] = lambda: service.set("alice", creator, False, "unfollow", expected_version=1)
    assert execute_follow(service, old_job, old_payload)["status"] == "superseded"
    state = service.state("alice", creator)
    assert state["following"] is False and state["relationVersion"] == 2
    assert not service.snapshot("alice")["creatorIds"]
    calls = len(remote["requests"])
    assert execute_follow(service, old_job, old_payload)["status"] == "superseded"
    assert len(remote["requests"]) == calls
    verify_job, verify_payload = job_for(service, "creator_follow_verify")
    assert execute_follow(service, verify_job, verify_payload, verify=True)["following"] is False
    assert remote["following"] == set()


def test_binding_cookie_refresh_stable_logout_and_rebind_fence_old_jobs(following):
    service, auth, remote, creator = following
    initial = service.current_binding("alice")
    service.set("alice", creator, True, "queued", expected_version=0)
    job, payload = job_for(service, "creator_follow")
    auth._save_auth(
        "SESSDATA=refreshed; bili_jct=fixture-csrf; DedeUserID=101",
        None,
        BiliUserProfile(mid=101, name="Fixture refreshed"),
    )
    assert service.current_binding("alice") == initial
    auth.logout()
    assert service.snapshot("alice")["syncStatus"] == "unbound"
    auth._save_auth(
        "SESSDATA=rebound; bili_jct=fixture-csrf; DedeUserID=101",
        None,
        BiliUserProfile(mid=101, name="Fixture rebound"),
    )
    assert service.current_binding("alice") != initial
    assert execute_follow(service, job, payload)["status"] == "superseded"
    assert remote["requests"] == []
    assert service.state("alice", creator)["relationVersion"] == 0
    with pytest.raises(APIError):
        service.set("bob", creator, True, "bob", expected_version=0)


def test_unknown_and_ugc_sources_never_reach_bilibili(following):
    service, _, remote, _ = following
    from ugc.service import UGCService
    content = UGCService(service.db_path).command("alice", "create", {"kind": "text", "title": "Music Live"}, "ugc-source")["contentId"]
    assert service.content_creator("alice", content)["supported"] is False
    with get_connection(service.db_path) as conn:
        conn.execute("INSERT INTO creators VALUES ('ugc','ugc','202','UGC','',?)", (time.time(),))
    with pytest.raises(APIError):
        service.set("alice", "ugc", True, "wrong-source", expected_version=0)
    assert remote["requests"] == []


def test_partial_list_does_not_remove_unseen_following(following):
    service, _, remote, creator = following
    with get_connection(service.db_path) as conn:
        observe(
            service,
            conn,
            "alice",
            service.current_binding("alice"),
            creator,
            True,
            started=time.time(),
        )
    remote["pages"] = [
        {"list": [{"mid": i, "uname": f"Creator {i}"} for i in range(300, 350)], "total": 100}
    ]
    remote["fail_page"] = 2
    start_sync(service, "alice", "list")
    job, payload = job_for(service, "following_sync")
    execute_sync(service, job, payload)
    job, payload = job_for(service, "following_sync")
    with pytest.raises(RuntimeError):
        execute_sync(service, job, payload)
    assert service.state("alice", creator)["following"] is True
    assert len(service.snapshot("alice")["creatorIds"]) == 51


def test_complete_list_requires_individual_absence_confirmation(following):
    service, _, remote, creator = following
    with get_connection(service.db_path) as conn:
        observe(
            service,
            conn,
            "alice",
            service.current_binding("alice"),
            creator,
            True,
            started=time.time(),
        )
    start_sync(service, "alice", "list")
    job, payload = job_for(service, "following_sync")
    execute_sync(service, job, payload)
    assert service.state("alice", creator)["following"] is True
    job, payload = job_for(service, "following_sync")
    assert payload["phase"] == "verify"
    execute_sync(service, job, payload)
    assert service.state("alice", creator)["following"] is False
    assert service.snapshot("alice")["syncStatus"] == "completed"
    assert all(row[0] == "GET" for row in remote["requests"])


def test_busy_relation_retries_without_dropping_latest_command(following):
    service, _, remote, creator = following
    service.set("alice", creator, True, "busy", expected_version=0)
    with get_connection(service.db_path) as conn:
        conn.execute(
            "UPDATE creator_follow_relations SET lease_token='another-worker',lease_until=?",
            (time.time() + 60,),
        )
    job, payload = job_for(service, "creator_follow")
    with pytest.raises(RuntimeError, match="FollowRelationBusy"):
        execute_follow(service, job, payload)
    assert not remote["requests"]
    assert service.state("alice", creator)["syncStatus"] == "pending"
    with get_connection(service.db_path) as conn:
        assert (
            conn.execute("SELECT lease_token FROM creator_follow_relations").fetchone()[0]
            == "another-worker"
        )


def test_unchanged_follow_is_noop_and_sync_does_not_erase_pending_intent(following):
    service, _, remote, creator = following
    service.set("alice", creator, True, "first", expected_version=0)
    job, payload = job_for(service, "creator_follow")
    execute_follow(service, job, payload)
    assert (
        service.set("alice", creator, True, "same-state", expected_version=1)["relationVersion"]
        == 1
    )
    service.set("alice", creator, False, "remove", expected_version=1)
    with get_connection(service.db_path) as conn:
        observe(
            service,
            conn,
            "alice",
            service.current_binding("alice"),
            creator,
            True,
            started=time.time(),
        )
    assert service.state("alice", creator)["following"] is False
    assert service.state("alice", creator)["syncStatus"] == "pending"


def test_stale_profile_refresh_cannot_restore_previous_account(following):
    service, auth, _, _ = following
    old = auth._auth_row()["cookie_encrypted"]
    auth._save_auth(
        "SESSDATA=other; bili_jct=fixture-csrf; DedeUserID=303",
        None,
        BiliUserProfile(mid=303, name="Changed account"),
    )
    with pytest.raises(APIError):
        auth._save_auth(
            "SESSDATA=old; bili_jct=fixture-csrf; DedeUserID=101",
            None,
            BiliUserProfile(mid=101, name="Stale refresh"),
            expected_cookie=old,
        )
    assert service.current_binding("alice")["mid"] == "303"


def test_follow_outbox_native_stream_and_actual_dispatch_are_idempotent(
    following, tmp_path, monkeypatch
):
    import task_app
    from durable_jobs import publish_pending
    from following import service as service_module
    from redis_fixture import NativeRedis
    from redis_stream_jobs import RedisJobBus, RedisJobWorker

    service, _, remote, creator = following
    monkeypatch.setattr(task_app, "DEFAULT_DB_PATH", service.db_path)
    monkeypatch.setattr(service_module, "FollowingService", lambda *_args, **_kwargs: service)
    server = NativeRedis(tmp_path / "redis-following")
    try:
        bus = RedisJobBus(service.db_path, server.url)
        bus.ensure_groups(("jobs",))
        service.set("alice", creator, True, "stream-command", expected_version=0)
        assert publish_pending(service.db_path, bus.publish) == 1
        delivery = bus.read(("jobs",), "fixture-worker", block_ms=10)[0]
        worker = RedisJobWorker(bus, dispatcher=task_app.dispatch)
        assert worker.process(*delivery, "fixture-worker") == "completed"
        assert service.state("alice", creator)["confirmedFollowing"] is True
        job, _ = job_for(service, "creator_follow")
        bus.publish(job["job_id"], "creator_follow")
        replay = bus.read(("jobs",), "fixture-worker", block_ms=10)[0]
        assert worker.process(*replay, "fixture-worker") == "deferred_or_terminal"
        assert len([r for r in remote["requests"] if r[0] == "POST"]) == 1
        assert bus.redis.xpending(bus.key("jobs"), bus.group)["pending"] == 0
    finally:
        server.close()


def test_early_006_database_is_repaired_by_007_without_losing_account(following):
    from alembic import command
    from schema_migrations import config

    service, auth, _, _ = following
    expected = service.current_binding("alice")
    encrypted = auth._auth_row()["cookie_encrypted"]
    with get_connection(service.db_path) as conn:
        conn.execute("ALTER TABLE bili_binding_epochs DROP COLUMN binding_key")
        conn.execute("ALTER TABLE creator_supply DROP COLUMN next_page")
        conn.execute("ALTER TABLE creator_supply DROP COLUMN scan_complete")
        conn.execute("DROP INDEX idx_follow_supply")
        conn.execute("UPDATE alembic_version SET version_num='radio_006'")
        conn.execute("PRAGMA user_version=30")
    command.upgrade(config(service.db_path), "radio_007")
    with get_connection(service.db_path) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "radio_007"
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 31
        assert (
            conn.execute(
                "SELECT binding_key FROM bili_binding_epochs WHERE user_id='alice'"
            ).fetchone()[0]
            == expected["binding_key"]
        )
        assert {"next_page", "scan_complete"} <= {
            row[1] for row in conn.execute("PRAGMA table_info(creator_supply)")
        }
    assert auth._auth_row()["cookie_encrypted"] == encrypted
    assert service.current_binding("alice") == expected


def test_delayed_confirmation_does_not_overwrite_later_bilibili_unfollow(following):
    service, _, remote, creator = following
    service.set("alice", creator, True, "follow", expected_version=0)
    job, payload = job_for(service, "creator_follow")
    execute_follow(service, job, payload)
    remote["following"].discard(202)  # User changes the relationship directly on Bilibili.
    job, payload = job_for(service, "creator_follow_verify")
    execute_follow(service, job, payload, verify=True)
    assert service.state("alice", creator)["following"] is False
    assert len([r for r in remote["requests"] if r[0] == "POST"]) == 1
    remote["following"].add(202)
    job, payload = job_for(service, "creator_follow_verify", latest=False)
    execute_follow(service, job, payload, verify=True)
    assert service.state("alice", creator)["following"] is True
    assert len([r for r in remote["requests"] if r[0] == "POST"]) == 1
