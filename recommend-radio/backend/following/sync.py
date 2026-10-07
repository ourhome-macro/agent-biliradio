from __future__ import annotations

import json
import time
from uuid import uuid4

from database import get_connection
from durable_jobs import enqueue
from error_code import APIError
from job_errors import JobPermanentFailure

from .service import FollowingService, encode, identity, write


def start_sync(service, user, key):
    if not isinstance(key, str) or not key or len(key) > 180:
        raise APIError.validation_error("Idempotency-Key is required")
    with get_connection(service.db_path) as conn:
        write(conn, "follow-sync", user)
        binding = service.require_binding(user, conn=conn)
        existing = conn.execute(
            "SELECT * FROM follow_sync_runs WHERE user_id=? AND command_id=?", (user, key)
        ).fetchone()
        if existing:
            if existing["binding_key"] != binding["binding_key"]:
                raise APIError.conflict("Sync command belongs to an old binding")
            return {"syncRunId": existing["sync_run_id"], "status": existing["status"]}
        active = conn.execute(
            "SELECT * FROM follow_sync_runs WHERE user_id=? AND binding_key=? "
            "AND status IN ('queued','running') AND updated_at>? ORDER BY created_at DESC LIMIT 1",
            (user, binding["binding_key"], time.time() - 1800),
        ).fetchone()
        if active:
            return {"syncRunId": active["sync_run_id"], "status": active["status"]}
        run, now = uuid4().hex, time.time()
        conn.execute(
            "INSERT INTO "
            "follow_sync_runs(sync_run_id,user_id,bili_account_mid,binding_key,"
            "command_id,status,created_at,updated_at) "
            "VALUES (?,?,?,?,?,'queued',?,?)",
            (run, user, binding["mid"], binding["binding_key"], key, now, now),
        )
        enqueue_page(conn, user, run, 1)
    return {"syncRunId": run, "status": "queued"}


def enqueue_page(conn, user, run, page, *, phase="list"):
    enqueue(
        conn,
        kind="following_sync",
        user_id=user,
        retry_safe=True,
        job_id=f"following-sync:{run}:{phase}:{page}",
        payload={"sync_run_id": run, "page": page, "phase": phase},
    )


def observe(service, conn, user, binding, creator, confirmed, *, started):
    write(conn, "follow-relation", encode([user, binding["mid"], creator]))
    if service.current_binding(user, conn) != binding:
        raise APIError.conflict("Binding changed during following reconciliation")
    row = conn.execute(
        "SELECT * FROM creator_follow_relations WHERE user_id=? AND bili_account_mid=? AND "
        "creator_id=?",
        (user, binding["mid"], creator),
    ).fetchone()
    same_binding = row and row["binding_key"] == binding["binding_key"]
    if same_binding and (
        row["updated_at"] > started
        or row["sync_status"] in {"pending", "unknown", "failed"}
        or row["lease_until"] > time.time()
    ):
        return
    now = time.time()
    changed = (
        not same_binding
        or row["desired_state"] != int(confirmed)
        or row["confirmed_state"] != int(confirmed)
    )
    version = (row["version"] if row else 0) + int(changed)
    if row:
        conn.execute(
            "UPDATE creator_follow_relations SET "
            "binding_key=?,desired_state=?,confirmed_state=?,sync_status='synced',"
            "version=?,last_verified_at=?,error_type=NULL,"
            "lease_token=NULL,lease_until=0,updated_at=? "
            "WHERE user_id=? AND bili_account_mid=? AND creator_id=?",
            (
                binding["binding_key"],
                int(confirmed),
                int(confirmed),
                version,
                now,
                now,
                user,
                binding["mid"],
                creator,
            ),
        )
    else:
        conn.execute(
            "INSERT INTO "
            "creator_follow_relations(user_id,bili_account_mid,creator_id,binding_key,desired_state,"
            "confirmed_state,sync_status,version,last_verified_at,updated_at) VALUES "
            "(?,?,?,?,?,?,'synced',?,?,?)",
            (
                user,
                binding["mid"],
                creator,
                binding["binding_key"],
                int(confirmed),
                int(confirmed),
                version,
                now,
                now,
            ),
        )


def execute_sync(service: FollowingService, job, payload):
    user, run_id = job["user_id"], payload["sync_run_id"]
    with get_connection(service.db_path) as conn:
        run = conn.execute(
            "SELECT * FROM follow_sync_runs WHERE sync_run_id=? AND user_id=?", (run_id, user)
        ).fetchone()
    if not run or run["status"] in {"completed", "superseded", "partial"}:
        return {"status": "stale"}
    binding = {"mid": run["bili_account_mid"], "binding_key": run["binding_key"]}
    if service.current_binding(user) != binding:
        with get_connection(service.db_path) as conn:
            conn.execute(
                "UPDATE follow_sync_runs SET status='superseded',updated_at=? WHERE sync_run_id=?",
                (time.time(), run_id),
            )
        return {"status": "superseded"}
    page, phase = int(payload["page"]), payload.get("phase", "list")
    if phase == "list" and page != run["page"]:
        return {"status": "stale"}
    try:
        with service.client(user, binding) as client:
            if phase == "list":
                result = client.list_followings(int(binding["mid"]), page=page, page_size=50)
                entries, total = result["items"], result["total"]
                mids = []
                for entry in entries:
                    mid = entry.get("mid")
                    if type(mid) is not int or mid <= 0:
                        raise ValueError("InvalidFollowingIdentity")
                    mids.append(str(mid))
                with get_connection(service.db_path) as conn:
                    write(conn, "follow-sync-run", run_id)
                    current = conn.execute(
                        "SELECT * FROM follow_sync_runs WHERE sync_run_id=?", (run_id,)
                    ).fetchone()
                    if current["page"] != page or service.current_binding(user, conn) != binding:
                        return {"status": "superseded"}
                    seen = set(json.loads(current["seen_json"]))
                    previous_count = len(seen)
                    for entry in entries:
                        creator = service.creator(
                            conn, entry["mid"], entry.get("uname", ""), entry.get("face", "")
                        )
                        observe(
                            service, conn, user, binding, creator, True, started=run["created_at"]
                        )
                    seen.update(mids)
                    stable = current["total"] is None or current["total"] == total
                    complete = (
                        stable and len(seen) == total and (len(entries) < 50 or page * 50 >= total)
                    )
                    partial = (
                        not stable or page >= 200 or (not complete and len(seen) == previous_count)
                    )
                    status = "partial" if partial else "running"
                    conn.execute(
                        "UPDATE follow_sync_runs SET "
                        "page=?,total=?,seen_json=?,complete=?,status=?,updated_at=? WHERE "
                        "sync_run_id=?",
                        (
                            page + 1,
                            total,
                            encode(sorted(seen)),
                            int(complete),
                            status,
                            time.time(),
                            run_id,
                        ),
                    )
                    if complete:
                        missing = [
                            dict(row)
                            for row in conn.execute(
                                "SELECT r.creator_id,c.external_id FROM creator_follow_relations r "
                                "JOIN creators c ON c.creator_id=r.creator_id WHERE r.user_id=? "
                                "AND r.bili_account_mid=? AND r.binding_key=? AND "
                                "r.confirmed_state=1",
                                (user, binding["mid"], binding["binding_key"]),
                            )
                            if row["external_id"] not in seen
                        ]
                        conn.execute(
                            "UPDATE follow_sync_runs SET reconcile_json=? WHERE sync_run_id=?",
                            (encode(missing), run_id),
                        )
                        if missing:
                            enqueue_page(conn, user, run_id, 0, phase="verify")
                        else:
                            conn.execute(
                                "UPDATE follow_sync_runs SET status='completed' WHERE "
                                "sync_run_id=?",
                                (run_id,),
                            )
                    elif not partial:
                        enqueue_page(conn, user, run_id, page + 1)
                return {"status": "partial" if partial else "progress", "seen": len(seen)}
            missing = json.loads(run["reconcile_json"])
            for entry in missing[page : page + 8]:
                confirmed = client.get_follow_relation(int(entry["external_id"]))
                with get_connection(service.db_path) as conn:
                    observe(
                        service,
                        conn,
                        user,
                        binding,
                        entry["creator_id"],
                        confirmed,
                        started=run["created_at"],
                    )
            with get_connection(service.db_path) as conn:
                write(conn, "follow-sync-run", run_id)
                if page + 8 < len(missing):
                    enqueue_page(conn, user, run_id, page + 8, phase="verify")
                else:
                    conn.execute(
                        "UPDATE follow_sync_runs SET status='completed',updated_at=? WHERE "
                        "sync_run_id=?",
                        (time.time(), run_id),
                    )
            return {"status": "reconciled"}
    except Exception as error:
        auth_error = isinstance(error, APIError) and error.status_code in {401, 403}
        with get_connection(service.db_path) as conn:
            conn.execute(
                "UPDATE follow_sync_runs SET status=?,error_type=?,updated_at=? WHERE "
                "sync_run_id=?",
                (
                    "failed" if auth_error or job.get("attempts", 0) >= 8 else "running",
                    "BilibiliAuthExpired" if auth_error else type(error).__name__,
                    time.time(),
                    run_id,
                ),
            )
        if auth_error:
            raise JobPermanentFailure("FollowingListRequiresLogin") from None
        raise RuntimeError("FollowingListReadFailed") from None


def probe(service, job, payload):
    user = job["user_id"]
    binding = {"mid": payload["account_mid"], "binding_key": payload["binding_key"]}
    if service.current_binding(user) != binding:
        return {"status": "superseded"}
    with get_connection(service.db_path) as conn:
        creator = conn.execute(
            "SELECT * FROM creators WHERE creator_id=?", (payload["creator_id"],)
        ).fetchone()
    if not creator or creator["provider"] != "bili":
        raise JobPermanentFailure("UnsupportedCreator")
    started = time.time()
    with service.client(user, binding) as client:
        confirmed = client.get_follow_relation(int(creator["external_id"]))
    with get_connection(service.db_path) as conn:
        observe(service, conn, user, binding, creator["creator_id"], confirmed, started=started)
    return {"confirmed": confirmed}


def enqueue_probe(service, user, creator):
    binding = service.current_binding(user)
    if not binding:
        return
    state = service.state(user, creator)
    if state["syncStatus"] != "unknown" or state["relationVersion"]:
        return
    with get_connection(service.db_path) as conn:
        enqueue(
            conn,
            kind="creator_follow_probe",
            user_id=user,
            retry_safe=True,
            job_id="follow-probe:"
            + identity("probe", user, creator, binding, int(time.time() / 300)),
            payload={
                "creator_id": creator,
                "account_mid": binding["mid"],
                "binding_key": binding["binding_key"],
            },
        )
