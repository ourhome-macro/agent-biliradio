from __future__ import annotations

import time
from uuid import uuid4

from database import get_connection
from durable_jobs import enqueue
from error_code import APIError
from job_errors import JobPermanentFailure

from .service import FollowingService, encode, identity, write


class FollowSuperseded(Exception):
    pass


def _current(conn, service, user, binding, creator, version, token=None):
    if service.current_binding(user, conn) != binding:
        raise FollowSuperseded()
    row = conn.execute(
        "SELECT * FROM creator_follow_relations WHERE user_id=? AND bili_account_mid=? AND "
        "creator_id=?",
        (user, binding["mid"], creator),
    ).fetchone()
    if not row or row["binding_key"] != binding["binding_key"] or row["version"] != version:
        raise FollowSuperseded()
    if token and (row["lease_token"] != token or row["lease_until"] < time.time()):
        raise FollowSuperseded()
    return dict(row)


def execute_follow(service: FollowingService, job, payload, *, verify=False):
    user, creator = job["user_id"], payload["creator_id"]
    binding = {"mid": payload["account_mid"], "binding_key": payload["binding_key"]}
    token, version, posted, claimed = uuid4().hex, payload.get("version"), False, False
    observed_external = False
    relation_key = encode([user, binding["mid"], creator])
    try:
        with get_connection(service.db_path) as conn:
            write(conn, "follow-relation", relation_key)
            if verify:
                found = conn.execute(
                    "SELECT version FROM creator_follow_relations WHERE user_id=? AND "
                    "bili_account_mid=? AND creator_id=?",
                    (user, binding["mid"], creator),
                ).fetchone()
                version = found[0] if found else None
            row = _current(conn, service, user, binding, creator, version)
            latest_command = (
                conn.execute(
                    "SELECT MAX(version) FROM creator_follow_commands WHERE user_id=? "
                    "AND bili_account_mid=? AND binding_key=? AND creator_id=?",
                    (user, binding["mid"], binding["binding_key"], creator),
                ).fetchone()[0]
                or 0
            )
            read_only = (
                verify
                and row["sync_status"] == "synced"
                and latest_command <= payload.get("origin_version", latest_command)
            )
            if row["lease_token"] and row["lease_until"] > time.time():
                raise RuntimeError("FollowRelationBusy")
            creator_row = conn.execute(
                "SELECT * FROM creators WHERE creator_id=?", (creator,)
            ).fetchone()
            if not creator_row or creator_row["provider"] != "bili":
                raise JobPermanentFailure("UnsupportedCreatorProvider")
            conn.execute(
                "UPDATE creator_follow_relations SET lease_token=?,lease_until=? WHERE user_id=? "
                "AND bili_account_mid=? AND creator_id=?",
                (token, time.time() + 60, user, binding["mid"], creator),
            )
            claimed = True
        desired = bool(row["desired_state"])
        with service.client(user, binding) as client:
            confirmed = client.get_follow_relation(int(creator_row["external_id"]))
            if read_only and confirmed != desired:
                desired, observed_external = confirmed, True
            with get_connection(service.db_path) as conn:
                write(conn, "follow-relation", relation_key)
                _current(conn, service, user, binding, creator, version, token)
            if confirmed != desired:
                # Persist delayed verification before the external write, so a crash or
                # late server-side commit cannot silently escape reconciliation.
                with get_connection(service.db_path) as conn:
                    write(conn, "follow-relation", relation_key)
                    _current(conn, service, user, binding, creator, version, token)
                    for delay in (15, 120):
                        verify_id = "follow-verify:" + identity(
                            "verify", user, creator, binding, version, delay
                        )
                        enqueue(
                            conn,
                            kind="creator_follow_verify",
                            user_id=user,
                            retry_safe=True,
                            job_id=verify_id,
                            payload={
                                "creator_id": creator,
                                "account_mid": binding["mid"],
                                "binding_key": binding["binding_key"],
                                "origin_version": version,
                            },
                        )
                        conn.execute(
                            "UPDATE durable_jobs SET available_at=?,next_publish_at=? WHERE "
                            "job_id=? AND attempts=0",
                            (time.time() + delay, time.time() + delay, verify_id),
                        )
                posted = True
                try:
                    client.set_follow_relation(int(creator_row["external_id"]), desired)
                except APIError as error:
                    if error.status_code in {401, 403}:
                        raise
                    # A timeout is an unknown outcome. Read before any further write.
                confirmed = client.get_follow_relation(int(creator_row["external_id"]))
            if confirmed != desired:
                raise RuntimeError("FollowNotYetConfirmed")
        with get_connection(service.db_path) as conn:
            write(conn, "follow-relation", relation_key)
            _current(conn, service, user, binding, creator, version, token)
            conn.execute(
                "UPDATE creator_follow_relations SET desired_state=?,confirmed_state=?,"
                "version=?,sync_status='synced',"
                "last_verified_at=?,error_type=NULL,updated_at=? WHERE user_id=? AND "
                "bili_account_mid=? AND creator_id=?",
                (
                    int(desired),
                    int(confirmed),
                    version + int(observed_external),
                    time.time(),
                    time.time(),
                    user,
                    binding["mid"],
                    creator,
                ),
            )
        return {
            "status": "synced",
            "version": version + int(observed_external),
            "following": confirmed,
        }
    except FollowSuperseded:
        return {"status": "superseded"}
    except Exception as error:
        if not claimed:
            raise
        permanent = isinstance(error, JobPermanentFailure) or (
            isinstance(error, APIError) and error.status_code in {401, 403}
        )
        with get_connection(service.db_path) as conn:
            write(conn, "follow-relation", relation_key)
            try:
                _current(conn, service, user, binding, creator, version, token)
            except FollowSuperseded:
                return {"status": "superseded"}
            status = "failed" if permanent or job.get("attempts", 0) >= 8 else "unknown"
            conn.execute(
                "UPDATE creator_follow_relations SET sync_status=?,error_type=?,updated_at=? "
                "WHERE user_id=? AND bili_account_mid=? AND creator_id=? AND lease_token=?",
                (
                    status,
                    "BilibiliAuthExpired" if permanent else type(error).__name__,
                    time.time(),
                    user,
                    binding["mid"],
                    creator,
                    token,
                ),
            )
        if permanent:
            raise JobPermanentFailure("FollowingRequiresBilibiliLogin") from None
        raise RuntimeError("FollowingOutcomeUnknown" if posted else "FollowingReadFailed") from None
    finally:
        with get_connection(service.db_path) as conn:
            write(conn, "follow-relation", relation_key)
            conn.execute(
                "UPDATE creator_follow_relations SET lease_token=NULL,lease_until=0 "
                "WHERE user_id=? AND bili_account_mid=? AND creator_id=? AND lease_token=?",
                (user, binding["mid"], creator, token),
            )
