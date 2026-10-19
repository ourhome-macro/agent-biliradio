"""Run the same HTTP-fixture contracts against isolated real MySQL 8.4 schemas."""

import inspect
from uuid import uuid4

import pymysql
import pytest
import test_following as contracts
from mysql_fixture import NativeMySQL
from mysql_storage.connection import engine_for
from sqlalchemy.engine import make_url


@pytest.fixture(scope="module")
def mysql_server(tmp_path_factory):
    server = NativeMySQL(tmp_path_factory.mktemp("following-mysql-server"))
    yield server
    server.close()


@pytest.fixture
def mysql_following(mysql_server, tmp_path, monkeypatch):
    name = "following_" + uuid4().hex
    base = make_url(mysql_server.url)
    administrator = pymysql.connect(
        host=base.host, port=base.port, user=base.username, password=base.password, autocommit=True
    )
    administrator.cursor().execute(
        f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
    )
    target = base.set(database=name).render_as_string(hide_password=False)
    try:
        yield from contracts.make_following(tmp_path, monkeypatch, db_path=target)
    finally:
        engine_for(target).dispose()
        assert name.startswith("following_") and name[10:].isalnum()
        administrator.cursor().execute(f"DROP DATABASE `{name}`")
        administrator.close()


CASES = [
    contracts.test_follow_command_queries_then_mutates_with_csrf_and_confirms,
    contracts.test_timeout_after_remote_commit_is_confirmed_without_second_write,
    contracts.test_old_task_and_inflight_ack_cannot_override_new_desired_state,
    contracts.test_binding_cookie_refresh_stable_logout_and_rebind_fence_old_jobs,
    contracts.test_unknown_and_ugc_sources_never_reach_bilibili,
    contracts.test_partial_list_does_not_remove_unseen_following,
    contracts.test_complete_list_requires_individual_absence_confirmation,
    contracts.test_busy_relation_retries_without_dropping_latest_command,
    contracts.test_unchanged_follow_is_noop_and_sync_does_not_erase_pending_intent,
    contracts.test_stale_profile_refresh_cannot_restore_previous_account,
    contracts.test_follow_outbox_native_stream_and_actual_dispatch_are_idempotent,
    contracts.test_delayed_confirmation_does_not_overwrite_later_bilibili_unfollow,
]


@pytest.mark.parametrize("contract", CASES, ids=lambda contract: contract.__name__)
def test_following_mysql_contract(contract, mysql_following, tmp_path, monkeypatch):
    values = {"following": mysql_following, "tmp_path": tmp_path, "monkeypatch": monkeypatch}
    contract(**{name: values[name] for name in inspect.signature(contract).parameters})
