"""UGC and authoritative policy contracts on real isolated MySQL schemas."""

import inspect
from uuid import uuid4

import pymysql
import pytest
import test_ugc_lifecycle as contracts
import test_ugc_reactions as reaction_contracts
from mysql_fixture import NativeMySQL
from mysql_storage.connection import engine_for
from sqlalchemy.engine import make_url


@pytest.fixture(scope="module")
def mysql_server(tmp_path_factory):
    server = NativeMySQL(tmp_path_factory.mktemp("ugc-mysql-server"))
    yield server
    server.close()


@pytest.fixture
def mysql_studio(mysql_server, tmp_path, monkeypatch):
    name = "ugc_" + uuid4().hex
    base = make_url(mysql_server.url)
    administrator = pymysql.connect(
        host=base.host, port=base.port, user=base.username, password=base.password, autocommit=True
    )
    administrator.cursor().execute(
        f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
    )
    target = base.set(database=name).render_as_string(hide_password=False)
    try:
        yield from contracts.make_studio(tmp_path, monkeypatch, db_path=target)
    finally:
        engine_for(target).dispose()
        assert name.startswith("ugc_") and name[4:].isalnum()
        administrator.cursor().execute(f"DROP DATABASE `{name}`")
        administrator.close()


CASES = [
    contracts.test_text_draft_publish_edit_cas_outbox_and_no_fake_bvid,
    contracts.test_public_to_private_revokes_old_feed_detail_history_share_and_playback,
    contracts.test_private_media_cannot_be_published_and_nonowner_cannot_mutate,
    contracts.test_delete_is_tombstone_and_failed_outbox_rolls_back,
    contracts.test_concurrent_cas_edit_has_one_winner_and_batch_scope_is_current,
    contracts.test_source_revocation_blocks_media_but_owner_can_still_restrict_post,
    contracts.test_admin_post_retirement_never_retires_shared_asset_or_allows_owner_bypass,
    reaction_contracts.test_ugc_reactions_project_bitmap_without_legacy_track_or_alias_writes,
    reaction_contracts.test_reaction_does_not_bypass_current_media_dependency_permission,
]


@pytest.mark.parametrize("contract", CASES, ids=lambda contract: contract.__name__)
def test_ugc_mysql_contract(contract, mysql_studio, monkeypatch):
    values = {"studio": mysql_studio, "monkeypatch": monkeypatch}
    contract(**{name: values[name] for name in inspect.signature(contract).parameters})
