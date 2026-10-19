"""Actual MinIO + SQLite backup/restore roundtrip in an isolated directory."""
from __future__ import annotations
import json,os,subprocess,tempfile
from dataclasses import replace
from pathlib import Path
from uuid import uuid4
from run_feed_e2e import ROOT,port,wait_until


def main():
    folder=Path(tempfile.mkdtemp(prefix="radio-backup-native-"))
    api,console=port(),port()
    access,secret="fixture"+uuid4().hex,uuid4().hex+uuid4().hex
    os.environ.update(APP_DATA_DIR=str(folder/"db"),AMEM_ENABLED="false",
        JOB_TRANSPORT="disabled",RABBITMQ_ENABLED="false",FEED_ENABLED="false",
        REQUIRE_DB_MIGRATION="0")
    report={"productionDataTouched":False,"checks":{}}
    log=(folder/"minio.log").open("wb")
    process=subprocess.Popen([str(ROOT/"minio.exe"),"server",str(folder/"objects"),
        "--address",f"127.0.0.1:{api}","--console-address",f"127.0.0.1:{console}"],
        env=dict(os.environ,MINIO_ROOT_USER=access,MINIO_ROOT_PASSWORD=secret),
        stdout=log,stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name=="nt" else 0)
    try:
        import requests
        wait_until(lambda:requests.get(f"http://127.0.0.1:{api}/minio/health/live",timeout=1).status_code==200,20)
        from feed_runtime.config import FeedConfig
        from feed_runtime.storage import MinIOStorage
        from feed_runtime.repository import FeedRepository,encode
        from feed_runtime.backup import create_backup,verify_backup,restore_backup
        from database import get_connection
        from models import Track
        from feed_runtime.media import MediaService
        config=replace(FeedConfig(),minio_endpoint=f"127.0.0.1:{api}",
            minio_public_endpoint=f"127.0.0.1:{api}",minio_access_key=access,
            minio_secret_key=secret,bucket="radio-backup-source",workdir=folder/"work")
        source=MinIOStorage(config);source.ensure_bucket()
        repo=FeedRepository(folder/"source.sqlite3")
        content=repo.admit(Track(bvid="BV0000000001",cid=1,title="Music Live",duration=12),user_id="legacy-owner")
        playback=MediaService(repo,config,source).create("legacy-owner",content,"one")
        entries={}
        for name,data in (("index.m3u8",b"#EXTM3U\n#EXT-X-ENDLIST\n"),("init.mp4",b"fixture-init")):
            path=folder/name;path.write_bytes(data)
            entries[name]=source.put_file("assets/fixture/version/"+name,path,content_type="application/octet-stream")
        with get_connection(repo.db_path) as conn:
            conn.execute("UPDATE media_assets SET status='ready',bytes=?,manifest_json=?",
                (sum(e["bytes"] for e in entries.values()),encode({"files":entries,"duration":12,"playlist":"#EXTM3U\n#EXT-X-ENDLIST\n"})))
        result=create_backup(repo,source,folder/"backup")
        report["checks"]["native_objects_downloaded_and_verified"]=result["objects"]==2 and bool(verify_backup(folder/"backup"))
        target=MinIOStorage(replace(config,bucket="radio-backup-restored"))
        restored=folder/"restored.sqlite3"
        restore_backup(folder/"backup",restored,target)
        with get_connection(restored) as conn:
            report["checks"]["sessions_revoked_and_jobs_fenced"]=(
                conn.execute("SELECT active FROM media_playbacks WHERE playback_id=?",(playback["playbackId"],)).fetchone()[0]==0
                and conn.execute("SELECT status FROM durable_jobs").fetchone()[0]=="needs_reconciliation")
        report["checks"]["restored_minio_objects_match"]=all(
            target.stat(e["key"])["sha256"]==e["sha256"] for e in entries.values())
        report["checks"]["manifest_mime_restored"]=target.client.stat_object(target.config.bucket,
            entries["index.m3u8"]["key"]).content_type=="application/vnd.apple.mpegurl"
        try:
            restore_backup(folder/"backup",folder/"rejected.sqlite3",source)
        except ValueError:
            report["checks"]["existing_source_bucket_refused"]=True
        else:
            report["checks"]["existing_source_bucket_refused"]=False
        report["passed"]=all(report["checks"].values())
        report["objects"]=result["objects"]
        (ROOT/"doc"/"feed-backup-native-2026-10-07.json").write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
        print(json.dumps(report))
        if not report["passed"]:
            raise SystemExit(1)
    finally:
        process.terminate()
        process.wait(timeout=10)
        log.close()


if __name__=="__main__":
    main()
