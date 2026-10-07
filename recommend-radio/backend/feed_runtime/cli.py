from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from database import DEFAULT_DB_PATH

from .maintenance import cleanup
from .services import FeedServices


def main():
    parser = argparse.ArgumentParser(description="Music Feed operational commands")
    parser.add_argument(
        "command",
        choices=(
            "health",
            "provision",
            "rebuild",
            "cleanup",
            "evaluate",
            "operations",
            "backup",
            "verify-backup",
            "restore",
        ),
    )
    parser.add_argument("--output", default="", help="Save the aggregate evaluation JSON")
    parser.add_argument(
        "--backup-dir", default="", help="Backup directory for verification or restore"
    )
    parser.add_argument("--target-db", default="", help="New database path for restore")
    parser.add_argument(
        "--target-mysql-env",
        default="",
        help="Environment variable holding a fresh target MySQL URL",
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--apply", action="store_true", help="Apply cleanup; default is a dry run")
    args = parser.parse_args()
    runtime = FeedServices(user_id="legacy-owner", db_path=args.db_path)
    if args.command in {"backup", "verify-backup", "restore"}:
        from .backup import create_backup, restore_backup, verify_backup

        if args.command == "backup":
            if not args.output:
                raise SystemExit("backup requires --output as a new directory")
            result = create_backup(runtime.repo, runtime.storage, args.output)
        elif args.command == "verify-backup":
            value = verify_backup(args.backup_dir)
            result = {"complete": True, "objects": len(value["objects"])}
        else:
            target_db = (
                os.getenv(args.target_mysql_env, "") if args.target_mysql_env else args.target_db
            )
            if not target_db:
                raise SystemExit("restore requires --target-db as a new path")
            result = restore_backup(args.backup_dir, target_db, runtime.storage)
    elif args.command == "operations":
        from .operations import FeedOperations

        result = FeedOperations(runtime).summary()
    elif args.command == "provision":
        if not runtime.config.minio_access_key or not runtime.config.minio_secret_key:
            raise SystemExit("Configure MinIO credentials before provisioning")
        runtime.storage.ensure_bucket()
        result = {"bucket": runtime.config.bucket, "ready": runtime.storage.health()}
    elif args.command == "health":
        result = runtime.health()
    elif args.command == "rebuild":
        result = runtime.reactions.rebuild()
    elif args.command == "evaluate":
        from .evaluation import evaluate

        result = evaluate(runtime.repo)
    else:
        result = cleanup(runtime.repo, runtime.config, runtime.storage, apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.output and args.command != "backup":
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
