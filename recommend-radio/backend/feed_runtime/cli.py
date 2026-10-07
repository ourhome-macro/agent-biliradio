from __future__ import annotations

import argparse
import json
from pathlib import Path

from database import DEFAULT_DB_PATH

from .maintenance import cleanup
from .services import FeedServices


def main():
    parser = argparse.ArgumentParser(description="Music Feed operational commands")
    parser.add_argument(
        "command", choices=("health", "provision", "rebuild", "cleanup", "evaluate")
    )
    parser.add_argument("--output", default="", help="Save the aggregate evaluation JSON")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--apply", action="store_true", help="Apply cleanup; default is a dry run")
    args = parser.parse_args()
    runtime = FeedServices(user_id="legacy-owner", db_path=args.db_path)
    if args.command == "provision":
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
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
