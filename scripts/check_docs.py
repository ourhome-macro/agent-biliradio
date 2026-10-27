"""Enforce a bounded Markdown document and resolvable repository file links."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
MAX_CHARACTERS = 8_000
LINK = re.compile(r"!?\[[^\]\n]*\]\((<[^>]+>|[^\s)]+)(?:\s+\"[^\"]*\")?\)")


def repository_documents() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.md"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return sorted({ROOT / p.decode("utf-8") for p in result.stdout.split(b"\0") if p})


def inspect_document(path: Path, *, root: Path = ROOT) -> list[str]:
    text = path.read_text(encoding="utf-8")
    errors = []
    # Text mode normalizes CRLF. All characters, including spaces and code, count.
    if len(text) > MAX_CHARACTERS:
        errors.append(f"{len(text)} characters; maximum is {MAX_CHARACTERS}; split by topic")
    if "\ufffd" in text or text.startswith("\ufeff"):
        errors.append("use UTF-8 without BOM or replacement characters")
    fence = None
    for number, line in enumerate(text.splitlines(), 1):
        marker = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if marker:
            current = marker[1]
            if fence is None:
                fence = current
            elif current[0] == fence[0] and len(current) >= len(fence):
                fence = None
            continue
        if fence:
            continue
        for match in LINK.finditer(line):
            target = match[1].strip("<>")
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            local = unquote(parsed.path)
            resolved = root / local.lstrip("/") if local.startswith("/") else path.parent / local
            if not resolved.exists():
                errors.append(f"line {number}: missing local link: {target}")
    return errors


def check(paths: list[Path] | None = None) -> int:
    documents = repository_documents() if paths is None else paths
    failures = []
    for path in documents:
        if not path.is_file() or path.suffix.lower() != ".md":
            continue
        try:
            errors = inspect_document(path)
        except UnicodeError:
            errors = ["document is not UTF-8"]
        failures.extend(f"{path.relative_to(ROOT)}: {error}" for error in errors)
    for failure in failures:
        print(failure)
    print(f"Documentation contracts: {len(documents)} files, {len(failures)} failures")
    return int(bool(failures))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    raise SystemExit(check([ROOT / p for p in args.files] if args.files else None))
