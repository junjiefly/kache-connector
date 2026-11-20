#!/usr/bin/env python3
"""Build and verify the public KacheConnector package.

The package is built from packaging/public-files.txt. It intentionally uses an
allowlist rather than tar exclusions so future private files cannot leak if a
new directory is added to the repository.
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "packaging" / "public-files.txt"
PUBLIC_MAKEFILE = "packaging/Makefile.public"

REQUIRED_FILES = {
    ".gitignore",
    "LICENSE",
    "NOTICE",
    "README.md",
    "README.zh-CN.md",
    "go.mod",
    "go.sum",
    "packaging/Makefile.public",
    "tools/generate_proto.py",
    "tools/package_public.py",
    "transport/transport.go",
}

FORBIDDEN_PATH_PATTERNS = (
    re.compile(r"(^|/)private(/|$)"),
    re.compile(r"(^|/)build(/|$)"),
    re.compile(r"(^|/)\.git(/|$)"),
)

FORBIDDEN_CONTENT_PATTERNS = (
    ("private data-plane dependency", re.compile(r"rdma[-_]kit|github\.com/rdma-project", re.I)),
    ("private module import", re.compile(r"github\.com/junjiefly/kache-connector/private")),
    ("local development path", re.compile(r"[a-z]:\\work|/work/[a-z0-9_]+", re.I)),
    ("backend project name", re.compile(r"\bjo" + "ss\\b|jo" + "ss_dev", re.I)),
    ("internal host", re.compile(r"\b10\.237\.\d+\.\d+\b")),
    ("credential artifact", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
)

TEXT_SUFFIXES = {
    ".go", ".py", ".md", ".txt", ".mod", ".sum", ".proto", ".yml", ".yaml",
    ".json", ".toml", ".sh", ".bat", ".ps1", ".gitignore",
}


def read_manifest() -> list[str]:
    entries: list[str] = []
    seen: set[str] = set()
    for raw_line in MANIFEST.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        path = PurePosixPath(line)
        if path.is_absolute() or ".." in path.parts or str(path) != line:
            raise RuntimeError(f"unsafe manifest path: {line}")
        if line.startswith("private/") or line.startswith("build/"):
            raise RuntimeError(f"private/build path in manifest: {line}")
        if line in seen:
            raise RuntimeError(f"duplicate manifest path: {line}")
        seen.add(line)
        entries.append(line)
    missing = REQUIRED_FILES - seen
    if missing:
        raise RuntimeError("manifest is missing required files: " + ", ".join(sorted(missing)))
    return entries


def source_path(entry: str) -> Path:
    candidate = (ROOT / entry).resolve()
    try:
        candidate.relative_to(ROOT)
    except ValueError as exc:
        raise RuntimeError(f"manifest path escapes repository: {entry}") from exc
    return candidate


def validate_entries(entries: list[str]) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    for entry in entries:
        for pattern in FORBIDDEN_PATH_PATTERNS:
            if pattern.search(entry):
                raise RuntimeError(f"forbidden manifest path: {entry}")
        path = source_path(entry)
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f"manifest entry is not a regular file: {entry}")
        data = path.read_bytes()
        suffix = path.suffix.lower()
        if suffix in TEXT_SUFFIXES or entry == ".gitignore":
            text = data.decode("utf-8")
            for label, pattern in FORBIDDEN_CONTENT_PATTERNS:
                if pattern.search(text):
                    raise RuntimeError(f"forbidden content ({label}) in {entry}")
        contents[entry] = data
    return contents


def write_archive(output: Path, contents: dict[str, bytes]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(contents):
            data = contents[name]
            info = zipfile.ZipInfo(filename=name)
            info.external_attr = (0o755 if name.startswith("tools/") else 0o644) << 16
            archive.writestr(info, data)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "build" / "kache-connector-open.zip",
    )
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()

    entries = read_manifest()
    contents = validate_entries(entries)
    contents["Makefile"] = contents.pop(PUBLIC_MAKEFILE)
    if args.check_only:
        print(f"public package check passed ({len(contents)} files)")
        return 0
    if args.output.resolve().relative_to(ROOT).as_posix() in contents:
        raise RuntimeError("output archive cannot be included in itself")
    write_archive(args.output, contents)
    print(f"wrote {args.output} ({len(contents)} files)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
