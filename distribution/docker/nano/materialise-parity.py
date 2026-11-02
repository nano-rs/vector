#!/usr/bin/env python3
"""Regenerate distribution/docker/nano/parity/ch-base/ from nanosiem's Vector configs.

nano/v0.61 serves the single-tenant ClickHouse product (nanosiem `clickhouse/*`, NAN-4904).
Its parity units are:

  ch-base/            nanosiem config/vector at a ref, as the compose runs it: the top-level
                      static TOMLs plus sources/configs/*.toml and sources/parsers/*.toml (this
                      script). `${NAME:-default}` env references are replaced by their default
                      (or a `parity-<name>` literal), because the workflow validates without
                      --dangerously-allow-env-var-interpolation.
  opt-in-templates/   the base's opt-in source templates (sources/{file,syslog,http}.toml.disabled)
                      as an operator enables them, plus a blackhole consuming every template source
  classic-*.toml      classic source generator output (hand-written, source_configs shape):
                      kafka SASL_SSL, aws_s3 static keys, gcp_pubsub credentials_path, and the
                      fork's azure_event_hubs source
  open-ingest-tokens.toml  valid_tokens_file on http_server / splunk_hec (NAN-3770)
  enrichment/         hand-written mmdb + geoip + CSV file tables read by
                      get_enrichment_table_record; fixture data in enrichment/data/
  harness.toml        stdin / demo_logs harness shapes

Usage (from the repository root):

  distribution/docker/nano/materialise-parity.py ~/Projects/nanosiem@origin/clickhouse/dev

REF may be WORKTREE to read the checked-out files instead of a commit.
Python 3 standard library only.
"""

from __future__ import annotations

import io
import re
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

PARITY = Path(__file__).resolve().parent / "parity"
UNIT = PARITY / "ch-base"
ENV = re.compile(r"\$\{([A-Z0-9_]+)(?::?-([^}]*))?\}")
HEADER = (
    "# Parity unit rendered by materialise-parity.py from nanosiem {ref}:config/vector/{rel};\n"
    "# env references replaced by their defaults (or a parity- literal).\n"
)


def render(text: str) -> str:
    return ENV.sub(
        lambda m: m.group(2) if m.group(2) is not None else "parity-" + m.group(1).lower(), text
    )


def read_tree(repo: Path, ref: str) -> dict[str, str]:
    """config/vector files (path relative to config/vector -> text) at `ref`."""
    if ref == "WORKTREE":
        base = repo / "config" / "vector"
        return {
            str(p.relative_to(base)): p.read_text()
            for p in base.rglob("*.toml")
            if p.is_file()
        }
    blob = subprocess.run(
        ["git", "-C", str(repo), "archive", ref, "config/vector"],
        check=True,
        capture_output=True,
    ).stdout
    out = {}
    with tarfile.open(fileobj=io.BytesIO(blob)) as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith(".toml"):
                rel = member.name[len("config/vector/"):]
                out[rel] = tar.extractfile(member).read().decode()
    return out


def main() -> int:
    if len(sys.argv) != 2 or "@" not in sys.argv[1]:
        print(__doc__, file=sys.stderr)
        return 2
    path, ref = sys.argv[1].rsplit("@", 1)
    tree = read_tree(Path(path).expanduser(), ref)
    picked: list[tuple[str, str]] = []
    for rel in sorted(tree):
        if "/" not in rel:
            picked.append((rel, rel))
        elif re.fullmatch(r"sources/configs/[^/]+\.toml", rel):
            picked.append((rel, "configs-" + rel.rsplit("/", 1)[1]))
        elif re.fullmatch(r"sources/parsers/[^/]+\.toml", rel):
            picked.append((rel, "parsers-" + rel.rsplit("/", 1)[1]))
    if not picked:
        print("no config/vector TOMLs found", file=sys.stderr)
        return 1
    if UNIT.exists():
        shutil.rmtree(UNIT)
    UNIT.mkdir(parents=True)
    for rel, name in picked:
        (UNIT / name).write_text(HEADER.format(ref=ref, rel=rel) + render(tree[rel]))
    print(f"wrote {len(picked)} files to {UNIT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
