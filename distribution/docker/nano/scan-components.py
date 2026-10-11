#!/usr/bin/env python3
"""Inventory the Vector component types nano ships or renders (NAN-3604).

Reads git trees at fixed refs (`git ls-tree` / `git show`, never the working tree, never a
checkout) and collects, from every Vector config found:

  source / transform / sink / enrichment_table / secret component types,
  encoding / decoding / batch_encoding codecs, framing methods, compression, buffer types,
  the [api] table, SECRET[...] references.

Configs are found in TOML and YAML files, inside .tar.gz bundles (recursively), and inside
string literals of renderer code and tests (Rust, TS/JS, Python, shell): the scanner reads
config-shaped lines (`[sinks.x]`, `type = "remap"`, `type: http`) wherever they appear, and
ignores assertion lines such as `contains("type = \\"kafka\\"")`. A type rendered from a
variable (`type = "{}"`) is reported as dynamic with its location; its values are
resolved from the enum/list that drives the renderer (RENDERED_DYNAMIC below). The classic
open-core source generator builds its configs at runtime with toml::Table, so its types are
read from the generator code itself (CLASSIC_SOURCES below).

Usage (each repo as NAME=PATH@REF; NAME selects the include/exclude rules below):

  scan-components.py nanosiem=~/Projects/nanosiem@origin/main \
                     nano-main=~/Projects/nano-main@origin/main \
                     parsers=~/.cache/parsers@origin/main \
                     vector=.@HEAD

  REF may be WORKTREE to read the checked-out files instead of a commit.

  --json      machine-readable output
  --check F   exit 1 if a shipped or KEPT component (source/transform/sink/enrichment_table)
              is missing from COMPONENTS file F (the fork's expected `vector list` output), if a
              RETIRED type is rendered inside its retired scope (hosted pull renderers and
              fixtures; RETIRED_FIXTURES excepted), or if a dynamic or classic-generator type
              is unresolved

Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import tarfile
from collections import defaultdict

# --- what to read, per repository -----------------------------------------------------------

# Path prefixes scanned per known repo name; anything else in the repo is ignored.
INCLUDE = {
    "nanosiem": [
        "config/vector/",
        "deploy/",
        "docker-compose",
        "nanosiem-core/src/",
        "nanosiem-core/tests/",
        "nanosiem-api/src/",
        "nanosiem-lake-worker/tests/",
        "workers/hosted-ingest/",
        "scripts/",
        "ludus/",
        "nanosiem-web/src/lib/",
    ],
    "nano-main": ["apps/api/src/", "deploy/", "tools/", "packages/"],
    "parsers": [""],
    "vector": ["distribution/docker/nano/"],
}

# Prefixes or substrings that are configs for Vector running on CUSTOMER hosts (upstream
# Vector, not this image), docs, or build output. They are scanned and reported separately,
# never counted as shipped.
EXCLUDE = {
    "nanosiem": [
        ("nanosiem-web/", "sender template (customer-side Vector)"),
        ("nanosiem-core/src/ingest_sender_templates.rs", "sender template (customer-side Vector)"),
        ("ludus/", "Ludus lab range (endpoint agents and aggregator run upstream Vector)"),
        ("docs", "documentation"),
    ],
    "nano-main": [
        ("tools/doc-vectorize/", "docs tooling"),
        ("tools/generate-product-docs.ts", "docs tooling"),
        ("tools/product-doc-vectorize/", "docs tooling"),
        (".wrangler/", "build output"),
        ("docs", "documentation"),
    ],
    "parsers": [],
    "vector": [],
}

# Opt-in templates shipped in the base bundle (`sources/file.toml.disabled`): an operator
# enables one by renaming it, so their types are shipped like any other config.
DISABLED_SUFFIXES = (".disabled",)

TEXT_EXT = (
    ".toml", ".toml.j2", ".toml.in", ".yaml", ".yml", ".yaml.in", ".yml.in", ".j2",
    ".rs", ".ts", ".js", ".mjs", ".cjs", ".py", ".sh", ".tpl",
)
ARCHIVE_EXT = (".tar.gz", ".tgz")
SKIP_EXT = (".md", ".mdx", ".json", ".lock", ".snap.json")

# Renderers that emit a component type from a variable (`type = "{}"`, `type = "${s.kind}"`).
# The values are read from the enum/list that drives the renderer, in the same repo and ref, so
# a new kind there shows up in the inventory without editing this table. A dynamic site that
# matches no entry is reported as UNRESOLVED (and fails --check).
RENDERED_DYNAMIC = [
    {
        "repo": "nanosiem",
        "sites": ("nanosiem-core/src/parsers/tenant_bundle/pull.rs",
                  "nanosiem-core/src/parsers/tenant_bundle/pull_tests.rs"),
        "kind": "source",
        "values_file": "nanosiem-core/src/parsers/tenant_bundle/model.rs",
        "scope": r"impl PullSourceKind\b.*?\n}",
        "pattern": r'Self::\w+\s*=>\s*"([a-z0-9_]+)"',
    },
    {
        "repo": "nano-main",
        "sites": ("apps/api/src/services/shared-infrastructure/pull-bundle.ts",),
        "kind": "source",
        "values_file": "apps/api/src/services/shared-infrastructure/pull-bundle.ts",
        "scope": r"PULL_SOURCE_KINDS\s*=\s*\[[^\]]*\]",
        "pattern": r'"([a-z0-9_]+)"',
    },
]

# Classic (open-core) source generator: types emitted at RUNTIME from toml::Table code
# (`source.insert("type".into(), "kafka".into())`), never committed as TOML. Read from code so a
# new generator or variant cannot be missed:
#   emit   every match of `pattern` inside `scope` is a shipped `kind` type;
#   cover  every match must be an emitted type, a system intermediary (`system` scope: a source
#          declared in config/vector/*.toml), or a CLASSIC_ALIASES key; otherwise UNRESOLVED.
# A listed file that exists but whose scope no longer matches is UNRESOLVED too.
CLASSIC_SOURCES = [
    {
        "repo": "nanosiem", "mode": "emit", "kind": "source",
        "file": "nanosiem-core/src/source_configs/service.rs",
        "scope": r"\n    (?:async )?fn generate_\w+_source\b.*?(?=\n    (?:pub(?:\(\w+\))? )?(?:async )?fn |\Z)",
        "pattern": r'\.insert\(\s*"type"\.into\(\),\s*"([a-z0-9_]+)"\.into\(\)\s*\)',
    },
    {
        # Credential providers a classic source can be bound to: each is a Vector source type.
        "repo": "nanosiem", "mode": "emit", "kind": "source",
        "file": "nanosiem-core/src/enterprise_shared_sources_credentials.rs",
        "scope": r"SUPPORTED_PROVIDERS\s*:[^=]*=\s*&\[[^\]]*\]",
        "pattern": r'"([a-z0-9_]+)"',
    },
    {
        "repo": "nanosiem", "mode": "system", "kind": "source",
        "file": "nanosiem-core/src/source_configs/service.rs",
        "scope": r"fn system_intermediary_source\b.*?\n    }",
        "pattern": r'"([a-z0-9_]+)"\s*(?:\||=>)',
    },
    {
        "repo": "nanosiem", "mode": "cover", "kind": "source",
        "file": "nanosiem-core/src/log_sources/types.rs",
        "scope": r"impl SourceType \{.*?fn as_str\(&self\)[^{]*\{.*?\n    }",
        "pattern": r'Self::\w+\s*=>\s*"([a-z0-9_]+)"',
    },
]
# SourceType values that are not a generator config_type of their own.
CLASSIC_ALIASES = {"routed": "http (system listener, config/vector)"}
CLASSIC: dict = {}   # label -> {"emit": {name: file}, "system": set, "cover": {name: file}, "broken": [..]}

# Types this image does not build for a given renderer family. Retirement is SCOPED: rendering
# one inside a listed scope is an ERROR under --check (it must fail at render/build time, never at
# config load), while the same type elsewhere (e.g. the classic open-core generator) is shipped.
RETIRED = {
    ("source", "kafka"): {
        "why": "hosted pull: the never-shipped Azure-over-Kafka kind, replaced by azure_event_hubs "
               "(NAN-3604). Classic/open-core Kafka sources still ship.",
        "scopes": (
            ("nanosiem", "nanosiem-core/src/parsers/tenant_bundle/pull"),
            ("nano-main", "apps/api/src/services/shared-infrastructure/pull-bundle"),
            ("nano-main", "apps/api/src/services/shared-infrastructure/test-fixtures/nan3381-pull-bundle/"),
        ),
    },
}
# Frozen fixtures in a retired scope that are tolerated until regenerated. Empty: nano-main#2131
# regenerated the nan3381 hosted pull fixtures without kafka.
RETIRED_FIXTURES: tuple = ()

# Built although no scanned config uses them yet. Listed so the inventory says why.
KEPT = {
    ("enrichment_table", "file"): "kept for NAN-3489 (all enrichment traverses Vector); built in",
    ("enrichment_table", "geoip"): "kept for NAN-3489 (all enrichment traverses Vector)",
    ("enrichment_table", "mmdb"): "kept for NAN-3489 (lake-lane IPinfo MMDB lookup may move into VRL)",
    ("enrichment_table", "memory"): "kept for NAN-3489 (all enrichment traverses Vector)",
}

DYNAMIC_VALUES: dict = {}

COMPONENT_SECTIONS = {
    "sources": "source",
    "transforms": "transform",
    "sinks": "sink",
    "enrichment_tables": "enrichment_table",
    "secret": "secret",
}

# --- git access -----------------------------------------------------------------------------


def git(repo: str, *args: str) -> bytes:
    return subprocess.run(
        ["git", "-C", repo, *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).stdout


# REF may be the literal WORKTREE: the checked-out files (tracked + untracked, not ignored),
# for pre-commit use by the guard tests in nanosiem / nano-main.
WORKTREE = "WORKTREE"


def list_files(repo: str, ref: str) -> list[str]:
    if ref == WORKTREE:
        out = git(repo, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
        return sorted(p for p in set(out.decode().split("\0")) if p and os.path.isfile(os.path.join(repo, p)))
    out = git(repo, "ls-tree", "-r", "--name-only", "-z", ref)
    return [p for p in out.decode().split("\0") if p]


def read_blob(repo: str, ref: str, path: str) -> bytes:
    if ref == WORKTREE:
        with open(os.path.join(repo, path), "rb") as f:
            return f.read()
    return git(repo, "show", f"{ref}:{path}")


# --- config-line extraction -----------------------------------------------------------------

STRING_LINE = re.compile(r"""^\s*(?:r#*)?(["'`])(.*)\1#*\s*[,;+)]*\s*$""")
ASSERTION = re.compile(r"\b(contains|assert|expect|includes|toContain|toMatch|match|find|replace|starts_with|ends_with)\b")


def candidate_texts(text: str, is_code: bool) -> list[str]:
    """Return config-shaped text views of a file.

    Config files are used as-is. Code files additionally yield a view where string literals
    and escape sequences are unwrapped, so config embedded in renderers and tests is visible.
    """
    if not is_code:
        return [text]
    lines = []
    for raw in text.splitlines():
        if ASSERTION.search(raw):
            continue
        line = raw
        m = STRING_LINE.match(line)
        if m:
            line = m.group(2)
        # Rust/JS escapes inside string literals; continuation backslashes.
        line = line.replace("\\n\\", "\n").replace("\\n", "\n").replace('\\"', '"').replace("\\x20", " ")
        line = re.sub(r"\\\s*$", "", line)
        lines.extend(line.split("\n"))
    return [text, "\n".join(lines)]


TOML_HEADER = re.compile(r"""^\s*\[(?!\[)\s*([^\]]+?)\s*\]\s*(?:#.*)?$""")
TOML_KV = re.compile(r"""^\s*([A-Za-z0-9_"'.\-{}% ]+?)\s*=\s*(.+?)\s*$""")
YAML_KV = re.compile(r"""^(\s*)(?:-\s+)?([A-Za-z0-9_.\-"']+):\s*(.*?)\s*$""")
STRING_VALUE = re.compile(r"""^(["'])(.*?)\1\s*(?:#.*)?,?$""")
INLINE_PAIR = re.compile(r"""([A-Za-z0-9_.]+)\s*=\s*["']([^"']*)["']""")


def split_key(key: str) -> list[str]:
    parts = re.findall(r'"[^"]*"|\'[^\']*\'|[^.]+', key)
    return [p.strip().strip("\"'") for p in parts if p.strip()]


def scalar(value: str) -> str | None:
    value = value.strip()
    m = STRING_VALUE.match(value)
    if m:
        return m.group(2)
    if re.fullmatch(r"[A-Za-z0-9_\-]+", value):
        return value
    return None


TOML_ARRAY_HEADER = re.compile(r"""^\s*\[\[\s*([^\]]+?)\s*\]\]""")
STRICT_KEY = re.compile(r"""^[A-Za-z0-9_"'.\-{}%]+$""")


def toml_pairs(text: str, strict: bool):
    """Yield (path, value) from TOML-shaped lines; skips multi-line string bodies (VRL).

    strict (code views): any line that is not config-shaped ends the current table, so a
    header in one string literal never leaks onto unrelated code further down.
    """
    table: list[str] = []
    in_multiline = None
    for line in text.splitlines():
        if in_multiline:
            if in_multiline in line:
                in_multiline = None
            continue
        if TOML_ARRAY_HEADER.match(line):
            # [[tests]], [[tests.inputs]]: Vector unit-test tables, not components.
            table = ["<array-table>"]
            continue
        m = TOML_HEADER.match(line)
        if m:
            table = split_key(m.group(1))
            continue
        m = TOML_KV.match(line)
        if not m or (strict and not STRICT_KEY.match(m.group(1))):
            if strict and line.strip() and not line.strip().startswith("#"):
                table = []
            continue
        key, value = m.group(1), m.group(2)
        for quote in ("'''", '"""'):
            if value.startswith(quote) and value.count(quote) == 1:
                in_multiline = quote
        if in_multiline:
            continue
        path = table + split_key(key)
        if value.startswith("{"):
            for k, v in INLINE_PAIR.findall(value):
                yield path + split_key(k), v
            continue
        v = scalar(value)
        if v is not None:
            yield path, v


def yaml_pairs(text: str, strict: bool):
    """Yield (path, value) from YAML-shaped lines by indentation; skips block scalars."""
    stack: list[tuple[int, str]] = []
    block_indent = None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if block_indent is not None:
            if indent > block_indent:
                continue
            block_indent = None
        m = YAML_KV.match(line)
        if not m:
            if strict:
                stack = []
            continue
        indent = len(m.group(1))
        key, value = m.group(2).strip("\"'"), m.group(3)
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = [k for _, k in stack] + [key]
        if value in ("", "|", ">", "|-", ">-", "|+", ">+"):
            stack.append((indent, key))
            if value:
                block_indent = indent
            continue
        v = scalar(value)
        if v is not None:
            yield path, v


def classify(path: list[str], value: str):
    """Map one (path, value) pair to (kind, name) or None."""
    # Find where a Vector config starts inside the path (YAML embedding may prefix it).
    for i, part in enumerate(path):
        if part in COMPONENT_SECTIONS:
            p = path[i:]
            break
        if part == "api" and path[i:] == ["api", "enabled"]:
            return ("api", f"enabled={value}")
    else:
        return None
    section = COMPONENT_SECTIONS[p[0]]
    if len(p) == 3 and p[2] == "type":
        return (section, value)
    if len(p) < 3:
        return None
    rest = p[2:]
    if rest[-1] == "codec" and len(rest) >= 2 and rest[-2] in ("encoding", "decoding", "batch_encoding"):
        return (rest[-2], value)
    if rest[-1] == "method" and "framing" in rest:
        return ("framing", value)
    if rest[-1] == "compression" and len(rest) == 1:
        return ("compression", value)
    if rest[-1] == "algorithm" and len(rest) >= 2 and rest[-2] == "compression":
        return ("compression", value)
    if rest[-1] == "type" and "buffer" in rest:
        return ("buffer", value)
    if rest[-1] == "azure_credential_kind":
        return ("azure_credential", value)
    if rest[-1] == "type" and len(rest) == 2:
        return (f"{section}.{rest[0]}", value)
    return None


DYNAMIC = re.compile(r"\{[^}]*\}|\$\{")


def scan_text(text: str, is_code: bool):
    found = set()
    for n, view in enumerate(candidate_texts(text, is_code)):
        strict = is_code
        for parser in (toml_pairs, yaml_pairs):
            for path, value in parser(view, strict):
                hit = classify(path, value)
                if hit:
                    kind, name = hit
                    if DYNAMIC.search(name):
                        name = "<dynamic>"
                    found.add((kind, name))
    for m in re.finditer(r"SECRET\[([A-Za-z0-9_]+)\.", text):
        found.add(("secret_ref", m.group(1)))
    return found


VRL_ENRICH = re.compile(r"\b(get_enrichment_table_record|find_enrichment_table_records)\b")


def scan_parser_yaml(text: str):
    """nano-rs/parsers: parser.yaml files are VRL lanes rendered as remap transforms."""
    found = set()
    if re.search(r"^parser_vrl:", text, re.M):
        found.add(("parser_lane", "remap"))
    if VRL_ENRICH.search(text):
        found.add(("vrl_enrichment_lookup", "used"))
    return found


# --- walking --------------------------------------------------------------------------------


def exclusion(name: str, path: str) -> str | None:
    for needle, why in EXCLUDE.get(name, []):
        if path.startswith(needle) or f"/{needle}" in path:
            return why
    return None


def wanted(name: str, path: str) -> bool:
    prefixes = INCLUDE.get(name, [""])
    return any(path.startswith(p) for p in prefixes)


def scan_blob(label: str, path: str, data: bytes, results, depth=0):
    lower = path.lower()
    if lower.endswith(ARCHIVE_EXT) and depth < 3:
        try:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
                for member in tar.getmembers():
                    if member.isfile():
                        f = tar.extractfile(member)
                        if f:
                            scan_blob(label, f"{path}#{member.name.lstrip('./')}", f.read(), results, depth + 1)
        except (tarfile.TarError, OSError, EOFError):
            pass
        return
    if lower.endswith(SKIP_EXT) or not lower.endswith(TEXT_EXT + DISABLED_SUFFIXES):
        return
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return
    if lower.endswith(DISABLED_SUFFIXES):
        for kind, n in scan_text(text, False):
            results[(None, kind, n)].add(f"{label}:{path} (opt-in template)")
        return
    is_code = lower.endswith((".rs", ".ts", ".js", ".mjs", ".cjs", ".py", ".sh"))
    hits = scan_text(text, is_code)
    if label.startswith("parsers@") and lower.endswith("parser.yaml"):
        hits |= scan_parser_yaml(text)
    for kind, n in hits:
        results[(None, kind, n)].add(f"{label}:{path}")


def scan_repo(spec: str, results, excluded):
    name, _, rest = spec.partition("=")
    repo, _, ref = rest.rpartition("@")
    repo = os.path.expanduser(repo or ".")
    if ref == WORKTREE:
        label = f"{name}@worktree"
    else:
        label = f"{name}@{git(repo, 'rev-parse', '--short=12', ref).decode().strip()}"
    classic = CLASSIC.setdefault(label, {"emit": {}, "system": set(), "cover": {}, "broken": []})
    for entry in CLASSIC_SOURCES:
        if entry["repo"] != name:
            continue
        try:
            text = read_blob(repo, ref, entry["file"]).decode()
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
        scopes = re.findall(entry["scope"], text, re.S)
        values = [v for sc in scopes for v in re.findall(entry["pattern"], sc)]
        if not values:
            classic["broken"].append(f'{label}:{entry["file"]} ({entry["mode"]} scope matched nothing)')
        for v in values:
            if entry["mode"] == "system":
                classic["system"].add(v)
            else:
                classic[entry["mode"]].setdefault(v, entry["file"])
    for entry in RENDERED_DYNAMIC:
        if entry["repo"] != name:
            continue
        try:
            text = read_blob(repo, ref, entry["values_file"]).decode()
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
        scope = re.search(entry["scope"], text, re.S)
        values = re.findall(entry["pattern"], scope.group(0)) if scope else []
        DYNAMIC_VALUES[(label, entry["kind"])] = (entry["sites"], sorted(set(values)), entry["values_file"])
    for path in list_files(repo, ref):
        if not wanted(name, path):
            continue
        lower = path.lower()
        if not lower.endswith(TEXT_EXT + ARCHIVE_EXT + DISABLED_SUFFIXES):
            continue
        why = exclusion(name, path)
        data = read_blob(repo, ref, path)
        if why:
            sub = defaultdict(set)
            scan_blob(label, path, data, sub)
            for (_, kind, n), locs in sub.items():
                excluded[(why, kind, n)] |= locs
            continue
        scan_blob(label, path, data, results)
    return label


LIST_SECTIONS = {"Sources:": "source", "Transforms:": "transform", "Sinks:": "sink",
                 "Enrichment tables:": "enrichment_table"}


def read_components(path: str) -> set:
    """(kind, name) pairs from a `vector list` text file; `#` lines are comments."""
    out, kind = set(), None
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if line in LIST_SECTIONS:
                kind = LIST_SECTIONS[line]
            elif line.startswith("- ") and kind:
                out.add((kind, line[2:].strip()))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("repos", nargs="+", help="NAME=PATH@REF")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--check", metavar="COMPONENTS", help="fail if a shipped component is not in this file")
    ap.add_argument("--max-locations", type=int, default=3)
    args = ap.parse_args()

    results: dict = defaultdict(set)
    excluded: dict = defaultdict(set)
    labels = [scan_repo(spec, results, excluded) for spec in args.repos]

    shipped: dict = defaultdict(set)
    for (_, kind, n), locs in results.items():
        shipped[(kind, n)] |= locs

    # Expand known dynamic renderers; report unknown dynamic sites.
    unresolved = []
    for (kind, n), locs in list(shipped.items()):
        if n != "<dynamic>":
            continue
        del shipped[(kind, n)]
        for loc in sorted(locs):
            label, _, path = loc.partition(":")
            entry = DYNAMIC_VALUES.get((label, kind))
            if entry and path.split("#")[0] in entry[0] and entry[1]:
                for v in entry[1]:
                    shipped[(kind, v)].add(f"{loc} (rendered; values from {entry[2]})")
            else:
                unresolved.append((kind, loc))
    # Classic generator: emitted types are shipped; every SourceType value must be covered.
    for label, c in CLASSIC.items():
        for v, f in c["emit"].items():
            shipped[("source", v)].add(f"{label}:{f} (classic generator, runtime)")
        for v, f in c["cover"].items():
            if v not in c["emit"] and v not in c["system"] and v not in CLASSIC_ALIASES:
                unresolved.append(("source", f"{label}:{f} SourceType value {v!r} has no generator"))
        for b in c["broken"]:
            unresolved.append(("source", f"{b}: update CLASSIC_SOURCES"))

    retired: dict = {}
    retired_fixture: dict = {}
    for key in [k for k in shipped if k in RETIRED]:
        for loc in sorted(shipped[key]):
            label, _, path = loc.partition(":")
            repo = label.split("@")[0]
            if not any(repo == r and path.startswith(pre) for r, pre in RETIRED[key]["scopes"]):
                continue
            shipped[key].discard(loc)
            tolerated = any(repo == r and path.startswith(pre) for r, pre in RETIRED_FIXTURES)
            (retired_fixture if tolerated else retired).setdefault(key, set()).add(loc)
        if not shipped[key]:
            del shipped[key]

    if args.json:
        json.dump(
            {
                "repos": labels,
                "shipped": [{"kind": k, "name": n, "seen_in": sorted(l)} for (k, n), l in sorted(shipped.items())],
                "kept": [{"kind": k, "name": n, "why": w, "seen_in": sorted(shipped.get((k, n), ()))}
                         for (k, n), w in sorted(KEPT.items())],
                "retired_fixture": [{"kind": k, "name": n, "seen_in": sorted(l)} for (k, n), l in sorted(retired_fixture.items())],
                "excluded": [{"reason": r, "kind": k, "name": n, "seen_in": sorted(l)} for (r, k, n), l in sorted(excluded.items())],
                "retired": [{"kind": k, "name": n, "why": RETIRED[(k, n)]["why"], "seen_in": sorted(l)} for (k, n), l in sorted(retired.items())],
                "unresolved_dynamic": [{"kind": k, "seen_in": l} for k, l in unresolved],
            },
            sys.stdout,
            indent=1,
        )
        print()
    else:
        print("# scanned: " + ", ".join(labels))
        current = None
        for (kind, n), locs in sorted(shipped.items()):
            if kind != current:
                current = kind
                print(f"\n[{kind}]")
            locs = sorted(locs)
            more = f" (+{len(locs) - args.max_locations} more)" if len(locs) > args.max_locations else ""
            print(f"  {n:28} {len(locs):4} files  e.g. {', '.join(locs[: args.max_locations])}{more}")
        if unresolved:
            print("\n[UNRESOLVED dynamic types: add the renderer to RENDERED_DYNAMIC]")
            for kind, loc in unresolved:
                print(f"  {kind:12} {loc}")
        print("\n[kept: built on purpose even where no product config uses them yet]")
        for (kind, n), why in sorted(KEPT.items()):
            print(f"  {kind}/{n}  {len(shipped.get((kind, n), ()))} files: {why}")
        if retired:
            print("\n[RETIRED but still rendered: ERROR, the image does not build these]")
            for (kind, n), locs in sorted(retired.items()):
                print(f"  {kind}/{n}: {RETIRED[(kind, n)]['why']}")
                for loc in sorted(locs):
                    print(f"      {loc}")
        if retired_fixture:
            print("\n[retired, tolerated in frozen golden fixtures (RETIRED_FIXTURES)]")
            for (kind, n), locs in sorted(retired_fixture.items()):
                print(f"  {kind}/{n}  {len(locs)} files")
        if excluded:
            print("\n[excluded: not run by this image]")
            for (why, kind, n), locs in sorted(excluded.items()):
                print(f"  {why}: {kind}/{n}  {len(locs)} files")

    if args.check:
        listed = read_components(args.check)
        component_kinds = {"source", "transform", "sink", "enrichment_table"}
        missing = sorted({
            f"{kind}/{n}" for (kind, n) in list(shipped) + list(KEPT)
            if kind in component_kinds and (kind, n) not in listed
        })
        for (kind, n), locs in sorted(retired_fixture.items()):
            print(f"note: retired {kind}/{n} tolerated in {len(locs)} frozen fixture file(s)", file=sys.stderr)
        failed = False
        for m in missing:
            print(f"MISSING from {args.check}: {m}", file=sys.stderr)
            failed = True
        for (kind, n), locs in sorted(retired.items()):
            for loc in sorted(locs):
                print(f"RETIRED {kind}/{n} is rendered at {loc}: {RETIRED[(kind, n)]['why']}", file=sys.stderr)
            failed = True
        for kind, loc in unresolved:
            print(f"UNRESOLVED dynamic {kind} at {loc}", file=sys.stderr)
            failed = True
        if failed:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
