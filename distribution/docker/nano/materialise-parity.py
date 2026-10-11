#!/usr/bin/env python3
"""Regenerate distribution/docker/nano/parity/ from nano's real Vector configs (NAN-3604).

Each parity unit is one config that `vector validate --no-environment` must accept with this
image (the nano-image workflow runs them all). A unit is either a top-level *.toml / *.yaml file
or a directory whose *.toml files form one config. Units, one per renderer family:

  platform-base/      nanosiem tenant_bundle testdata platform-base-collector-v2.tar.gz
                      (the base every cell starts from: http_server, splunk_hec, vector,
                      opentelemetry sources; lanes; Bronze parquet sink; metrics; clickhouse)
  tenant-bundle/      nano-main nan2596-collector-v2/bundle.tar.gz (a built tenant bundle:
                      base + tenant lanes, including sampling)
  pull-bundle/        nano-main nan3381-pull-bundle/pull-base.tar.gz (pull_base: the filtered
                      platform base + rendered pull sources + loss probe), with the retired kafka
                      kind replaced by azure_event_hubs as DESIGN NAN-3604 section 9 renders it.
                      (The fixture's full pull-bundle.tar.gz is not used: its synthetic tenant
                      lanes predate VRL 0.36 and fail E651 on upstream 0.59 too; tenant lanes are
                      covered by tenant-bundle/.)
  pull-*.toml         one standalone config per pull kind (hand-written, pull.rs shape)
  open-lean-audit.toml  nanosiem deploy/vector/open-lean-audit.toml
  region-log-shipper.yaml  nano-main hosted-shared-region-log-shipping.ts vectorConfig()
  harness.toml        stdin / demo_logs harness shapes (pipe_native_tests.rs,
                      verify-vector-generation-rollback.sh)

Sanitising: `${VAR:-default}` becomes the default; `${VAR}` becomes a placeholder literal (URLs
point at .invalid hosts, secrets become "parity-not-a-secret"); no hostnames that matter.

Usage: materialise-parity.py NANOSIEM_REPO@REF NANO_MAIN_REPO@REF   (writes into parity/)
Python 3 standard library only. Hand-written units (pull-*.toml, harness.toml) are left alone.
"""

from __future__ import annotations

import io
import os
import re
import shutil
import subprocess
import sys
import tarfile

HERE = os.path.dirname(os.path.abspath(__file__))
PARITY = os.path.join(HERE, "parity")

ENV = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-([^}]*))?\}")


def placeholder(name: str) -> str:
    if name.endswith("QUEUE_URL"):
        return "https://sqs.us-east-1.amazonaws.com/000000000000/parity-queue"
    if name.endswith(("_URL", "_ENDPOINT", "_URI")):
        return "http://parity.invalid"
    if name.endswith("_REGION") or name == "AWS_REGION":
        return "us-east-1"
    if any(s in name for s in ("PASSWORD", "SECRET", "TOKEN", "ACCESS_KEY")):
        return "parity-not-a-secret"
    if name.endswith("_EH_NAMESPACE"):
        return "parity-ns.servicebus.windows.net"
    if name.endswith("_AZURE_CLIENT_ID"):
        return "00000000-0000-4000-8000-000000000003"
    return "parity-" + name.lower().replace("_", "-")


# Defaults that point at a real service: replaced even though the source has a default.
OVERRIDE = {
    "NANO_LAKE_OBJECT_ENDPOINT": "https://objects.parity.invalid",
    "NANO_LAKE_FORCE_PATH_STYLE": "true",
}


def sanitise(text: str) -> str:
    def sub(m):
        name, default = m.group(1), m.group(2)
        if name in OVERRIDE:
            return OVERRIDE[name]
        return default if default is not None else placeholder(name)

    out = ENV.sub(sub, text)
    assert "${" not in out, "unresolved env reference"
    return out


def git_show(spec: str, path: str) -> bytes:
    repo, _, ref = spec.rpartition("@")
    return subprocess.run(
        ["git", "-C", os.path.expanduser(repo), "show", f"{ref}:{path}"],
        check=True, stdout=subprocess.PIPE,
    ).stdout


def flatten_bundle(blob: bytes, dest: str, transform=None) -> None:
    """Write every loaded *.toml of a bundle (top level, sources/configs, sources/parsers)."""
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest)
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        for member in sorted(tar.getmembers(), key=lambda m: m.name):
            name = member.name.lstrip("./")
            if not member.isfile() or not name.endswith(".toml"):
                continue
            if "/" in name and not name.startswith(("sources/configs/", "sources/parsers/")):
                continue  # backup/ and other dirs are not loaded by --config-dir
            flat = name.replace("sources/configs/", "configs-").replace("sources/parsers/", "parsers-")
            text = tar.extractfile(member).read().decode()
            if transform:
                text = transform(flat, text)
            with open(os.path.join(dest, flat), "w") as f:
                f.write(sanitise(text))


AZURE_FOR_KAFKA = '''[sources.{id}]
type = "azure_event_hubs"
namespace = "${{NANO_PULL_{hex}_EH_NAMESPACE}}"
event_hub_name = "${{NANO_PULL_{hex}_EH_NAME}}"
consumer_group = "${{NANO_PULL_{hex}_EH_CONSUMER_GROUP}}"
acknowledgements.enabled = true

[sources.{id}.auth]
azure_credential_kind = "managed_identity"
user_assigned_managed_identity_id = "${{NANO_PULL_{hex}_AZURE_CLIENT_ID}}"

[sources.{id}.checkpoint_store]
type = "azure_blob"
account_name = "${{NANO_PULL_{hex}_CHECKPOINT_ACCOUNT}}"
container_name = "${{NANO_PULL_{hex}_CHECKPOINT_CONTAINER}}"

[sources.{id}.checkpoint_store.auth]
azure_credential_kind = "managed_identity"
user_assigned_managed_identity_id = "${{NANO_PULL_{hex}_AZURE_CLIENT_ID}}"

'''


def kafka_to_azure(flat: str, text: str) -> str:
    if flat != "05-pull-sources.toml":
        return text
    pattern = re.compile(
        r'\[sources\.(nano_pull_([0-9a-f]+))\]\ntype = "kafka"\n.*?(?=\n\[transforms\.)', re.S
    )

    def sub(m):
        return AZURE_FOR_KAFKA.format(id=m.group(1), hex=m.group(2).upper()).rstrip("\n")

    text = pattern.sub(sub, text)
    text = text.replace('.content_format = "nano_pull_kafka"', '.content_format = "nano_pull_azure_event_hubs"')
    text = text.replace("(kafka) -> lane", "(azure_event_hubs) -> lane")
    assert '"kafka"' not in text
    return text


REGION_EXPR = {
    "q(selector)": '"kubernetes.io/metadata.name notin (kube-system,nano-log-shipping)"',
    "q(REGION_LOG_SOURCE_TYPE)": '"nano-lake-workload"',
    "q(o.region)": '"parity-region"',
    "q(o.clusterId)": '"parity-cluster"',
    "q(o.clusterName)": '"parity-cluster"',
    "o.maxEventsPerSecondPerNode": "100",
    "q(o.sinkUrl)": '"https://org.parity.invalid/ingest"',
}


def region_log_shipper(ts: str) -> str:
    body = re.search(r"function vectorConfig\(.*?return \[\n(.*?)\n\s*\]\.join", ts, re.S).group(1)
    lines = []
    for raw in body.splitlines():
        raw = raw.strip()
        if raw.startswith("//"):
            continue
        m = re.fullmatch(r'(["`])(.*)\1,?', raw)
        if not m:
            raise SystemExit(f"region log shipper: unexpected line {raw!r}")
        line = m.group(2).replace('\\"', '"')
        if m.group(1) == "`":
            line = re.sub(r"\$\{([^}]*)\}", lambda e: REGION_EXPR[e.group(1)], line)
        lines.append(line)
    return sanitise("\n".join(lines) + "\n")


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    nanosiem, nano_main = sys.argv[1], sys.argv[2]
    flatten_bundle(
        git_show(nanosiem, "nanosiem-core/src/parsers/tenant_bundle/testdata/platform-base-collector-v2.tar.gz"),
        os.path.join(PARITY, "platform-base"),
    )
    fixtures = "apps/api/src/services/shared-infrastructure/test-fixtures"
    flatten_bundle(
        git_show(nano_main, f"{fixtures}/nan2596-collector-v2/bundle.tar.gz"),
        os.path.join(PARITY, "tenant-bundle"),
    )
    flatten_bundle(
        git_show(nano_main, f"{fixtures}/nan3381-pull-bundle/pull-base.tar.gz"),
        os.path.join(PARITY, "pull-bundle"),
        transform=kafka_to_azure,
    )
    with open(os.path.join(PARITY, "open-lean-audit.toml"), "w") as f:
        f.write(sanitise(git_show(nanosiem, "deploy/vector/open-lean-audit.toml").decode()))
    ts = git_show(nano_main, "apps/api/src/services/shared-infrastructure/hosted-shared-region-log-shipping.ts").decode()
    with open(os.path.join(PARITY, "region-log-shipper.yaml"), "w") as f:
        f.write(region_log_shipper(ts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
