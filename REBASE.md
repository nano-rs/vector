# nano-rs/vector: fork maintenance

This repository is nano's fork of [vectordotdev/vector](https://github.com/vectordotdev/vector).
It carries a small series of patches on top of an upstream release tag, for components upstream
does not ship (yet). Nothing here is pushed or proposed upstream unless we decide to.

## Branches

| Branch | Meaning |
| --- | --- |
| `nano/vX.Y` | Upstream tag `vX.Y.0` plus our patch series. The newest one is the default branch. |
| `feat/NAN-…`, `fix/NAN-…` | Work on a patch, PR'd into the current `nano/vX.Y`. |

Branches are never force-pushed. Moving to a new upstream release creates a **new**
`nano/vX.Y` branch; the old one stays as the rollback point for images built from it.

No upstream GitHub Actions workflow exists on a `nano/vX.Y` branch (see "Upstream CI is
removed" below). Only the fork's own workflows (`.github/workflows/nano-*.yml`) run here.

## The patch series

Every patch is a self-contained commit (squash-merge each PR), in this order:

1. `feat(azure_event_hubs source): …` (NAN-3599): `src/sources/azure_event_hubs/`,
   `src/internal_events/azure_event_hubs.rs`, docs (`website/…/azure_event_hubs*`),
   `changelog.d/4261_azure_event_hubs_source.feature.md`, emulator test config in
   `tests/integration/azure/config/`, and small hunks in `Cargo.toml` (dependencies and
   features), `src/sources/mod.rs` and `src/internal_events/mod.rs`.
2. `chore(nano): fork tooling`: this file and `distribution/docker/nano/`; NAN-3604 added
   `.github/workflows/nano-image.yml` and `distribution/docker/nano/parity/`.
3. `chore(nano): neutralise upstream workflows` (NAN-3604): deletes every upstream file in
   `.github/workflows/` and `.github/dependabot.yml`. See "Upstream CI is removed".

Rules that keep rebases cheap:

- Prefer new files over edits to upstream files; keep hunks in shared files (`Cargo.toml`,
  `mod.rs` lists) to a few lines.
- Never mix `Cargo.lock` churn from unrelated `cargo update` runs into a patch. Only the
  entries a patch adds belong to it.
- Keep `tokio-util` at the version upstream's lockfile pins. `fe2o3-amqp` 0.14 (the AMQP stack
  under `azure_messaging_eventhubs` 0.15) does not compile with `tokio-util` 0.7.20.

## Moving to a new upstream release

```sh
git fetch upstream --tags
OLD=v0.59.0 NEW=v0.60.0                       # example
git switch -c nano/v0.60 "$NEW"
# Replay our commits (oldest first) from the previous nano branch:
git cherry-pick $(git rev-list --reverse "$OLD"..origin/nano/v0.59)
```

Re-delete upstream CI on the new branch. Replaying the deletion commit stops with
modify/delete conflicts for every workflow upstream changed since the previous tag; resolve
them all as deleted (`git rm` the listed paths, `git cherry-pick --continue`). That commit
also only removes files that existed at the previous tag; the new tag may add workflows. Remove everything that is
not ours, and fold it into the deletion commit's replay (or a follow-up `chore(nano)` commit):

```sh
git ls-files .github/workflows | grep -v '^.github/workflows/nano-' | xargs -r git rm -q
git rm -q --ignore-unmatch .github/dependabot.yml
git commit -m "chore(nano): neutralise upstream workflows of $NEW"   # if anything was left
git ls-files .github/workflows   # must list only nano-*.yml
```

Resolving conflicts:

- `Cargo.lock`: take upstream's (`git checkout "$NEW" -- Cargo.lock`), then let cargo add our
  dependencies back without touching anything else:
  `cargo-slot update -p azure_messaging_eventhubs -p azure_messaging_eventhubs_checkpointstore_blob`
  (a plain `cargo metadata` or `cargo check` also works). Check that only additions appear in
  `git diff "$NEW" -- Cargo.lock`, and that `tokio-util` did not move.
- `Cargo.toml`, `src/sources/mod.rs`, `src/internal_events/mod.rs`: re-apply the few lines.
- Compile errors from upstream API changes go in a follow-up commit on the new branch,
  `fix(azure_event_hubs source): adapt to vX.Y`, not into the original patch.

Verify, then publish the branch and make it the default:

```sh
F=sources-azure_event_hubs,sinks-console,sinks-file
cargo-slot check  --no-default-features --features $F --lib
cargo-slot test   --no-default-features --features $F --lib sources::azure_event_hubs
# Emulator integration tests (Event Hubs emulator + Azurite):
docker compose -f tests/integration/azure/config/compose.yaml up -d   # or vdev int test azure
EVENTHUBS_EMULATOR_ADDRESS=localhost AZURITE_ADDRESS=localhost \
  cargo-slot test --no-default-features --features azure-event-hubs-integration-tests \
  --lib sources::azure_event_hubs::integration_tests
git push -u origin nano/v0.60
gh repo edit nano-rs/vector --default-branch nano/v0.60
```

`cargo-slot` is the workstation's build gate; use plain `cargo` elsewhere. The upstream lint
`#![deny(warnings)]` flags an unused import in `src/trace.rs` tests when the crate is built with
`--no-default-features`; build the test binaries with `RUSTFLAGS="-A unused_imports"` for
minimal-feature test runs (this replaces the `-D warnings` from `.cargo/config.toml`, so also run
the `check --lib` above without it).

## Upstream CI is removed

Upstream's workflows are written for vectordotdev's infrastructure: they publish to its
registries and package repositories, prune GHCR images, run nightly/regression/scorecard jobs on
a schedule, label and comment on pull requests, and expect paid larger runners. With Actions
enabled on this fork, any of them could run against our packages or spend our minutes. So on
`nano/vX.Y` every upstream workflow file is deleted (NAN-3604), not disabled in place. They
come back only through a rebase onto a new upstream tag, which is why the procedure above
re-deletes them.

Deleted from `nano/v0.59` (all 53 files at `v0.59.0`, plus `.github/dependabot.yml`):

| Trigger (as shipped by upstream) | Workflows |
| --- | --- |
| `schedule` | `cleanup-ghcr-images`, `component_features`, `coverage`, `deny`, `k8s_e2e`, `nightly`, `regression`, `scorecard` |
| `push` (branches/tags) | `check_generated_vrl_docs`, `release` (tag `v*`), `release_autotag`, `scorecard`, `static-analysis`, `vdev_autotag`, `vdev_publish`, `warm-cache` |
| `pull_request`, `merge_group` | `changelog`, `check_generated_vrl_docs`, `deny`, `deprecation`, `integration`, `integration_windows`, `k8s_e2e`, `master_merge_queue`, `preview_site_trigger`, `protobuf`, `release_pr_check`, `spelling`, `test` |
| `pull_request_target`, `pull_request_review`, `issue_comment` | `add_docs_review_label`, `check_docs_review`, `ci-integration-review`, `ci-review-trigger`, `cla`, `labeler`, `remove_waiting_author` |
| `workflow_run` | `build_preview_sites`, `check_generated_vrl_docs_comment`, `release_unfreeze`, `remove_docs_review_label` |
| `workflow_dispatch` / `workflow_call` only | `build-test-runner`, `changes`, `create_preview_sites`, `cross`, `custom_builds`, `install-sh`, `integration-test`, `msrv`, `publish`, `release_housekeeping`, `release_manifests`, `release_prepare`, `release_website`, `test-make-command`, `unit-tests`, `unit_mac`, `unit_windows` |

Left in place because nothing triggers them on their own: `.github/actions/` (composite
actions), `.github/labeler.yml`, `.github/actionlint.yaml`, issue/PR templates, `CODEOWNERS`.

The fork also mirrors upstream's other branches (`master`, `dependabot/*`, contributor
branches). Those still carry upstream's workflows, and a push to one of them (for example a
"Sync fork" of `master`) would run its `push`/`pull_request` workflows. Do not push to
mirrored branches; see the repository's Actions policy for how that is contained.

## Cutting an image

Images are built by GitHub Actions (`.github/workflows/nano-image.yml`) and pushed to
`ghcr.io/nano-rs/vector`. Nothing builds on branch pushes or pull requests; an image is cut
only by pushing an image tag or by a manual dispatch.

`distribution/docker/nano/Dockerfile` builds a drop-in replacement for
`timberio/vector:<version>-debian`: same Debian base and runtime packages, `/usr/bin/vector`
as the entrypoint, `/var/lib/vector` present, and `/etc/vector` holding exactly upstream's
`examples/` and nothing else. It compiles only nano's component set (`FEATURES`, see
"Component set" below) with thin LTO and 16 codegen units (`LTO=fat CODEGEN_UNITS=1`
reproduces upstream's profile). It fails if `vector list` lacks `azure_event_hubs`.

Version scheme: `<upstream version>-nano.<n>`, for example `0.59.0-nano.1`. `<n>` starts at 1
and increments for every image cut from the same upstream version; the workflow refuses a
version that does not start with the `Cargo.toml` version or that already exists in GHCR.
The git tag is `image-<version>` (`image-0.59.0-nano.1`). The `image-` prefix keeps it clear
of upstream's release tag filters (`v0.*`), which still exist on mirrored branches.

1. Pick `n`: one more than the newest `0.59.0-nano.*` tag at
   https://github.com/nano-rs/vector/pkgs/container/vector.
2. Start the build, either way:

   ```sh
   git tag image-0.59.0-nano.2 origin/nano/v0.59 && git push origin image-0.59.0-nano.2
   # or
   gh workflow run nano-image.yml --repo nano-rs/vector --ref nano/v0.59 -f nano_rev=2
   ```

3. Wait (about 2-4 h; amd64 and arm64 build in parallel on native runners). The jobs:
   build per arch (pushed untagged, by digest) -> multi-arch index tagged `sha-<12-char sha>`
   -> parity on amd64 -> promote, which adds `<version>` and `<version>-debian` to the same
   index. Parity checks that `vector --version` is the plain upstream version (see
   "Invariants"), that `/etc/vector` is identical to upstream's, that `vector list` equals
   `distribution/docker/nano/COMPONENTS` exactly, and that every config under
   `distribution/docker/nano/parity/` passes `vector validate --no-environment` (each
   top-level `*.toml` / `*.yaml` alone, each subdirectory as one config; this parses, wires
   and compiles VRL but contacts no source or sink).
4. Read the digests from the promote job's summary. Never consume a tag; tags are only for
   humans.
   - GHCR / Kubernetes: the multi-arch index, `ghcr.io/nano-rs/vector@sha256:…`.
   - Cloudflare Containers: the **linux/amd64 single-platform manifest** digest, printed
     separately. Cloudflare needs a linux/amd64 image and its acceptance of an OCI index is
     undocumented, so nano-main copies and pins the amd64 manifest, not the index.

Cloudflare Containers cannot pull from GHCR. nano-main's roll step copies the pinned amd64
digest into `registry.cloudflare.com` (`crane copy ghcr.io/nano-rs/vector@sha256:<amd64> …`);
that step and its credentials belong to nano-main, not to this repository.

## Invariants

- The crate version stays the upstream version (`0.59.0`); the nano suffix exists only in
  image tags. `vector --version` must keep matching nanosiem's `VECTOR_VERSION_RE`
  (`^vector \d+\.\d+\.\d+(\s|$)`), which refuses `0.59.0-nano.1`. Never bump `version` in
  `Cargo.toml`; the image identity goes into `VECTOR_BUILD_DESC` (after the target triple).
- `/etc/vector` in the image equals upstream's (only `examples/`). nano's entrypoints untar
  the config bundle there and run `--config-dir /etc/vector`, so a default config in the image
  would run alongside every tenant pipeline.
- `vector list` equals `COMPONENTS` exactly. The image is NOT a superset of upstream (Dan,
  2026-10-11): it builds only what nano ships or renders.

## Component set

Files in `distribution/docker/nano/`:

| File | What |
| --- | --- |
| `scan-components.py` | Inventories every component type, codec, framing, compression and buffer in nano's Vector configs: nanosiem (`config/vector`, base bundles, renderer code and tests, compose, deploy), nano-main (pull bundle, region log shipper, fixtures), nano-rs/parsers lanes, and `parity/`. Reads git trees at given refs; never checks anything out. |
| `FEATURES` | The cargo features (one per line, comments allowed) the Dockerfile builds with `--no-default-features`. |
| `COMPONENTS` | The exact `vector list` of that build. |
| `materialise-parity.py` | Regenerates the bundle-derived parity units from nanosiem and nano-main. |
| `parity/` | One config per renderer family (see `materialise-parity.py`), plus hand-written `pull-*.toml` and `harness.toml`. |

Adding a component (nano starts rendering a new type):

```sh
python3 distribution/docker/nano/scan-components.py \
  nanosiem=~/Projects/nanosiem@origin/main nano-main=~/Projects/nano-main@origin/main \
  parsers=<parsers clone>@origin/main vector=.@HEAD --check distribution/docker/nano/COMPONENTS
```

`--check` fails on any shipped type missing from `COMPONENTS`, and on any renderer that emits
a type from a variable the scanner cannot resolve (`RENDERED_DYNAMIC`). Then:

1. Add the feature to `FEATURES` (Cargo.toml names: `sources-<x>`, `transforms-<x>`,
   `sinks-<x>`; one feature can enable several components, e.g. `sinks-prometheus` gives
   `prometheus_exporter` and `prometheus_remote_write`).
2. Regenerate `COMPONENTS` from a build:
   `cargo-slot build --no-default-features --features "$(sed -e 's/#.*//' -e 's/[[:space:]]//g' -e '/^$/d' distribution/docker/nano/FEATURES | paste -sd, -)" --bin vector`
   (a dev build with `CARGO_PROFILE_DEV_DEBUG=0` is enough), then `target/debug/vector list > distribution/docker/nano/COMPONENTS`.
3. Add or refresh a parity unit that uses the type.
4. Update the vendored `COMPONENTS` copies in nanosiem and nano-main (their guard tests pin
   this repository's commit; see the comment at the top of each copy).

Deliberately left out, though something still mentions them:

- `kafka` source: the never-shipped Azure-over-Kafka pull kind, replaced by `azure_event_hubs`
  (`RETIRED` in the scanner). Building it needs librdkafka (cmake) and vendored SASL/GSSAPI.
- `file`, `syslog` sources and the other `*.toml.disabled` templates in the base bundle: never
  loaded.
- Customer-side Vector (sender templates, Ludus lab agents and aggregator), which run upstream
  images.
- `secrets` (AWS Secrets Manager backend), enrichment tables (geoip, mmdb, memory), `dnstap`
  (and its `parse_dnstap` VRL function): no nano config uses them. The `file` and `exec` secret
  backends and the `file` enrichment table are built in regardless.

Parity configs are sanitised (no secrets, env references replaced by literals). When moving to
a new upstream release, update `UPSTREAM_IMAGE` (tag and index digest) in the workflow; it is
still used for the `/etc/vector` comparison.

One-time setup per repository: Actions enabled with an allow-list that
covers the actions the workflow pins, and the `vector` package's visibility set to public
(the first push creates it private under the org).

Local build, for debugging the Dockerfile only:

```sh
docker build -f distribution/docker/nano/Dockerfile -t nano-vector:dev .
docker run --rm nano-vector:dev --version
```
