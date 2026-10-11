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
2. `chore(nano): fork tooling`: this file and `distribution/docker/nano/`.
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

`distribution/docker/nano/Dockerfile` builds a drop-in replacement for
`timberio/vector:<version>-debian`: same Debian base and runtime packages, `/usr/bin/vector`
as the entrypoint, `/etc/vector` and `/var/lib/vector` present. It compiles the upstream release
feature set (`target-x86_64-unknown-linux-gnu`), which includes our sources.

```sh
TAG=0.59.0-nano.1-debian                   # <upstream version>-nano.<n>-debian
docker build -f distribution/docker/nano/Dockerfile -t nano-vector:$TAG .
docker run --rm nano-vector:$TAG --version
```

`<n>` increments for every image cut from the same upstream version. The Dockerfile runs
`vector list` during the build and fails if `azure_event_hubs` is missing. Pushing the image to
a registry is a separate, deliberate step.
