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

GitHub Actions are disabled on the fork; no upstream workflow runs here.

## The patch series

Every patch is a self-contained commit (squash-merge each PR), in this order:

1. `feat(azure_event_hubs source): …` (NAN-3599): `src/sources/azure_event_hubs/`,
   `src/internal_events/azure_event_hubs.rs`, docs (`website/…/azure_event_hubs*`),
   `changelog.d/4261_azure_event_hubs_source.feature.md`, emulator test config in
   `tests/integration/azure/config/`, and small hunks in `Cargo.toml` (dependencies and
   features), `src/sources/mod.rs` and `src/internal_events/mod.rs`.
2. `chore(nano): fork tooling`: this file and `distribution/docker/nano/`.

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
