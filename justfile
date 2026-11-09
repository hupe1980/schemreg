# schemreg task runner — the single source of truth for what "the checks" are.
#
# `.github/workflows/ci.yml` calls these recipes rather than repeating the
# commands, so a check cannot pass locally and fail in CI because the two
# drifted apart. `just ci-parity` fails when a gate recipe has no CI step.
#
#   just            list every recipe
#   just ci         everything CI runs, except supply chain, MSRV and Docker
#   just ci-full    `ci` plus deny, audit, MSRV and cross-language conformance
#   just pre-commit the fast subset worth running before every commit

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

export RUSTFLAGS := "-D warnings"
export RUSTDOCFLAGS := "-D warnings"

# Minimum supported Rust version, mirroring `rust-version` in Cargo.toml.
msrv := "1.94.1"

# Every feature linted in isolation as well as together: a `cfg`-gated item
# that only compiles under `--all-features` is a real bug for anyone who
# enables a single backend. Mirrors the `clippy` job's matrix.
clippy_sets := "'' confluent apicurio glue avro json protobuf confluent,avro confluent,json apicurio,avro glue,avro confluent,protobuf ALL"

# Each feature on its own, none, and all.
test_sets := "'' confluent apicurio glue avro json protobuf ALL"

default:
    @just --list

# Everything CI runs except supply chain, MSRV and the Docker conformance job.
ci: fmt-check clippy public-api test doc examples bench-smoke site-check ci-parity
    @echo "✓ ci passed"

# `ci` plus every job that needs the network, another toolchain or Docker.
ci-full: ci deny audit msrv conformance
    @echo "✓ ci-full passed"

# The fast subset.
pre-commit: fmt-check
    cargo clippy --all-features --all-targets -- -D warnings

fmt-check:
    cargo fmt --check

fmt:
    cargo fmt

clippy:
    #!/usr/bin/env bash
    set -euo pipefail
    for set in {{clippy_sets}}; do
      if [ "$set" = ALL ]; then flags="--all-features"
      elif [ -z "$set" ]; then flags="--no-default-features"
      else flags="--no-default-features --features $set"; fi
      echo "── clippy $flags"
      cargo clippy $flags --all-targets -- -D warnings
    done

# A `pub` item unreachable from the crate root, or the crate failing to build
# as a featureless dependency. Mirrors the `public-api` job.
public-api:
    cargo doc --all-features --no-deps
    cargo build --no-default-features

test:
    #!/usr/bin/env bash
    set -euo pipefail
    for set in {{test_sets}}; do
      if [ "$set" = ALL ]; then flags="--all-features"
      elif [ -z "$set" ]; then flags="--no-default-features"
      else flags="--no-default-features --features $set"; fi
      echo "── test $flags"
      cargo test $flags --all-targets
      cargo test $flags --doc
    done

# The docs.rs configuration: nightly with `--cfg docsrs`. Mirrors the `docs` job.
doc:
    RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +nightly doc --all-features --no-deps

# Every example in examples/ is executed, not just compiled — they all run
# against in-memory stubs, and a compiling-but-broken example is still broken
# documentation. No list to keep in sync: a new file is picked up.
examples:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --examples --no-default-features
    for file in examples/*.rs; do
      name="$(basename "$file" .rs)"
      echo "── example $name"
      cargo run -q --all-features --example "$name"
    done

# The whole suite with every feature — the cross-platform and beta CI jobs.
test-all-features:
    cargo test --all-features --all-targets
    cargo test --all-features --doc

# Compiles and smoke-runs the benchmarks. Not a performance gate.
bench-smoke:
    cargo bench --all-features --bench wire -- --warm-up-time 0.2 --measurement-time 0.5 --sample-size 10

# Full benchmark run, for the numbers quoted on the performance page.
bench:
    cargo bench --all-features --bench wire

# Builds the site and resolves internal links. CI also fetches external links;
# locally that is `just site-check-external`, since it needs the network.
site-check:
    zola --root site build
    zola --root site check --skip-external-links

site-check-external:
    zola --root site check

site-serve:
    zola --root site serve

deny:
    cargo deny check

audit:
    cargo audit --deny warnings

msrv:
    cargo +{{msrv}} build --all-features --locked
    cargo +{{msrv}} test --all-features --locked

# Every recipe `ci` and `ci-full` depend on must be run by a CI step, so the
# workflow cannot silently stop asking a question the local gate asks.
ci-parity:
    #!/usr/bin/env bash
    set -euo pipefail
    missing=0
    recipes="$(sed -nE 's/^ci(-full)?: *//p' justfile | tr ' ' '\n' | grep -vx -e ci -e ci-parity -e '' | sort -u)"
    [ -n "$recipes" ] || { echo "✗ found no gate recipes — has the ci recipe been renamed?"; exit 1; }
    for recipe in $recipes; do
      if ! grep -qE "just ${recipe}( |\$|\")" .github/workflows/ci.yml; then
        echo "✗ recipe '$recipe' is in the gate but no CI step runs it"; missing=1
      fi
    done
    [ "$missing" = 0 ] && echo "✓ all $(echo "$recipes" | wc -l | tr -d ' ') gate recipes have a CI step"

# Regenerates the wire-format fixtures with the official confluent-kafka
# serializers, fails if they drifted, and decodes them. Needs Docker.
conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'docker compose -f conformance/docker-compose.yml down -v' EXIT
    docker compose -f conformance/docker-compose.yml up --build --abort-on-container-exit --exit-code-from fixtures
    git diff --exit-code -- tests/fixtures/
    cargo test --all-features --test conformance_fixtures
