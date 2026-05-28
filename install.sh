#!/usr/bin/env bash
# wytchr installer.
#
# Provisions Aiven for PostgreSQL via Terraform (or OpenTofu — the script
# auto-detects), then boots wytchr with the resulting connection string
# injected straight into the container env. The DATABASE_URL is NEVER
# written to .env or any file on disk by this script.
#
# Requires:
#   fnox installed; cwd must contain a fnox.toml defining AIVEN_API_TOKEN.
#   Decryption uses the age identity at ~/.config/fnox/age.txt.
#   tofu >= 1.6 OR terraform >= 1.5 (prefers tofu; override via $TF_BIN)
#   docker, docker compose
#
# NOTE: The IaC tool writes infra/terraform.tfstate locally (same filename
# regardless of binary). That state file contains the DB password in
# plaintext. It is gitignored, but treat it like a secret on this host.

set -euo pipefail

# --provision-only: run terraform, print DATABASE_URL to stdout, skip
# `docker compose up`. Used by ~/homelab/compose/wytchr/install.sh so the
# homelab can orchestrate its own deploy after we hand it the DB URL.
#
# --infra-dir <path> / $INFRA_DIR: path to the Terraform working directory
# (where main.tf lives and where .terraform/ + terraform.tfstate will be
# written). Required. Operational state must NOT live inside this repo —
# see infra.example/README.md.
PROVISION_ONLY=0
INFRA_DIR="${INFRA_DIR:-}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --provision-only) PROVISION_ONLY=1; shift ;;
    --infra-dir) INFRA_DIR="$2"; shift 2 ;;
    --infra-dir=*) INFRA_DIR="${1#*=}"; shift ;;
    *) echo "error: unknown arg: $1" >&2; exit 2 ;;
  esac
done

# In provision-only mode, stdout is reserved for the DATABASE_URL payload —
# redirect all human-readable logging to stderr so callers can capture it
# cleanly via `DATABASE_URL=$(install.sh --provision-only)`.
if [[ $PROVISION_ONLY -eq 1 ]]; then
  exec 3>&2
else
  exec 3>&1
fi
log() { echo "$@" >&3; }

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "$INFRA_DIR" ]]; then
  echo "error: --infra-dir <path> (or INFRA_DIR env var) is required." >&2
  echo "       See infra.example/README.md — operational state must not live in this repo." >&2
  exit 2
fi
if [[ ! -f "$INFRA_DIR/main.tf" ]]; then
  echo "error: $INFRA_DIR does not contain main.tf" >&2
  exit 2
fi
# Reject the repo's sample dir explicitly — its only job is to be copied.
case "$INFRA_DIR" in
  "$REPO_DIR/infra.example"|"$REPO_DIR/infra.example/")
    echo "error: refusing to run against repo sample dir $INFRA_DIR." >&2
    echo "       Copy it somewhere you own and point --infra-dir there." >&2
    exit 2 ;;
esac

# Pick the IaC binary. Honor $TF_BIN if set, otherwise prefer `tofu`
# (OpenTofu — Apache-2.0, drop-in compatible) and fall back to
# `terraform`. Both consume the same main.tf, both write
# terraform.tfstate.
TF="${TF_BIN:-}"
if [[ -z "$TF" ]]; then
  if command -v tofu >/dev/null 2>&1; then
    TF=tofu
  elif command -v terraform >/dev/null 2>&1; then
    TF=terraform
  else
    echo "error: neither 'tofu' nor 'terraform' found in PATH (set \$TF_BIN to override)." >&2
    exit 1
  fi
elif ! command -v "$TF" >/dev/null 2>&1; then
  echo "error: \$TF_BIN='$TF' not found in PATH." >&2
  exit 1
fi

# fnox is installed via mise; its shims are only on PATH in shells that
# ran `mise activate`. The interactive shell does, but a plain `bash -lc`
# (how the homelab wrapper invokes us) does not — so pull mise's shims
# onto PATH ourselves before looking fnox up.
if ! command -v fnox >/dev/null 2>&1; then
  mise_bin="$(command -v mise || true)"
  : "${mise_bin:=$HOME/.local/bin/mise}"
  [[ -x "$mise_bin" ]] && eval "$("$mise_bin" activate bash --shims)"
fi

for bin in docker fnox; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "error: '$bin' not found in PATH." >&2
    exit 1
  fi
done

# Fetch the token straight into a shell var. fnox resolves against the
# fnox.toml in cwd — the homelab wrapper invokes us with cwd set to
# compose/wytchr/. Never written to disk.
AIVEN_API_TOKEN="$(fnox get AIVEN_API_TOKEN)"
if [[ -z "$AIVEN_API_TOKEN" ]]; then
  echo "error: fnox get returned empty for AIVEN_API_TOKEN (check fnox.toml in cwd)" >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "error: 'docker compose' plugin not available." >&2
  exit 1
fi

# TF_VAR_* is how we hand the token to terraform without it touching disk
# or shell history beyond this process tree.
export TF_VAR_aiven_api_token="$AIVEN_API_TOKEN"

log "==> $TF init"
"$TF" -chdir="$INFRA_DIR" init -input=false >&3

log "==> $TF apply (provisioning Aiven for PostgreSQL in jay-miller/do-nyc)"
"$TF" -chdir="$INFRA_DIR" apply -auto-approve -input=false >&3

# -raw avoids JSON quoting; capture into a shell var so it stays in memory
# only. Do NOT echo it, do NOT write it to a file.
DATABASE_URL="$("$TF" -chdir="$INFRA_DIR" output -raw database_url)"
export DATABASE_URL

if [[ -z "$DATABASE_URL" ]]; then
  echo "error: $TF did not produce a database_url output." >&2
  exit 1
fi

if [[ $PROVISION_ONLY -eq 1 ]]; then
  # Hand the URL to the caller on stdout. Nothing else touches stdout.
  printf '%s\n' "$DATABASE_URL"
  unset DATABASE_URL TF_VAR_aiven_api_token AIVEN_API_TOKEN
  exit 0
fi

log "==> docker compose pull (fetch latest image if digest differs)"
docker compose -f "$REPO_DIR/compose.yml" pull >&3

log "==> docker compose up -d (DATABASE_URL injected via process env only)"
# Compose interpolates ${DATABASE_URL} from our exported env into the
# container's environment. Nothing is persisted to .env.
docker compose -f "$REPO_DIR/compose.yml" up -d >&3

# Scrub from this shell's environment before exit. (Subshell var dies with
# the script anyway; this is belt-and-suspenders for anyone sourcing it.)
unset DATABASE_URL TF_VAR_aiven_api_token AIVEN_API_TOKEN

log
log "wytchr is up. The Postgres connection string lives only in:"
log "  - infra/terraform.tfstate (gitignored, on this host)"
log "  - the running wytchr container's env"
log "Re-run this script to refresh the container with the latest URL."
