#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
DEPS_STAMP="$VENV_DIR/.deps-installed-stamp"
CONFIG_DIR="${CONFIG_DIR:-$ROOT_DIR/config}"
ENV_FILE="${ENV_FILE:-$CONFIG_DIR/.env}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "python3 not found. Please install Python 3.11+." >&2
  exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

if [ ! -f "$DEPS_STAMP" ] || [ "$ROOT_DIR/pyproject.toml" -nt "$DEPS_STAMP" ]; then
  if ! python -m pip install --disable-pip-version-check -e . >/dev/null; then
    echo "Dependency installation failed." >&2
    echo "Ensure internet access to PyPI (or configure proxy/PIP_INDEX_URL), then retry ./run-local.sh." >&2
    exit 1
  fi
  touch "$DEPS_STAMP"
fi

# Avoid stale inherited values from parent processes (MCP client shells, IDEs, etc.).
# If needed, set GOOGLE_SERVICE_ACCOUNT_FILE explicitly in config/.env.
unset GOOGLE_SERVICE_ACCOUNT_FILE
unset GOOGLE_APPLICATION_CREDENTIALS

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
fi

if [[ "${DEFAULT_GSC_SITE_URL:-}" == REPLACE_WITH_* ]]; then
  echo "Warning: DEFAULT_GSC_SITE_URL in $ENV_FILE still has a placeholder value." >&2
fi
if [[ "${DEFAULT_GA4_PROPERTY_ID:-}" == REPLACE_WITH_* ]]; then
  echo "Warning: DEFAULT_GA4_PROPERTY_ID in $ENV_FILE still has a placeholder value." >&2
fi

# Force Domain property format for GSC to avoid URL-prefix permission mismatches.
if [ -n "${DEFAULT_GSC_SITE_URL:-}" ]; then
  _site="${DEFAULT_GSC_SITE_URL#sc-domain:}"
  _site="${_site#http://}"
  _site="${_site#https://}"
  _site="${_site%%/*}"
  _site="${_site%%:*}"
  _site="${_site#.}"
  _site="${_site%.}"
  if [[ "$_site" == www.* ]]; then
    _site="${_site#www.}"
  fi
  if [ -n "$_site" ]; then
    DEFAULT_GSC_SITE_URL="sc-domain:${_site}"
    export DEFAULT_GSC_SITE_URL
  fi
fi

# If a default site URL is set, allow tools to use it without requiring site_url in every call.
if [ -n "${DEFAULT_GSC_SITE_URL:-}" ] && [ -z "${REQUIRE_EXPLICIT_GSC_SITE_URL:-}" ]; then
  export REQUIRE_EXPLICIT_GSC_SITE_URL=false
fi

DEFAULT_SERVICE_ACCOUNT_FILE="$CONFIG_DIR/service-account.json"

if [ -z "${GOOGLE_SERVICE_ACCOUNT_FILE:-}" ]; then
  GOOGLE_SERVICE_ACCOUNT_FILE="$DEFAULT_SERVICE_ACCOUNT_FILE"
fi

if [[ "$GOOGLE_SERVICE_ACCOUNT_FILE" != /* ]]; then
  GOOGLE_SERVICE_ACCOUNT_FILE="$ROOT_DIR/${GOOGLE_SERVICE_ACCOUNT_FILE#./}"
fi

if [ ! -f "$GOOGLE_SERVICE_ACCOUNT_FILE" ] \
  && [ "$GOOGLE_SERVICE_ACCOUNT_FILE" != "$DEFAULT_SERVICE_ACCOUNT_FILE" ] \
  && [ -f "$DEFAULT_SERVICE_ACCOUNT_FILE" ]; then
  echo "Configured GOOGLE_SERVICE_ACCOUNT_FILE not found: $GOOGLE_SERVICE_ACCOUNT_FILE" >&2
  echo "Falling back to default: $DEFAULT_SERVICE_ACCOUNT_FILE" >&2
  GOOGLE_SERVICE_ACCOUNT_FILE="$DEFAULT_SERVICE_ACCOUNT_FILE"
fi
export GOOGLE_SERVICE_ACCOUNT_FILE

if [ ! -f "$GOOGLE_SERVICE_ACCOUNT_FILE" ]; then
  echo "Missing service account file: $GOOGLE_SERVICE_ACCOUNT_FILE" >&2
  echo "Create it with: cp config/service-account.example.json config/service-account.json" >&2
  echo "Then fill all REPLACE_WITH_* values, or set GOOGLE_SERVICE_ACCOUNT_FILE in $ENV_FILE." >&2
  exit 1
fi

if grep -q "REPLACE_WITH_" "$GOOGLE_SERVICE_ACCOUNT_FILE"; then
  echo "service-account.json still contains placeholder values." >&2
  echo "Open $GOOGLE_SERVICE_ACCOUNT_FILE and replace all REPLACE_WITH_* fields." >&2
  exit 1
fi

echo "Using service account file: $GOOGLE_SERVICE_ACCOUNT_FILE" >&2

exec seo-analytics-mcp "$@"
