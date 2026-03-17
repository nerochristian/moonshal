#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
BRANCH="${BRANCH:-main}"
BOT_SERVICE_NAME="${BOT_SERVICE_NAME:-moonshal-bot}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-$APP_DIR/.venv}"
VENV_PYTHON="${VENV_PYTHON:-$VENV_DIR/bin/python}"

cd "$APP_DIR"

if ! command -v git >/dev/null 2>&1; then
  echo "git is not installed"
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "$PYTHON_BIN is not installed"
  exit 1
fi

if [[ ! -x "$VENV_PYTHON" ]]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

if [[ -f requirements.txt ]]; then
  "$VENV_PYTHON" -m pip install -r requirements.txt
fi

sudo systemctl restart "$BOT_SERVICE_NAME"
sudo systemctl status "$BOT_SERVICE_NAME" --no-pager
