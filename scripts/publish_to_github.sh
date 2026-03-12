#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${1:-}"
if [ -z "$REPO_URL" ]; then
  echo "Usage: $0 <github_repo_url>"
  echo "Example: $0 git@github.com:your-org/godmode-media-library.git"
  exit 1
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -d .git ]; then
  git init
fi

git add .
git commit -m "Initial GOD MODE media library app" || true

if ! git remote | grep -q '^origin$'; then
  git remote add origin "$REPO_URL"
else
  git remote set-url origin "$REPO_URL"
fi

git branch -M main
git push -u origin main

echo "Published to $REPO_URL"
