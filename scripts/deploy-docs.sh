#!/usr/bin/env bash
#
# Copyright (c) 2026 Talon Contributors
# Author: dark.lijin@gmail.com
# Licensed under the Talon Community Dual License Agreement.
# See the LICENSE file in the project root for full license information.
#
# Deploy Talon docs to darkmice/talon-docs GitHub Pages repo
# Usage: ./scripts/deploy-docs.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCS_DIR="$PROJECT_ROOT/packages/docs"
DIST_DIR="$DOCS_DIR/.vitepress/dist"
DEPLOY_REPO="https://github.com/darkmice/talon-docs.git"
DEPLOY_BRANCH="gh-pages"

echo "==> Building docs..."
cd "$DOCS_DIR"
npm install --silent
npx vitepress build

echo "==> Deploying to $DEPLOY_REPO ($DEPLOY_BRANCH)..."
cd "$DIST_DIR"
git init
git checkout -b "$DEPLOY_BRANCH"
git add -A
git commit -m "docs: deploy $(date '+%Y-%m-%d %H:%M:%S')"
git push -f "$DEPLOY_REPO" "$DEPLOY_BRANCH"

echo "==> Done! Site: https://darkmice.github.io/talon-docs/"
