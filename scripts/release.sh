#!/bin/bash
#
# Copyright (c) 2026 Talon Contributors
# Author: dark.lijin@gmail.com
# Licensed under the Talon Community Dual License Agreement.
# See the LICENSE file in the project root for full license information.
#
# Talon 一键发版脚本
# 用法: ./scripts/release.sh v0.1.4
#
# 同步更新三个仓库的版本 tag：
#   1. talon (superclaw-db) — 打 tag 并推送，触发 GitHub Actions CI
#   2. talon-bin — 更新 Cargo.toml 版本号 + 打 tag
#   3. talon-sdk — 打 tag（libtalon.a 由 CI 自动推送）
#
# 前提：三个仓库在同一父目录下（../talon-bin, ../talon-sdk）

set -euo pipefail

# ── 颜色 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }

# ── 参数检查 ──
SKIP_CONFIRM=false
if [ "${1:-}" = "-y" ]; then
    SKIP_CONFIRM=true
    shift
fi

if [ $# -ne 1 ]; then
    echo "用法: $0 [-y] <version>"
    echo "示例: $0 v0.1.4"
    echo "  -y  跳过确认提示"
    exit 1
fi

VERSION="$1"

# 校验版本格式
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    error "版本格式错误: $VERSION（需要 vX.Y.Z 格式）"
fi

# 去掉 v 前缀用于 Cargo.toml
SEMVER="${VERSION#v}"

# ── 路径 ──
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TALON_DIR="$(dirname "$SCRIPT_DIR")"
TALON_BIN_DIR="$(dirname "$TALON_DIR")/talon-bin"
TALON_SDK_DIR="$(dirname "$TALON_DIR")/talon-sdk"

# 验证仓库存在
[ -d "$TALON_DIR/.git" ]     || error "talon 仓库不存在: $TALON_DIR"
[ -d "$TALON_BIN_DIR/.git" ] || error "talon-bin 仓库不存在: $TALON_BIN_DIR"
[ -d "$TALON_SDK_DIR/.git" ] || error "talon-sdk 仓库不存在: $TALON_SDK_DIR"

echo ""
echo "═══════════════════════════════════════════"
echo "  Talon Release $VERSION"
echo "═══════════════════════════════════════════"
echo ""
echo "  talon:     $TALON_DIR"
echo "  talon-bin: $TALON_BIN_DIR"
echo "  talon-sdk: $TALON_SDK_DIR"
echo ""
if [ "$SKIP_CONFIRM" = false ]; then
    REPLY=""
    read -p "确认发版 $VERSION？(y/N) " -n 1 -r REPLY || true
    echo ""
    [[ "${REPLY:-}" =~ ^[Yy]$ ]] || { warn "已取消"; exit 0; }
fi

# ── 1. talon-bin: 更新版本号 ──
info "更新 talon-bin Cargo.toml 版本号..."
cd "$TALON_BIN_DIR"
git pull --rebase origin main 2>/dev/null || true

CARGO_TOML="$TALON_BIN_DIR/talon-sys/Cargo.toml"
if [ -f "$CARGO_TOML" ]; then
    sed -i.bak "s/^version = \".*\"/version = \"$SEMVER\"/" "$CARGO_TOML"
    rm -f "${CARGO_TOML}.bak"
    git add talon-sys/Cargo.toml
    if ! git diff --cached --quiet; then
        git commit -m "chore: bump version to $VERSION"
        info "talon-bin Cargo.toml 已更新到 $SEMVER"
    else
        info "talon-bin Cargo.toml 版本已是 ${SEMVER}, 跳过"
    fi
fi

# ── 2. talon-sdk: 更新各语言 SDK 版本号 ──
info "更新 talon-sdk 各语言版本号..."
cd "$TALON_SDK_DIR"
git pull --rebase origin main 2>/dev/null || true

SDK_CHANGED=false

# Python: pyproject.toml
PY_TOML="$TALON_SDK_DIR/python/pyproject.toml"
if [ -f "$PY_TOML" ]; then
    sed -i.bak "s/^version = \".*\"/version = \"$SEMVER\"/" "$PY_TOML"
    rm -f "${PY_TOML}.bak"
    git add python/pyproject.toml
fi

# Node.js: package.json
NODE_PKG="$TALON_SDK_DIR/nodejs/package.json"
if [ -f "$NODE_PKG" ]; then
    sed -i.bak "s/\"version\": \".*\"/\"version\": \"$SEMVER\"/" "$NODE_PKG"
    rm -f "${NODE_PKG}.bak"
    git add nodejs/package.json
fi

# .NET: Talon.csproj
CSPROJ="$TALON_SDK_DIR/dotnet/Talon/Talon.csproj"
if [ -f "$CSPROJ" ]; then
    sed -i.bak "s|<Version>.*</Version>|<Version>$SEMVER</Version>|" "$CSPROJ"
    rm -f "${CSPROJ}.bak"
    git add dotnet/Talon/Talon.csproj
fi

# Java: pom.xml (项目版本，不改依赖版本)
POM="$TALON_SDK_DIR/java/pom.xml"
if [ -f "$POM" ]; then
    # 只替换 <artifactId>talon-java</artifactId> 后的第一个 <version>
    sed -i.bak '/<artifactId>talon-java<\/artifactId>/{n;s|<version>.*</version>|<version>'"$SEMVER"'</version>|;}' "$POM"
    rm -f "${POM}.bak"
    git add java/pom.xml
fi

if ! git diff --cached --quiet; then
    git commit -m "chore: bump SDK versions to $VERSION"
    info "talon-sdk 各语言版本已更新到 $SEMVER"
    SDK_CHANGED=true
else
    info "talon-sdk 版本已是 ${SEMVER}, 跳过"
fi

# ── 3. talon (主仓库): 更新版本号 + 打 tag ──
info "更新 talon Cargo.toml 版本号..."
cd "$TALON_DIR"
git pull --rebase origin main 2>/dev/null || true

MAIN_CARGO_TOML="$TALON_DIR/Cargo.toml"
if [ -f "$MAIN_CARGO_TOML" ]; then
    sed -i.bak "s/^version = \".*\"/version = \"$SEMVER\"/" "$MAIN_CARGO_TOML"
    rm -f "${MAIN_CARGO_TOML}.bak"
    git add Cargo.toml
    if ! git diff --cached --quiet; then
        git commit -m "chore: bump version to $VERSION"
        info "talon Cargo.toml 已更新到 $SEMVER"
    else
        info "talon Cargo.toml 版本已是 ${SEMVER}, 跳过"
    fi
fi

info "talon 主仓库打 tag $VERSION..."
if git tag -l "$VERSION" | grep -q "$VERSION"; then
    warn "talon tag $VERSION 已存在，跳过"
else
    git tag "$VERSION"
    info "talon tag $VERSION 已创建"
fi

# ── 4. 推送 ──
info "推送 talon-bin..."
cd "$TALON_BIN_DIR"
git push origin main 2>/dev/null || true
git tag "$VERSION" 2>/dev/null || true
git push origin "$VERSION" 2>/dev/null || true
info "talon-bin $VERSION 已推送"

if [ "$SDK_CHANGED" = true ]; then
    info "推送 talon-sdk..."
    cd "$TALON_SDK_DIR"
    git push origin main 2>/dev/null || true
    info "talon-sdk 版本号已推送"
fi

info "推送 talon..."
cd "$TALON_DIR"
git push origin main
git push origin "$VERSION"
info "talon $VERSION 已推送 → CI 将自动构建并更新 talon-sdk"

# ── 5. talon-sdk: tag 由 CI 自动打 ──
# talon-sdk 的 tag 由 CI 的 update-sdk job 在推送完 libtalon.a 后自动打
info "talon-sdk tag 将由 CI 自动创建（确保包含预编译库）"

echo ""
echo "═══════════════════════════════════════════"
echo "  ✅ Release $VERSION 完成"
echo "═══════════════════════════════════════════"
echo ""
echo "  talon:     tag $VERSION → CI 构建中..."
echo "  talon-bin: tag $VERSION + Cargo.toml $SEMVER"
echo "  talon-sdk: 版本 $SEMVER + 等待 CI 打 tag"
echo ""
echo "  CI 完成后可用:"
echo "    go get github.com/darkmice/talon-sdk@$VERSION"
echo "    pip install talon-db==$SEMVER"
echo "    npm install talon-db@$SEMVER"
echo ""
