#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }

# --- Prompt for image name ---
DEFAULT_IMAGE="thunderdb"
read -rp "Image name [${DEFAULT_IMAGE}]: " IMAGE_NAME
IMAGE_NAME="${IMAGE_NAME:-$DEFAULT_IMAGE}"

# --- Prompt for tag ---
GIT_SHORT_SHA=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")
DEFAULT_TAG="latest"
read -rp "Tag [${DEFAULT_TAG}]: " IMAGE_TAG
IMAGE_TAG="${IMAGE_TAG:-$DEFAULT_TAG}"

# --- Prompt for registry/username ---
read -rp "Registry/Username (e.g. saurav7055 or ghcr.io/org): " REGISTRY
if [[ -z "$REGISTRY" ]]; then
    error "Registry/username is required."
    exit 1
fi

FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
FULL_IMAGE_SHA="${REGISTRY}/${IMAGE_NAME}:${GIT_SHORT_SHA}"

info "Will build and push:"
info "  ${FULL_IMAGE}"
info "  ${FULL_IMAGE_SHA}"
echo ""

# --- Check Docker login ---
check_docker_login() {
    local registry_host="$1"
    local config_file="${DOCKER_CONFIG:-$HOME/.docker}/config.json"

    if [[ ! -f "$config_file" ]]; then
        return 1
    fi

    # For Docker Hub, check for https://index.docker.io/v1/
    if [[ "$registry_host" == "docker.io" || "$registry_host" == "dockerhub" ]]; then
        if grep -q "index.docker.io" "$config_file" 2>/dev/null; then
            return 0
        fi
        return 1
    fi

    # For other registries, check for the host in config
    if grep -q "$registry_host" "$config_file" 2>/dev/null; then
        return 0
    fi
    return 1
}

# Determine registry host for login check
if [[ "$REGISTRY" == *"."* ]]; then
    REGISTRY_HOST="$REGISTRY"
else
    REGISTRY_HOST="docker.io"
fi

if ! check_docker_login "$REGISTRY_HOST"; then
    warn "Not logged in to ${REGISTRY_HOST}."
    info "Running docker login..."
    if [[ "$REGISTRY_HOST" == "docker.io" ]]; then
        docker login
    else
        docker login "$REGISTRY_HOST"
    fi
    ok "Docker login successful."
else
    ok "Already logged in to ${REGISTRY_HOST}."
fi

# --- Select platform ---
HOST_ARCH=$(uname -m)
echo ""
info "Host architecture: ${HOST_ARCH}"
echo "Select target platform:"
echo "  1) linux/amd64"
echo "  2) linux/arm64"
echo "  3) linux/amd64,linux/arm64 (multi-arch)"
echo "  4) native (fastest build)"
read -rp "Choice [4]: " PLATFORM_CHOICE
PLATFORM_CHOICE="${PLATFORM_CHOICE:-4}"

case "$PLATFORM_CHOICE" in
    1) PLATFORM="linux/amd64" ;;
    2) PLATFORM="linux/arm64" ;;
    3) PLATFORM="linux/amd64,linux/arm64" ;;
    4) PLATFORM="" ;;
    *) error "Invalid choice"; exit 1 ;;
esac

# --- Ensure buildx builder exists ---
BUILDER_NAME="thunderdb-builder"
if ! docker buildx inspect "$BUILDER_NAME" &>/dev/null; then
    info "Creating buildx builder: ${BUILDER_NAME}"
    docker buildx create --name "$BUILDER_NAME" --driver docker-container --bootstrap
fi
docker buildx use "$BUILDER_NAME"

# --- Build and push ---
echo ""
info "Starting build..."
info "Project root: ${PROJECT_ROOT}"
echo ""

BUILD_ARGS=(
    "buildx" "build"
    "--file" "${PROJECT_ROOT}/Dockerfile"
    "--tag" "$FULL_IMAGE"
    "--tag" "$FULL_IMAGE_SHA"
    "--push"
)

if [[ -n "$PLATFORM" ]]; then
    BUILD_ARGS+=("--platform" "$PLATFORM")
fi

BUILD_ARGS+=("${PROJECT_ROOT}")

START_TIME=$(date +%s)
info "Running: docker ${BUILD_ARGS[*]}"
echo ""

docker "${BUILD_ARGS[@]}"

END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))
MINUTES=$(( ELAPSED / 60 ))
SECONDS=$(( ELAPSED % 60 ))

echo ""
ok "Build and push completed in ${MINUTES}m ${SECONDS}s"
ok "Pushed: ${FULL_IMAGE}"
ok "Pushed: ${FULL_IMAGE_SHA}"
echo ""
info "Pull with: docker pull ${FULL_IMAGE}"
