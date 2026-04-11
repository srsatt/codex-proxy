#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${IMAGE_TAG:-codex-proxy:latest}"
HOST_PORT="${HOST_PORT:-18317}"
CONTAINER_NAME="${CONTAINER_NAME:-codex-proxy-release-check}"
COMPOSE_SERVICE="${COMPOSE_SERVICE:-codex-proxy}"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker compose down -v >/dev/null 2>&1 || true
}

wait_for_health() {
  local container_name="$1"
  local label="$2"

  for _ in $(seq 1 20); do
    local status
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_name}" 2>/dev/null || true)"
    if [[ "${status}" == "healthy" ]]; then
      echo "${label} is healthy"
      return 0
    fi
    sleep 1
  done

  echo "${label} did not become healthy" >&2
  docker logs "${container_name}" >&2 || true
  return 1
}

trap cleanup EXIT

echo "Running cargo test"
cargo test

echo "Building Docker image ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" .

echo "Starting standalone container smoke test"
docker run -d --rm \
  --name "${CONTAINER_NAME}" \
  -p "127.0.0.1:${HOST_PORT}:8317" \
  "${IMAGE_TAG}" >/dev/null
wait_for_health "${CONTAINER_NAME}" "standalone container"
docker rm -f "${CONTAINER_NAME}" >/dev/null

echo "Starting docker compose smoke test"
docker compose up --build -d
wait_for_health "${COMPOSE_SERVICE}" "docker compose service"

echo "Release validation completed successfully"
