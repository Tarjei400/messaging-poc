#!/usr/bin/env bash
#
# Brings up both brokers, then runs the full scenario suite for every broker in
# BOTH languages, so the terminal shows side-by-side outcomes.
#
# Usage:
#   scripts/run-all.sh              # all brokers, both languages
#   scripts/run-all.sh artemis      # one broker, both languages
#
set -euo pipefail

# Allow running on a host that only has a newer .NET runtime than the net8.0
# target (harmless when the .NET 8 SDK is present).
export DOTNET_ROLL_FORWARD="${DOTNET_ROLL_FORWARD:-LatestMajor}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROKERS=("${1:-all}")
if [[ "${BROKERS[0]}" == "all" ]]; then
  BROKERS=(artemis rabbitmq)
fi

echo "==> Starting brokers via docker compose"
docker compose -f "$ROOT/infra/docker-compose.yml" up -d --build

echo "==> Waiting for brokers to report healthy"
for svc in artemis rabbitmq; do
  cid="$(docker compose -f "$ROOT/infra/docker-compose.yml" ps -q "$svc" || true)"
  [[ -z "$cid" ]] && continue
  for _ in $(seq 1 40); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$cid" 2>/dev/null || echo starting)"
    [[ "$status" == "healthy" ]] && break
    sleep 2
  done
  echo "    $svc: ${status:-unknown}"
done

run_ts() {
  local broker="$1"
  echo ""
  echo "######################################################################"
  echo "# TypeScript :: $broker"
  echo "######################################################################"
  ( cd "$ROOT/typescript" && npm run --silent scenarios -- "$broker" ) || true
}

run_dotnet() {
  local broker="$1"
  echo ""
  echo "######################################################################"
  echo "# .NET :: $broker"
  echo "######################################################################"
  ( cd "$ROOT/dotnet" && dotnet run --project src/Messaging.Runner -c Release -- "$broker" ) || true
}

echo "==> Installing TypeScript deps (first run only)"
( cd "$ROOT/typescript" && npm install --silent )

echo "==> Restoring .NET deps (first run only)"
( cd "$ROOT/dotnet" && dotnet restore )

for b in "${BROKERS[@]}"; do
  run_ts "$b"
  run_dotnet "$b"
done

echo ""
echo "==> Done. To tear the brokers down:  docker compose -f infra/docker-compose.yml down -v"
