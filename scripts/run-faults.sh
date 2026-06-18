#!/usr/bin/env bash
#
# Runs the narrated fault-tolerance demos (consumer crash -> redelivery, broker
# disconnect -> retry -> circuit breaker -> reconnect, poison -> dead-letter)
# for every broker in BOTH languages.
#
# Usage:
#   scripts/run-faults.sh              # all brokers, both languages
#   scripts/run-faults.sh artemis      # one broker, both languages
#   scripts/run-faults.sh in-memory    # no broker required (faults are injected)
#
set -euo pipefail

# Allow running on a host that only has a newer .NET runtime than the net8.0
# target (harmless when the .NET 8 SDK is present).
export DOTNET_ROLL_FORWARD="${DOTNET_ROLL_FORWARD:-LatestMajor}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-all}"
if [[ "$TARGET" == "all" ]]; then
  BROKERS=(artemis rabbitmq)
else
  BROKERS=("$TARGET")
fi

if [[ "$TARGET" != "in-memory" ]]; then
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
fi

run_ts() {
  echo ""
  echo "######################################################################"
  echo "# TypeScript fault demo :: $1"
  echo "######################################################################"
  ( cd "$ROOT/typescript" && npm run --silent fault -- "$1" ) || true
}

run_dotnet() {
  echo ""
  echo "######################################################################"
  echo "# .NET fault demo :: $1"
  echo "######################################################################"
  ( cd "$ROOT/dotnet" && dotnet run --project src/Messaging.Runner -c Release -- fault "$1" ) || true
}

( cd "$ROOT/typescript" && npm install --silent )

for b in "${BROKERS[@]}"; do
  run_ts "$b"
  run_dotnet "$b"
done

echo ""
echo "==> Done."
