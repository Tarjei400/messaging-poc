#!/usr/bin/env bash
#
# Lays out a tmux session that shows pub/sub behavior live across several panes:
# two fanout subscribers, one competing-consumer pair on a second queue, a DLQ
# watcher, and a publisher pane you drive by hand. Requires tmux + brokers up.
#
# Usage:
#   scripts/demo-tmux.sh [broker]      # default: rabbitmq
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROKER="${1:-rabbitmq}"
TS="$ROOT/typescript"
SESSION="mbc-demo"

command -v tmux >/dev/null || { echo "tmux is required"; exit 1; }

echo "==> Ensuring brokers are up"
docker compose -f "$ROOT/infra/docker-compose.yml" up -d --build >/dev/null
( cd "$TS" && npm install --silent )

tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$TS"

# Pane 0: fanout subscriber A.
tmux send-keys -t "$SESSION" "npm run demo:subscribe -- $BROKER orders --id A --kind fanout" C-m
# Pane 1: fanout subscriber B.
tmux split-window -h -t "$SESSION" -c "$TS"
tmux send-keys -t "$SESSION" "npm run demo:subscribe -- $BROKER orders --id B --kind fanout" C-m
# Pane 2: DLQ watcher for the 'jobs' topic.
tmux split-window -v -t "$SESSION" -c "$TS"
tmux send-keys -t "$SESSION" "npm run demo:subscribe -- $BROKER jobs.dlq --kind fanout --id dlq" C-m
# Pane 3: a flaky worker on 'jobs' (its failures dead-letter into the DLQ pane).
tmux select-pane -t "$SESSION".0
tmux split-window -v -t "$SESSION" -c "$TS"
tmux send-keys -t "$SESSION" "npm run demo:worker -- $BROKER jobs --id w1 --fail-rate 1" C-m
# Driver pane: you publish from here.
tmux split-window -v -t "$SESSION" -c "$TS"
tmux send-keys -t "$SESSION" "# Try:  npm run demo:publish -- $BROKER orders --count 5 --kind fanout" C-m
tmux send-keys -t "$SESSION" "# Then: npm run demo:publish -- $BROKER jobs --count 2" C-m

tmux select-layout -t "$SESSION" tiled
echo "==> Attaching. Detach with Ctrl-b d; kill with: tmux kill-session -t $SESSION"
tmux attach -t "$SESSION"
