#!/usr/bin/env bash
# Regression gate for the crash-replay-verification facility.
#
# Pins three things together:
#   1. crash reproduction: kill mid-write / flush-stall / truncated-WAL,
#      plus the clean-shutdown path;
#   2. replay verification: reopen the same data and read the synced keys back
#      with full values in stable order, with exact counts;
#   3. durability sync: Sync declarations backed by real fsyncs and fsync
#      errors propagated.
#
# The Go test additionally refuses to pass if unrelated compaction scheduling
# sources (compaction_scheduler.go, compaction_picker.go,
# read_compaction_queue.go) changed. Any failure reports its reason and exits
# non-zero.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "crash-replay gate: reproducing kills, verifying replay, auditing sync..."
go test ./crashreplay/ -count=1 -v
