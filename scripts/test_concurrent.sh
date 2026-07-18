#!/bin/bash
# Usage: bash scripts/test_concurrent.sh <count> <port1> [port2]

COUNT=${1:-200}
PORT1=${2:-8081}
PORT2=${3:-}
TMPDIR=$(mktemp -d)

echo "=== Firing $COUNT clients at port $PORT1 ==="
for i in $(seq 1 $COUNT); do
  (echo "client-$i" | nc -w 2 127.0.0.1 $PORT1 > "$TMPDIR/a_$i.txt") &
done

if [ -n "$PORT2" ]; then
  echo "=== Firing $COUNT clients at port $PORT2 ==="
  for i in $(seq 1 $COUNT); do
    (echo "client-$i" | nc -w 2 127.0.0.1 $PORT2 > "$TMPDIR/b_$i.txt") &
  done
fi

echo "=== Waiting for all clients to finish... ==="
wait

echo ""
echo "============================================"
echo "  DETAILED LOGS — Port $PORT1"
echo "============================================"
for i in $(seq 1 $COUNT); do
  RESP=$(cat "$TMPDIR/a_$i.txt" 2>/dev/null)
  if [ -z "$RESP" ]; then
    echo "  client-$i  →  [EMPTY]"
  else
    echo "  client-$i  →  $RESP"
  fi
done

if [ -n "$PORT2" ]; then
  echo ""
  echo "============================================"
  echo "  DETAILED LOGS — Port $PORT2"
  echo "============================================"
  for i in $(seq 1 $COUNT); do
    RESP=$(cat "$TMPDIR/b_$i.txt" 2>/dev/null)
    if [ -z "$RESP" ]; then
      echo "  client-$i  →  [EMPTY]"
    else
      echo "  client-$i  →  $RESP"
    fi
  done
fi

echo ""
echo "============================================"
echo "  SUMMARY"
echo "============================================"
echo ""

A_COUNT=$(grep -rl "SERVICE-A" "$TMPDIR"/a_*.txt 2>/dev/null | wc -l | tr -d ' ')
A_EMPTY=$(find "$TMPDIR" -name 'a_*.txt' -empty | wc -l | tr -d ' ')
echo "  Port $PORT1 → SERVICE-A : $A_COUNT / $COUNT"
echo "  Port $PORT1 → Empty     : $A_EMPTY / $COUNT"

if [ -n "$PORT2" ]; then
  B_COUNT=$(grep -rl "SERVICE-B" "$TMPDIR"/b_*.txt 2>/dev/null | wc -l | tr -d ' ')
  B_EMPTY=$(find "$TMPDIR" -name 'b_*.txt' -empty | wc -l | tr -d ' ')
  echo "  Port $PORT2 → SERVICE-B : $B_COUNT / $COUNT"
  echo "  Port $PORT2 → Empty     : $B_EMPTY / $COUNT"
fi

# Cross-talk check
CROSSTALK_A=$(grep -rl "SERVICE-B" "$TMPDIR"/a_*.txt 2>/dev/null | wc -l | tr -d ' ')
if [ -n "$PORT2" ]; then
  CROSSTALK_B=$(grep -rl "SERVICE-A" "$TMPDIR"/b_*.txt 2>/dev/null | wc -l | tr -d ' ')
else
  CROSSTALK_B=0
fi

echo ""
if [ "$CROSSTALK_A" -eq 0 ] && [ "$CROSSTALK_B" -eq 0 ]; then
  echo "  ✅ VERDICT: ZERO CROSS-TALK — All responses routed correctly!"
else
  echo "  ❌ VERDICT: CROSS-TALK DETECTED — $CROSSTALK_A wrong on port1, $CROSSTALK_B wrong on port2"
fi
echo ""

rm -rf "$TMPDIR"
