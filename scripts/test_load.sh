#!/bin/bash

# A manual load-tester that blasts 1000 requests to the Gateway
# It sends them in 10 batches of 100 to prevent macOS from crashing due to "Too many open processes" limits.

PORT=8081
TOTAL=${1:-1000}
BATCH_SIZE=100
BATCHES=$((TOTAL / BATCH_SIZE))

echo "============================================="
echo "STARTING MASSIVE LOAD TEST ($TOTAL CLIENTS)"
echo "============================================="
echo "Target: 127.0.0.1:$PORT"
echo ""

TMPDIR=$(mktemp -d)

# Time the entire operation
START=$(date +%s)

for batch in $(seq 1 $BATCHES); do 
  echo "  -> Sending batch $batch (100 concurrent clients)..."
  for i in $(seq 1 $BATCH_SIZE); do
    # Calculate global request ID
    REQ_ID=$(( (batch - 1) * BATCH_SIZE + i ))
    
    # Send payload and save the backend's response to a file
    (echo "LOAD_TEST_$REQ_ID" | nc -w 1 127.0.0.1 $PORT > "$TMPDIR/req_${REQ_ID}.txt" 2>/dev/null) &
  done
  # Wait for this batch to finish
  wait
done

END=$(date +%s)
DURATION=$((END - START))

echo ""
echo "============================================="
echo "VERIFYING 1000 RESPONSES..."
echo "============================================="

# Count how many files successfully contain the backend's identity response
SUCCESS_COUNT=$(grep -rl "SERVICE-A" "$TMPDIR" 2>/dev/null | wc -l | tr -d ' ')
EMPTY_COUNT=$(find "$TMPDIR" -type f -empty | wc -l | tr -d ' ')

echo "Expected Responses : $TOTAL"
echo "Actual Responses   : $SUCCESS_COUNT"
echo "Dropped / Empty    : $EMPTY_COUNT"
echo ""

if [ "$SUCCESS_COUNT" -eq "$TOTAL" ]; then
  echo "SUCCESS: 1000/1000 Clients Routed Perfectly in $DURATION seconds!"
else
  echo "FAILED: Only $SUCCESS_COUNT/$TOTAL succeeded."
fi
echo "============================================="

rm -rf "$TMPDIR"
