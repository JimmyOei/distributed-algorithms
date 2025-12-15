#!/bin/bash

# Test Byzantine behavior: Vector Clock Inflation
# A Byzantine node inflates its vector clock values, causing correct nodes
# to delay delivery of its messages indefinitely

NUM_NODES=5
NUM_FAULTS=1
NUM_BYZANTINE=1
BYZANTINE_BEHAVIOR="vc_inflation"
BROADCASTERS=2  # Byzantine and one correct broadcaster
BROADCASTS=2
MIN_MESSAGE_DELAY=0.05
MAX_MESSAGE_DELAY=0.15
CONNECTIVITY=3
DEBUG_MODE=1
DEBUG_ALGORITHM="rco"

python3 -m cs4545.system.util compose \
    $NUM_NODES \
    topologies/dolev.yaml \
    rco \
    $NUM_FAULTS \
    --num_byzantine $NUM_BYZANTINE \
    --byzantine_behavior "$BYZANTINE_BEHAVIOR" \
    --broadcasters $BROADCASTERS \
    --broadcasts $BROADCASTS \
    --min_message_delay $MIN_MESSAGE_DELAY \
    --max_message_delay $MAX_MESSAGE_DELAY \
    --connectivity $CONNECTIVITY \
    --debug_mode $DEBUG_MODE \
    --debug_algorithm $DEBUG_ALGORITHM

if [ $? -ne 0 ]; then
    echo "Failed to generate configuration"
    exit 1
fi

docker compose build

LOG_FILE="byzantine_vc_inflation.log"
echo "Running test and capturing logs to $LOG_FILE..."

docker compose up 2>&1 | tee $LOG_FILE