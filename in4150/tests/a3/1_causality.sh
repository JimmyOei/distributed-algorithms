#!/bin/bash

NUM_NODES=10
NUM_FAULTS=1
NUM_BYZANTINE=0
BYZANTINE_BEHAVIOR="none"
BROADCASTERS=1
BROADCASTS=3
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
docker compose up
