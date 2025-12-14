#!/bin/bash

echo "Test Bracha: Faulty Node Broadcast"

NUM_NODES=10
NUM_FAULTS=1
NUM_BYZANTINE=0 # broadcast is byzantine if byzantine behavior is limited_broadcast
BYZANTINE_BEHAVIOR="limited_broadcast"
LIMITED_NEIGHBORS=2
BROADCASTERS=1 
BROADCASTS=1
MIN_MESSAGE_DELAY=0.05
MAX_MESSAGE_DELAY=0.15
CONNECTIVITY=4
DEBUG_MODE=2
DEBUG_ALGORITHM="bracha"

echo "Generating docker-compose configuration..."
python3 -m cs4545.system.util compose \
    $NUM_NODES \
    topologies/testing.yaml \
    bracha \
    --overwrite_topology \
    $NUM_FAULTS \
    --num_byzantine $NUM_BYZANTINE \
    --limited_neighbors $LIMITED_NEIGHBORS \
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
