#!/usr/bin/env python3
import re
import sys
from collections import defaultdict

def analyze_logs(log_file):
    # Track broadcasts (sender_id, message, broadcast_time)
    broadcasts = []

    # Dictionary to store delivery sequences per node
    node_deliveries = defaultdict(list)

    try:
        with open(log_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                # Match RCO-BROADCAST lines
                broadcast_match = re.search(
                    r'node(\d+)-1.*\[RCO-BROADCAST\] Node (\d+): Broadcasting message \[(\d+): "([^"]+)"\]',
                    line
                )
                if broadcast_match:
                    node_id = int(broadcast_match.group(2))
                    sender_id = int(broadcast_match.group(3))
                    message = broadcast_match.group(4)
                    broadcasts.append({
                        'sender': sender_id,
                        'message': message,
                        'line': line_num,
                        'label': f"{sender_id}:{message}"
                    })

                # Match RCO-DELIVER lines
                deliver_match = re.search(
                    r'node(\d+)-1.*\[RCO-DELIVER\] Node (\d+): Delivered message from sender (\d+): "([^"]+)".*VC=(\[.*?\])',
                    line
                )
                if deliver_match:
                    node_id = int(deliver_match.group(2))
                    sender_id = int(deliver_match.group(3))
                    message = deliver_match.group(4)
                    vector_clock = deliver_match.group(5)

                    node_deliveries[node_id].append({
                        'sender': sender_id,
                        'message': message,
                        'vc': vector_clock,
                        'line': line_num,
                        'label': f"{sender_id}:{message}"
                    })

    except FileNotFoundError:
        print(f"Error: {log_file} not found")
        sys.exit(1)

    if broadcasts:
        by_sender = defaultdict(list)
        for bcast in broadcasts:
            by_sender[bcast['sender']].append(bcast)

        for sender_id in sorted(by_sender.keys()):
            msgs = by_sender[sender_id]
            if len(msgs) >= 1:
                msg_chain = " -> ".join([f"\"{m['message']}\"" for m in msgs])
                print(f"  Sender {sender_id}: {msg_chain}")

    if node_deliveries:
        for node_id in sorted(node_deliveries.keys()):
            deliveries = node_deliveries[node_id]
            delivery_str = ' -> '.join([d['label'] for d in deliveries])
            print(f"Node {node_id}: {delivery_str}")

if __name__ == "__main__":
    analyze_logs(sys.argv[1])
