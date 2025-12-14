import json
import math
from typing import Tuple, Set, Dict
from cs4545.system.da_types import *
from cs4545.implementation.dolev_algorithm import DolevAlgorithm, DolevMessage
import os
from collections import defaultdict
import random

@dataclass(msg_id=5)
class BrachaMessage:
    """Bracha layer message - for reliable broadcast protocol"""
    sender_id: int
    content: str
    msg_type: str  # "SEND", "ECHO", or "READY"

    def __init__(self, sender_id: int, content: str, msg_type: str):
        self.sender_id = sender_id
        self.content = content
        self.msg_type = msg_type

    @property
    def key(self):
        return (self.sender_id, self.content)

    def to_json(self) -> str:
        """Serialize for passing to Dolev layer"""
        return json.dumps({
            'content': self.content,
            'msg_type': self.msg_type,
            'sender_id': self.sender_id
        })

    @staticmethod
    def from_json(json_str: str) -> 'BrachaMessage':
        """Deserialize from Dolev layer"""
        obj = json.loads(json_str)
        return BrachaMessage(
            sender_id=obj['sender_id'],
            content=obj['content'],
            msg_type=obj['msg_type']
        )


class BrachaAlgorithm(DolevAlgorithm):
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)

        # Override debug algorithm if not explicitly set for Bracha
        # This allows seeing Bracha logs by default when using Bracha algorithm
        if self.debug_algorithm == 'all':
            self.debug_algorithm = os.getenv('DEBUG_ALGORITHM', 'all')

        # Optimization flags
        self.opt_echo_amplification = os.getenv('OPT_ECHO_AMPLIFICATION', 'false').lower() == 'true'
        self.opt_single_hop_send = os.getenv('OPT_SINGLE_HOP_SEND', 'false').lower() == 'true'
        self.opt_reduced_messages = os.getenv('OPT_REDUCED_MESSAGES', 'false').lower() == 'true'

        # Bracha state per message (using message.key)
        # Track if we sent ECHO for each message
        self.sent_echo: Dict[Tuple[int, str], bool] = {}

        # Track if we sent READY for each message
        self.sent_ready: Dict[Tuple[int, str], bool] = {}

        # Track if we delivered each message
        self.bracha_delivered: Dict[Tuple[int, str], bool] = {}

        # Track ECHOs received per message: msg_key -> set of node_ids
        self.echos: Dict[Tuple[int, str], Set[int]] = defaultdict(set)

        # Track READYs received per message: msg_key -> set of node_ids
        self.readys: Dict[Tuple[int, str], Set[int]] = defaultdict(set)

    def _should_generate_echo(self, broadcaster_id: int) -> bool:
        """
        Optimization MBD.11: Determine if this node should generate ECHO messages.

        The ⌈(N+f+1)/2⌉+f nodes with smallest IDs after broadcaster (modulo N) generate ECHOs.
        """
        if not self.opt_reduced_messages:
            return True

        num_echo_nodes = math.ceil((self.num_nodes + self.f + 1) / 2) + self.f

        # Get node IDs in circular order starting after broadcaster
        nodes_after = [(broadcaster_id + i) % self.num_nodes for i in range(1, self.num_nodes + 1)]

        # First num_echo_nodes in this circular order should generate ECHOs
        echo_nodes = set(nodes_after[:num_echo_nodes])

        return self.node_id in echo_nodes

    def _should_generate_ready(self, broadcaster_id: int) -> bool:
        """
        Optimization MBD.11: Determine if this node should generate READY messages.

        The 2f+1+f nodes with smallest IDs after broadcaster (modulo N) generate READYs.
        """
        if not self.opt_reduced_messages:
            return True

        num_ready_nodes = 2 * self.f + 1 + self.f

        # Get node IDs in circular order starting after broadcaster
        nodes_after = [(broadcaster_id + i) % self.num_nodes for i in range(1, self.num_nodes + 1)]

        # First num_ready_nodes in this circular order should generate READYs
        ready_nodes = set(nodes_after[:num_ready_nodes])

        return self.node_id in ready_nodes

    async def on_start(self) -> None:
        """Override to broadcast using Bracha instead of raw Dolev"""
        await super().on_start()

        # Byzantine behavior: collude
        if self.byzantine_behavior == 'collude':
            await self._attempt_bracha_forgery()
            return

        for i in range(self.num_messages_to_broadcast):
            message_content = f"Message-{i}"
            await self.brb_broadcast(message_content)

    async def brb_broadcast(self, content: str) -> None:
        """
        Bracha Broadcast

        Initiates a BRB broadcast by sending SEND message to all nodes via Dolev layer.

        With single-hop optimization: SEND only goes to direct neighbors (single hop).
        """
        bracha_msg = BrachaMessage(
            sender_id=self.node_id,
            content=content,
            msg_type="SEND"
        )

        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BRB-BROADCAST] Node {self.node_id}: Broadcasting '{content}'")

        # Byzantine behavior: limited_broadcast
        if self.byzantine_behavior == 'limited_broadcast':
            # Only send to limited number of neighbors (not all)
            all_peers = self.get_peers()
            limited_peers = random.sample(all_peers, min(self.limited_neighbors, len(all_peers)))

            if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'bracha']:
                print(f"[BYZANTINE-LIMITED] Node {self.node_id}: Broadcasting to only {len(limited_peers)} neighbors")

            # Manually send Dolev message with empty path to limited peers only
            dolev_msg = DolevMessage(
                sender_id=self.node_id,
                content=bracha_msg.to_json(),
                path=tuple()
            )
            await self._send_message_to_peers(dolev_msg, limited_peers)

            # Still deliver locally (BRB-Validity requires broadcaster delivers)
            msg_key = bracha_msg.key
            self.delivered[msg_key] = True
            await self._handle_send(bracha_msg)
            return

        # Optimization: Single-hop Send messages
        if self.opt_single_hop_send:
            # Send SEND only to direct neighbors (don't use rc_broadcast which would relay)
            dolev_msg = DolevMessage(
                sender_id=self.node_id,
                content=bracha_msg.to_json(),
                path=tuple()
            )
            await self._send_message_to_peers(dolev_msg, self.get_peers())

            # Deliver locally and immediately send ECHO
            msg_key = bracha_msg.key
            self.delivered[msg_key] = True
            await self._handle_send(bracha_msg)
        else:
            # Use Dolev's rc_broadcast to send serialized Bracha message
            await self.rc_broadcast(bracha_msg.to_json())

    async def rc_deliver(self, sender_id: int, content: str) -> None:
        """
        Override Dolev's rc_deliver to handle Bracha messages.
        """
        # Deserialize Bracha message
        try:
            bracha_msg = BrachaMessage.from_json(content)
        except:
            # If not a Bracha message, call parent's rc_deliver
            await super().rc_deliver(sender_id, content)
            return

        if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BRB-RECEIVE] Node {self.node_id}: {bracha_msg.msg_type} from {sender_id} | "
                  f"Source={bracha_msg.sender_id}, Content='{bracha_msg.content}'")

        # Byzantine behavior: collude - support forged messages from other Byzantine nodes
        if self.byzantine_behavior == 'collude':
            await self._support_forgeries(bracha_msg, sender_id)

        # Process based on message type
        if bracha_msg.msg_type == "SEND":
            await self._handle_send(bracha_msg)
        elif bracha_msg.msg_type == "ECHO":
            await self._handle_echo(bracha_msg, sender_id)
        elif bracha_msg.msg_type == "READY":
            await self._handle_ready(bracha_msg, sender_id)

    async def _handle_send(self, msg: BrachaMessage) -> None:
        msg_key = msg.key

        # Optimization MBD.11: Check if we should generate ECHO
        if not self._should_generate_echo(msg.sender_id):
            if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
                print(f"[BRB-SKIP-ECHO] Node {self.node_id}: Not designated to send ECHO")
            return

        if not self.sent_echo.get(msg_key, False):
            self.sent_echo[msg_key] = True

            # Send ECHO to all nodes
            echo_msg = BrachaMessage(
                sender_id=msg.sender_id,
                content=msg.content,
                msg_type="ECHO"
            )

            if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
                print(f"[BRB-SEND-ECHO] Node {self.node_id}: Sending ECHO for '{msg.content}'")

            await self.rc_broadcast(echo_msg.to_json())

    async def _handle_echo(self, msg: BrachaMessage, sender_id: int) -> None:
        """
        Collect ECHOs. Upon ⌈(N+f+1)/2⌉ ECHOs (and not sentReady), send READY to all.
        """
        msg_key = msg.key

        # Record the ECHO from sender_id
        self.echos[msg_key].add(sender_id)

        echo_threshold = math.ceil((self.num_nodes + self.f + 1) / 2)

        if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BRB-ECHO-COUNT] Node {self.node_id}: {len(self.echos[msg_key])}/{echo_threshold} ECHOs")

        # Optimization: Echo amplification - upon f+1 ECHOs, send our ECHO
        if self.opt_echo_amplification:
            if len(self.echos[msg_key]) >= self.f + 1 and not self.sent_echo.get(msg_key, False):
                # Check if we should generate ECHO (MBD.11)
                if self._should_generate_echo(msg.sender_id):
                    self.sent_echo[msg_key] = True
                    echo_msg = BrachaMessage(
                        sender_id=msg.sender_id,
                        content=msg.content,
                        msg_type="ECHO"
                    )
                    if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
                        print(f"[BRB-ECHO-AMPLIFY] Node {self.node_id}: Amplifying ECHO after {len(self.echos[msg_key])} ECHOs")
                    await self.rc_broadcast(echo_msg.to_json())

        # Check if we have enough ECHOs to send READY
        if len(self.echos[msg_key]) >= echo_threshold and not self.sent_ready.get(msg_key, False):
            # Check if we should generate READY (MBD.11)
            if self._should_generate_ready(msg.sender_id):
                await self._send_ready(msg)

    async def _handle_ready(self, msg: BrachaMessage, sender_id: int) -> None:
        """
        Collect READYs. Upon f+1 READYs (and not sentReady), send READY to all.
        Upon 2f+1 READYs (and not delivered), deliver the message.
        """
        msg_key = msg.key

        self.readys[msg_key].add(sender_id)

        if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BRB-READY-COUNT] Node {self.node_id}: {len(self.readys[msg_key])} READYs")

        # Optimization: Echo amplification - upon receiving READY, send ECHO or READY
        if self.opt_echo_amplification:
            # Check if we can send READY (because of f+1 threshold)
            can_send_ready = len(self.readys[msg_key]) >= self.f + 1 and not self.sent_ready.get(msg_key, False)

            if can_send_ready and self._should_generate_ready(msg.sender_id):
                # If we can send READY, send READY (and mark ECHO as sent to avoid redundant ECHO)
                self.sent_echo[msg_key] = True
                await self._send_ready(msg)
            elif not self.sent_echo.get(msg_key, False) and self._should_generate_echo(msg.sender_id):
                # If we can't send READY yet, but haven't sent ECHO, send ECHO
                self.sent_echo[msg_key] = True
                echo_msg = BrachaMessage(
                    sender_id=msg.sender_id,
                    content=msg.content,
                    msg_type="ECHO"
                )
                if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
                    print(f"[BRB-ECHO-FROM-READY] Node {self.node_id}: Sending ECHO after receiving READY")
                await self.rc_broadcast(echo_msg.to_json())
        else:
            # Upon f+1 READYs, send our own READY (standard amplification)
            if len(self.readys[msg_key]) >= self.f + 1 and not self.sent_ready.get(msg_key, False):
                # Check if we should generate READY (MBD.11)
                if self._should_generate_ready(msg.sender_id):
                    await self._send_ready(msg)

        # Upon 2f+1 READYs, deliver
        if len(self.readys[msg_key]) >= 2 * self.f + 1 and not self.bracha_delivered.get(msg_key, False):
            await self._brb_deliver(msg)

    async def _send_ready(self, msg: BrachaMessage) -> None:
        """Send READY message to all nodes"""
        msg_key = msg.key
        self.sent_ready[msg_key] = True

        ready_msg = BrachaMessage(
            sender_id=msg.sender_id,
            content=msg.content,
            msg_type="READY"
        )

        if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BRB-SEND-READY] Node {self.node_id}: Sending READY for '{msg.content}'")

        await self.rc_broadcast(ready_msg.to_json())

    async def _brb_deliver(self, msg: BrachaMessage) -> None:
        msg_key = msg.key
        self.bracha_delivered[msg_key] = True

        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BRB-DELIVER] Node {self.node_id}: Delivered message from {msg.sender_id}: '{msg.content}'")

    async def _attempt_bracha_forgery(self) -> None:
        """Byzantine node attempts to forge a message from a correct node"""
        victim_node = random.choice([n for n in range(self.num_nodes) if n != self.node_id])

        forged_content = f"FORGED-Message-from-{victim_node}"

        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BYZANTINE-FORGERY] Node {self.node_id}: Attempting to forge message from node {victim_node}")

        # Send ECHO for forged message (Byzantine nodes collude by echoing each other's forgeries)
        echo_msg = BrachaMessage(
            sender_id=victim_node,
            content=forged_content,
            msg_type="ECHO"
        )
        await self.rc_broadcast(echo_msg.to_json())

        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BYZANTINE-FORGERY] Node {self.node_id}: Sending ECHO for forged message")

        # Also send READY to try to reach quorum
        ready_msg = BrachaMessage(
            sender_id=victim_node,
            content=forged_content,
            msg_type="READY"
        )
        await self.rc_broadcast(ready_msg.to_json())

        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'bracha']:
            print(f"[BYZANTINE-FORGERY] Node {self.node_id}: Sending READY for forged message")

    async def _support_forgeries(self, msg: BrachaMessage, sender_id: int) -> None:
        """Byzantine node supports forged messages from other Byzantine nodes"""
        msg_key = msg.key

        # If this looks like a forged message (content starts with "FORGED-"), support it
        if "FORGED-" in msg.content:
            if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'bracha']:
                print(f"[BYZANTINE-COLLUDE] Node {self.node_id}: Supporting forged message from {msg.sender_id}")

            # Immediately send ECHO if we received SEND
            if msg.msg_type == "SEND" and not self.sent_echo.get(msg_key, False):
                self.sent_echo[msg_key] = True
                echo_msg = BrachaMessage(
                    sender_id=msg.sender_id,
                    content=msg.content,
                    msg_type="ECHO"
                )
                await self.rc_broadcast(echo_msg.to_json())

            # Immediately send READY if we received ECHO or READY
            if (msg.msg_type in ["ECHO", "READY"]) and not self.sent_ready.get(msg_key, False):
                self.sent_ready[msg_key] = True
                ready_msg = BrachaMessage(
                    sender_id=msg.sender_id,
                    content=msg.content,
                    msg_type="READY"
                )
                await self.rc_broadcast(ready_msg.to_json())