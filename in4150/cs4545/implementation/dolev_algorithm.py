from typing import Tuple, Set, Dict
from cs4545.system.da_types import *
import os
import random
import asyncio
from collections import defaultdict

@dataclass(msg_id=4)
class DolevMessage:
    """Dolev layer message - for authenticated message delivery"""
    sender_id: int
    content: str
    path: Tuple[int, ...]

    def __init__(self, sender_id: int, content: str, path: Tuple[int, ...]):
        self.sender_id = sender_id
        self.content = content
        self.path = path

    @property
    def key(self):
        return (self.sender_id, self.content)

class DolevAlgorithm(DistributedAlgorithm):
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)
        self.add_message_handler(DolevMessage, self.on_message)

        # Environment parameters
        self.f = int(os.getenv('FAULTS', '0'))
        self.min_message_delay = float(os.getenv('MIN_MESSAGE_DELAY', '0.01'))
        self.max_message_delay = float(os.getenv('MAX_MESSAGE_DELAY', '0.1'))
        self.num_nodes = int(os.getenv('NUM_NODES', '1'))
        self.num_messages_to_broadcast = int(os.getenv('NUM_BROADCASTS', '1'))

        # Byzantine behavior configuration
        self.byzantine_behavior = os.getenv('BYZANTINE_BEHAVIOR', 'none')
        self.limited_neighbors = int(os.getenv('LIMITED_NEIGHBORS', '1'))

        # Debug mode: 0 = no logs, 1 = only RC-DELIVER, 2 = all logs
        self.debug_mode = int(os.getenv('DEBUG_MODE', '1'))
        self.debug_algorithm = os.getenv('DEBUG_ALGORITHM', 'all')

        # Dolev algorithm state (per message using message.key)
        # Dict mapping message.key -> bool (whether message has been delivered)
        self.delivered: Dict[Tuple[int, str], bool] = {}

        # Dict mapping message.key -> set of paths (each path is a tuple of node IDs)
        self.paths: Dict[Tuple[int, str], Set[Tuple[int, ...]]] = defaultdict(set)

        # Optimization state (MD.1-MD.5)
        # MD.3: Track which neighbors have delivered each message
        self.neighbors_delivered: Dict[Tuple[int, str], Set[int]] = defaultdict(set)

        # MD.4: Track neighbors that sent empty paths for each message
        self.empty_path_senders: Dict[Tuple[int, str], Set[int]] = defaultdict(set)

        # MD.5: Track if empty path has been forwarded after delivery
        self.empty_path_forwarded: Dict[Tuple[int, str], bool] = {}

    async def on_start(self) -> None:
        await super().on_start()
        
        for i in range(self.num_messages_to_broadcast):
            message_content = f"Message-{i}"
            await self.rc_broadcast(message_content)
        

    async def rc_broadcast(self, content: str) -> None:
        """
        Reliable Communication Broadcast

        Initiates a broadcast of content from this node.
        Sends message with empty path to all neighbors and delivers locally.
        """
        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'dolev']:
            print(f"[RC-BROADCAST] Node {self.node_id}: Broadcasting message '{content}'")

        # Send to all neighbors with empty path
        msg = DolevMessage(
                sender_id=self.node_id,
                content=content,
                path=tuple()  # Empty path
            )
        await self._send_message_to_peers(msg, self.get_peers())

        self.delivered[msg.key] = True
        await self.rc_deliver(self.node_id, content)

    @message_wrapper(DolevMessage)
    async def on_message(self, peer: Peer, msg: DolevMessage) -> None:
        """
        Process incoming message with optimizations

        MD.1: Direct delivery from source
        MD.2: Empty path relay after delivery
        MD.3: Only relay to non-delivered neighbors
        MD.4: Ignore paths containing empty-path senders
        MD.5: Stop relaying after delivery + empty path sent
        """
        sender_peer_id = self.node_id_from_peer(peer)
        msg_key = msg.key

        # Byzantine behavior: Handle malicious actions
        if self.byzantine_behavior == 'no_relay':
            # Byzantine node doesn't relay any messages
            if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'dolev']:
                print(f"[BYZANTINE] Node {self.node_id}: Not relaying message from {msg.sender_id}")
            return
        
        elif self.byzantine_behavior == 'forge_sender':
            await self._attempt_forgery()

        # MD.5: Stop processing if already delivered and empty path forwarded
        if self.delivered.get(msg_key, False) and self.empty_path_forwarded.get(msg_key, False):
            return

        # Check if path is empty (direct from source or post-delivery relay)
        is_empty_path = len(msg.path) == 0

        # Construct the new path: received path + sender
        new_path = tuple(msg.path) + (sender_peer_id,)

        if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'dolev']:
            path_display = "[]" if is_empty_path else str(new_path)
            print(f"[RC-RECEIVE] Node {self.node_id}: Received from peer {sender_peer_id} | "
                  f"Source={msg.sender_id}, Content='{msg.content}', Path={path_display}")

        # MD.4: If empty path received, record sender and ignore future paths with this sender
        if is_empty_path:
            self.empty_path_senders[msg_key].add(sender_peer_id)
            self.neighbors_delivered[msg_key].add(sender_peer_id)

        # MD.1: If received directly from source (path is empty), deliver immediately
        if is_empty_path and msg.sender_id == sender_peer_id and not self.delivered.get(msg_key, False):
            self.delivered[msg_key] = True
            await self.rc_deliver(msg.sender_id, msg.content)

            # MD.2: After delivery, relay with empty path to all neighbors
            await self._relay_empty_path(msg_key, msg.sender_id, msg.content)
            return

        # MD.4: Check if path contains any node that sent empty path - if so, ignore
        if not is_empty_path:
            path_nodes = set(new_path[:-1])  # Exclude the last node (sender)
            if path_nodes & self.empty_path_senders[msg_key]:
                return

        self.paths[msg_key].add(new_path)

        # Check if we should deliver (if not already delivered)
        if not self.delivered.get(msg_key, False):
            # Check for f+1 node-disjoint paths
            if self._has_f_plus_one_disjoint_paths(msg_key):
                self.delivered[msg_key] = True
                await self.rc_deliver(msg.sender_id, msg.content)

                # MD.2 & MD.5: After delivery, relay with empty path to all neighbors
                await self._relay_empty_path(msg_key, msg.sender_id, msg.content)
                return

        # Forward to neighbors (if not already delivered)
        if not self.delivered.get(msg_key, False):
            path_set = set(tuple(msg.path))
            neighbors_to_forward = set(self.nodes.keys()) - path_set - {sender_peer_id}

            # MD.3: Only relay to neighbors that have not delivered
            neighbors_to_forward = neighbors_to_forward - self.neighbors_delivered[msg_key]
            peers = [self.nodes[n_id] for n_id in neighbors_to_forward]
            forward_msg = DolevMessage(
                sender_id=msg.sender_id,
                content=msg.content,
                path=new_path
            )
            await self._send_message_to_peers(forward_msg, peers)

    async def _relay_empty_path(self, msg_key: Tuple[int, str], sender_id: int, content: str) -> None:
        if self.empty_path_forwarded.get(msg_key, False):
            return  # Already forwarded empty path

        empty_msg = DolevMessage(
                sender_id=sender_id,
                content=content,
                path=tuple()  # Empty path
            )
        await self._send_message_to_peers(empty_msg, self.get_peers())

        # MD.5: Mark empty path as forwarded
        self.empty_path_forwarded[msg_key] = True

        # Discard paths to save memory (MD.2)
        if msg_key in self.paths:
            del self.paths[msg_key]

    async def rc_deliver(self, sender_id: int, content: str) -> None:
        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'dolev']:
            print(f"[RC-DELIVER] Node {self.node_id}: Delivered message from {sender_id}: '{content}'")

    def _has_f_plus_one_disjoint_paths(self, msg_key: Tuple[int, str]) -> bool:
        """
        Check if we have at least f+1 node-disjoint paths

        Two paths are node-disjoint if they don't share any intermediate nodes
        (excluding the source and destination).

        Returns True if at least f+1 node-disjoint paths exist.
        """
        paths_list = list(self.paths[msg_key])

        if len(paths_list) < self.f + 1:
            return False

        # Find maximum set of node-disjoint paths using greedy approach
        disjoint_paths = []

        for path in paths_list:
            is_disjoint = True

            # Get intermediate nodes (exclude destination which is this node)
            path_nodes = set(path[:-1]) if len(path) > 0 else set()

            for selected_path in disjoint_paths:
                selected_nodes = set(selected_path[:-1]) if len(selected_path) > 0 else set()

                # If paths share any intermediate nodes, they're not disjoint
                if path_nodes & selected_nodes:
                    is_disjoint = False
                    break

            if is_disjoint:
                disjoint_paths.append(path)

                if len(disjoint_paths) >= self.f + 1:
                    return True

        return False

    async def _attempt_forgery(self) -> None:
        # Pick a random node to impersonate (not ourselves)
        victim_node = random.choice([n for n in range(self.num_nodes) if n != self.node_id])

        # Create forged message claiming to be from victim
        forged_content = f"FORGED-Message-from-{victim_node}"
        forged_msg = DolevMessage(
            sender_id=victim_node,  # Claim to be from victim
            content=forged_content,
            path=tuple()  # Empty path to appear as original broadcast
        )

        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'dolev']:
            print(f"[BYZANTINE-FORGERY] Node {self.node_id}: Attempting to forge message from node {victim_node}")

        await self._send_message_to_peers(forged_msg, self.get_peers())

    async def _send_message_to_peers(self, message: DolevMessage, peers) -> None:
        async def send_with_delay(peer):
            delay = random.uniform(self.min_message_delay, self.max_message_delay)
            await asyncio.sleep(delay)
            self.ez_send(peer, message)

        tasks = []
        for peer in peers:
            tasks.append(asyncio.create_task(send_with_delay(peer)))

        await asyncio.gather(*tasks)