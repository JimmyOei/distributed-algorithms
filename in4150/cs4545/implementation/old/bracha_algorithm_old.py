from typing import Tuple
from cs4545.system.da_types import *
import os
import random
import asyncio
from collections import defaultdict
import math

@dataclass(msg_id=4)
class DolevMessage:
    content: str
    msg_sender_id: int # Original sender of the message
    msg_type: str  # "SEND", "ECHO", or "READY"
    sender_id: int # Sender of message on Bracha layer
    path: Tuple[int]
    dolev_sender_id: int # Sender message on Dolev layer
    dolev_msg_sender_id: int # Original sender of the message on Dolev layer
    
    @property
    def bracha_key(self):
        return (self.msg_sender_id, self.content)

    @property
    def key(self):
        return (self.sender_id, self.content, self.msg_type)


class BrachaAlgorithm(DistributedAlgorithm):
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)
        self.add_message_handler(DolevMessage, self.on_ipv8_message)

        self.f = int(os.getenv('FAULTS', '0'))
        self.min_message_delay = float(os.getenv('MIN_MESSAGE_DELAY', '0.01')) # default 10ms
        self.max_message_delay = float(os.getenv('MAX_MESSAGE_DELAY', '0.1'))  # default 100ms
        self.num_nodes = int(os.getenv('NUM_NODES', '1'))
        self.num_messages_to_broadcast = int(os.getenv('NUM_BROADCASTS', '1'))

        # Byzantine behavior can be 'none', 'drop', 'alter_path', 'alter_sender', 'send_empty', 'limited_broadcast'
        self.byzantine_behavior = os.getenv('BYZANTINE_BEHAVIOR', 'none')
        self.limited_neighbors = int(os.getenv('LIMITED_NEIGHBORS', '1'))  # For limited_broadcast behavior


        self.dolev_delivered = {} # Dict mapping (message, sender_id, message_id) to node_ids that delivered it
        self.dolev_paths = {} # Dict mapping (message, sender_id, message_id) to set of paths (tuples of node_ids)
        
        self.sent_echo = set()
        self.sent_ready = set()
        self.bracha_delivered = set()
        self.echos = defaultdict(set)
        self.readys = defaultdict(set)

    async def on_start(self) -> None:
        print(f"[Start] Node {self.node_id} initialized with f={self.f}, message delay [{self.min_message_delay}, {self.max_message_delay}], NUM_BROADCASTS={self.num_messages_to_broadcast}, Byzantine Behavior={self.byzantine_behavior}, Nodes={self.num_nodes}")
        # Make sure to call this one last in this function
        await super().on_start()

        if self.num_messages_to_broadcast > 0:
            print(f"[Start] Node {self.node_id} Broadcasting {self.num_messages_to_broadcast} message(s)")
            for msg in range(self.num_messages_to_broadcast):
                content = f"Message {msg} from node {self.node_id}"

                # Byzantine behavior: limited_broadcast
                if self.byzantine_behavior == 'limited_broadcast':
                    await self.do_limited_broadcast(content, self.node_id, self.node_id, "SEND")
                else:
                    await self.do_dolev_broadcast(content, self.node_id, self.node_id, "SEND")

        # Byzantine collude behavior: Try to forge messages from a correct node
        if self.byzantine_behavior == 'collude':
            forged_sender_id = 0 if self.node_id != 0 else 1
            forged_content = f"FORGED MESSAGE claiming to be from node {forged_sender_id}"
            print(f"[Byzantine-Collude] Node {self.node_id}: Attempting to forge message from node {forged_sender_id}")
            await self.do_dolev_broadcast(forged_content, forged_sender_id, forged_sender_id, "SEND")
                
    async def send_message_to_peers(self, payload: DolevMessage) -> None:
        async def send_with_delay(peer):
            delay = random.uniform(self.min_message_delay, self.max_message_delay)
            await asyncio.sleep(delay)
            self.ez_send(peer, payload)
            
        peers = self.get_peers()
        tasks = []
        for peer in peers:
            peer_id = self.node_id_from_peer(peer)
            if (peer_id in payload.path) \
                or (payload.key in self.dolev_delivered and peer_id in self.dolev_delivered[payload.key]):
                continue
            tasks.append(asyncio.create_task(send_with_delay(peer)))
        
        await asyncio.gather(*tasks)

    async def do_dolev_broadcast(self, content: str, msg_sender_id: str, sender_id: str, msg_type: str) -> None:
        payload = DolevMessage(content, msg_sender_id, msg_type, sender_id, (), self.node_id, self.node_id)
        await self.send_message_to_peers(payload)
        await self.do_dolev_deliver(payload)

    async def do_limited_broadcast(self, content: str, msg_sender_id: str, sender_id: str, msg_type: str) -> None:
        payload = DolevMessage(content, msg_sender_id, msg_type, sender_id, (), self.node_id, self.node_id)
        peers = self.get_peers()
        limited_peers = random.sample(peers, min(self.limited_neighbors, len(peers)))

        print(f"[Byzantine-Limited] Node {self.node_id}: Broadcasting to only {len(limited_peers)}/{len(peers)} neighbors")

        async def send_with_delay(peer):
            delay = random.uniform(self.min_message_delay, self.max_message_delay)
            await asyncio.sleep(delay)
            self.ez_send(peer, payload)

        tasks = [asyncio.create_task(send_with_delay(peer)) for peer in limited_peers]
        await asyncio.gather(*tasks)

        await self.do_dolev_deliver(payload)       

    @message_wrapper(DolevMessage)
    async def on_ipv8_message(self, peer: Peer, payload: DolevMessage) -> None:
        try:
            sender_id = self.node_id_from_peer(peer)

            # Authentication check
            if sender_id != payload.dolev_sender_id:
                return

            # Drop if sender delivered
            if payload.key in self.dolev_delivered and self.node_id in self.dolev_delivered[payload.key]:
                return

            # Sender delivered, so update delivered set for this message
            if payload.path == ():
                if payload.key in self.dolev_delivered:
                    self.dolev_delivered[payload.key].add(sender_id)
                else:
                    self.dolev_delivered[payload.key] = set([sender_id])

            path = tuple(payload.path) + (sender_id,)

            nodes_that_delivered = self.dolev_delivered.get(payload.key, set())
            path_contains_delivered_node = any(node in nodes_that_delivered for node in path)
            if path_contains_delivered_node:
                return
            
            if payload.key not in self.dolev_paths:
                self.dolev_paths[payload.key] = set()
            self.dolev_paths[payload.key].add(path)
            
            send_payload = DolevMessage(payload.content, payload.msg_sender_id, payload.msg_type, payload.sender_id, path, self.node_id, payload.dolev_msg_sender_id)
            num_disjoint_paths = count_disjoint_paths(self.dolev_paths[payload.key]) if payload.key in self.dolev_paths else 0
            if (num_disjoint_paths >= self.f + 1 and payload.key not in self.dolev_delivered) \
                or (sender_id == payload.msg_sender_id and payload.path == ()):
                await self.do_dolev_deliver(payload)

                # When delivered, send with empty path
                send_payload.path = ()
            
            # Optimization Single-hop Send messages:
            # Don't forward SEND messages: since we get them from the sender directly, we immediately dolev-deliver it and send ECHO to neighbors instead
            if payload.msg_type != "SEND":
                await self.send_message_to_peers(send_payload)

        except Exception as e:
            print(f"[Error] on_ipv8_message: {e}")
            raise e
    
    async def do_dolev_deliver(self, payload: DolevMessage) -> None:
        # Authentication on Bracha layer
        if payload.sender_id != payload.dolev_msg_sender_id:
            return
        
        print(f"[Dolev] Node {self.node_id}: Message is delivered from sender {payload.sender_id} [{payload.msg_sender_id}: \"{payload.content}\"] (type: {payload.msg_type})")
        self.dolev_delivered[payload.key] = set([self.node_id])
        
        distance_from_source = (self.node_id - payload.msg_sender_id) % self.num_nodes

        if payload.msg_type in ["SEND", "ECHO"]:
            if not payload.bracha_key in self.sent_echo:
                self.sent_echo.add(payload.bracha_key)                    
                # Optimization 3
                if distance_from_source < math.ceil((self.num_nodes+self.f+1)/2) + self.f: 
                    await self.do_dolev_broadcast(payload.content, payload.msg_sender_id, self.node_id, "ECHO")

        if payload.msg_type == "ECHO":
            self.echos[payload.bracha_key].add(payload.sender_id)
            
        elif payload.msg_type == "READY":
            self.readys[payload.bracha_key].add(payload.sender_id)
        
        if not payload.bracha_key in self.sent_ready:
            if len(self.echos[payload.bracha_key]) >= math.ceil((self.num_nodes + self.f + 1) / 2) or \
                len(self.readys[payload.bracha_key]) >= self.f+1:
                self.sent_ready.add(payload.bracha_key)
                # Optimization 3
                if distance_from_source < (2*self.f + 1 + self.f):
                    await self.do_dolev_broadcast(payload.content, payload.msg_sender_id, self.node_id, "READY")
            
            elif not payload.bracha_key in self.sent_echo and \
                (payload.msg_type == "READY" or len(self.echos[payload.bracha_key]) >= self.f+1):
                # optimization 1
                self.sent_echo.add(payload.bracha_key)
                if distance_from_source < math.ceil((self.num_nodes+self.f+1)/2) + self.f:
                    # optimization 3
                    await self.do_dolev_broadcast(payload.content, payload.msg_sender_id, self.node_id, "ECHO")
                
        if not payload.bracha_key in self.bracha_delivered and len(self.readys[payload.bracha_key]) >= 2*self.f+1:
            self.do_bracha_deliver(payload)

    def do_bracha_deliver(self, payload: DolevMessage) -> None:
        self.bracha_delivered.add(payload.bracha_key)
        print(f"[Bracha] Node {self.node_id}: Message is delivered from sender {payload.sender_id} [{payload.msg_sender_id}: \"{payload.content}\"]")


def count_disjoint_paths(paths):
    sets = [set(s[1:]) for s in paths]
    n = len(sets)
    best = 0

    def backtrack(i, chosen, used):
        nonlocal best
        if i == n:
            best = max(best, len(chosen))
            return
        backtrack(i + 1, chosen, used)
        if sets[i].isdisjoint(used):
            backtrack(i + 1, chosen + [i], used | sets[i])

    backtrack(0, [], set())
    return best