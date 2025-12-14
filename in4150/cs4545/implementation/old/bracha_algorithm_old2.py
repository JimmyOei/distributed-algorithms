from typing import Tuple, Literal, List
from cs4545.system.da_types import *
import os
import random
import asyncio
from collections import defaultdict
import math

@dataclass(msg_id=4)
class DolevMessage:
    content: str
    broadcast_sender_id: int
    msg_type: str

    dolev_sender_id: int
    path: Tuple[int]

    intended_targets: Tuple[int] # Only for limited broadcasts. Defines which nodes should Dolev-deliver this message
    
    @property
    def bracha_key(self):
        return (self.content, self.broadcast_sender_id)

    @property
    def dolev_key(self):
        return (self.content, self.broadcast_sender_id, self.msg_type, self.dolev_sender_id)
    



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

        self.delivered = set() # Set of keys (message, sender_id) that have been delivered
        self.empty_forwarded = set() # Set of keys (message, sender_id) for which the empty path has been forwarded
        self.who_delivered = defaultdict(set) # Dictionary mapping a key (message, sender_id) to a set of nodes that have delivered this message
        self.paths = defaultdict(set) # Dict mapping (message, sender_id, message_id) to set of paths (tuples of node_ids)
        
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
                    await self.do_limited_broadcast(content, self.node_id, "SEND")
                else:
                    #await self.do_dolev_broadcast(content, self.node_id, "SEND") # TEMP: ECHO implies SEND
                    message = DolevMessage(content, self.node_id, "SEND", self.node_id, (), ())
                    await self.do_dolev_deliver(message)

        # Byzantine collude behavior: Try to forge messages from a correct node
        if self.byzantine_behavior == 'collude':
            if self.node_id != 0:
                forged_sender_id = 0
                forged_content = f"FORGED MESSAGE claiming to be from node {forged_sender_id}"
                print(f"[Byzantine-Collude] Node {self.node_id}: Attempting to forge message from node {forged_sender_id}")
                await self.do_dolev_broadcast(forged_content, forged_sender_id, "SEND")
                await self.do_dolev_broadcast(forged_content, forged_sender_id, "ECHO")
                await self.do_dolev_broadcast(forged_content, forged_sender_id, "READY")
                
    async def send_message_to_peers(self, message: DolevMessage, path, peers=None) -> None:
        message.path = path

        async def send_with_delay(peer):
            delay = random.uniform(self.min_message_delay, self.max_message_delay)
            await asyncio.sleep(delay)
            self.ez_send(peer, message)
            
        if peers == None:
            peers = self.get_peers()
        tasks = []
        for peer in peers:
            tasks.append(asyncio.create_task(send_with_delay(peer)))
        
        await asyncio.gather(*tasks)

    async def do_dolev_broadcast(self, content: str, broadcast_sender_id: str, msg_type: str) -> None:
        print(f"[BROADCASTING] {self.node_id}  - {msg_type} {broadcast_sender_id} {content}")
        message = DolevMessage(content, broadcast_sender_id, msg_type, self.node_id, (), ())
        await self.send_message_to_peers(message, ())
        await self.do_dolev_deliver(message)

    async def do_dolev_broadcast_if_close(self, content: str, broadcast_sender_id: str, msg_type: str) -> None: # Bracha optimization 3: only broadcast ECHO / READY messages if node id is close to source
        # distance_from_source = (self.node_id - broadcast_sender_id) % self.num_nodes
        # if   (msg_type == "ECHO" and distance_from_source < math.ceil((self.num_nodes+self.f+1)/2) + self.f) \
        #   or (msg_type == "READY" and distance_from_source < 3*self.f + 1):
        await self.do_dolev_broadcast(content, broadcast_sender_id, msg_type)

    async def do_limited_broadcast(self, content: str, broadcast_sender_id: str, msg_type: str) -> None:
        other_node_ids = list(set(range(self.num_nodes)).difference([self.node_id]))
        intended_targets = tuple(random.sample(other_node_ids, self.limited_neighbors))
        print(f"[Byzantine] Node {self.node_id}: sending limited broadcast to {intended_targets}")
        message = DolevMessage(content, broadcast_sender_id, msg_type, self.node_id, (), intended_targets)
        await self.send_message_to_peers(message, ())
        await self.do_dolev_deliver(message)

        '''payload = DolevMessage(content, msg_sender_id, msg_type, sender_id, (), self.node_id, self.node_id)
        peers = self.get_peers()
        limited_peers = random.sample(peers, min(self.limited_neighbors, len(peers)))

        print(f"[Byzantine-Limited] Node {self.node_id}: Broadcasting to only {len(limited_peers)}/{len(peers)} neighbors")

        async def send_with_delay(peer):
            delay = random.uniform(self.min_message_delay, self.max_message_delay)
            await asyncio.sleep(delay)
            self.ez_send(peer, payload)

        tasks = [asyncio.create_task(send_with_delay(peer)) for peer in limited_peers]
        await asyncio.gather(*tasks)

        await self.do_dolev_deliver(payload)'''  

    @message_wrapper(DolevMessage)
    async def on_ipv8_message(self, peer: Peer, message: DolevMessage) -> None:
        #if self.byzantine_behavior != 'none':
        #    await self.do_byzantine(peer, message)
        #    return

        real_sender_id = self.node_id_from_peer(peer)
        key = message.dolev_key

        if real_sender_id == message.dolev_sender_id: # Optimization 1
            if not key in self.delivered:
                await self.do_dolev_deliver(message)

        if real_sender_id == message.dolev_sender_id and message.msg_type == "SEND": # Bracha optimization 2: Don't have to forward SEND messages
            return
        
        if len(message.path) == 0: # Optimization 3
            if not key in self.who_delivered:
                self.who_delivered[key] = set()
            self.who_delivered[key].add(real_sender_id)

        path = tuple(message.path) + (real_sender_id,)
        self.paths[key].add(path)

        num_disjoint_paths = count_disjoint_paths(self.paths[key]) if key in self.paths else 0

        if (num_disjoint_paths >= self.f + 1 and key not in self.delivered):
            await self.do_dolev_deliver(message)
        
        peers = self.get_peers()
        peers_to_send = [peer for peer in peers if self.node_id_from_peer(peer) not in self.who_delivered[key]] # Optimization 3

        if any([node_id in message.path for node_id in self.who_delivered[key]]): # Optimization 4
            return
        
        if not key in self.delivered:
            await self.send_message_to_peers(message, path, peers_to_send)
        elif not key in self.empty_forwarded: # Optimization 5
            await self.send_message_to_peers(message, (), peers_to_send) # Optimization 2
            self.empty_forwarded.add(key)
    
    async def do_dolev_deliver(self, message: DolevMessage) -> None:
        if message.msg_type == "SEND" and message.dolev_sender_id != message.broadcast_sender_id and self.byzantine_behavior == "none":
            return # SEND message with forged broadcast sender id

        if message.dolev_key in self.delivered:
            return # This message has already been Dolev-delivered
        
        self.delivered.add(message.dolev_key)

        if len(message.intended_targets) and self.node_id not in message.intended_targets:
            return # For limited broadcasts: Don't actually Dolev deliver this message if not intended for us

        
        print(f"[Dolev] Node {self.node_id}: Message is delivered from sender {message.dolev_sender_id} [{message.broadcast_sender_id}: \"{message.content}\"] (type: {message.msg_type})")

        key = message.bracha_key
        #if message.msg_type == "SEND":
        if message.msg_type == "SEND" or (message.msg_type == "ECHO" and message.broadcast_sender_id == message.dolev_sender_id): # Optimization 2: ECHO from source implies SEND
            if not key in self.sent_echo:
                self.sent_echo.add(key)
                await self.do_dolev_broadcast_if_close(message.content, message.broadcast_sender_id, "ECHO")

        elif message.msg_type == "ECHO":
            self.echos[key].add(message.dolev_sender_id)
            if len(self.echos[key]) >= math.ceil((self.num_nodes + self.f + 1) / 2) and not key in self.sent_ready:
                self.sent_ready.add(key)
                await self.do_dolev_broadcast_if_close(message.content, message.broadcast_sender_id, "READY")

            if len(self.echos[key]) >= self.f+1 and key not in self.sent_echo: # Optimization 1
                self.sent_echo.add(key)
                await self.do_dolev_broadcast_if_close(message.content, message.broadcast_sender_id, "ECHO")

        elif message.msg_type == "READY":
            self.readys[key].add(message.dolev_sender_id)
            if len(self.readys[key]) >= self.f+1 and not key in self.sent_ready:
                self.sent_ready.add(key)
                await self.do_dolev_broadcast_if_close(message.content, message.broadcast_sender_id, "READY")

            if len(self.readys[key]) >= 2*self.f+1 and not key in self.bracha_delivered:
                self.do_bracha_deliver(message)

        
        if message.msg_type == "READY": # Optimization 1
            echo_message = DolevMessage(message.content, message.broadcast_sender_id, "ECHO", message.dolev_sender_id, message.path, message.intended_targets)
            if echo_message.dolev_key not in self.delivered: # READY message is Dolev-delivered but ECHO not yet. Deliver the ECHO now as well
                await self.do_dolev_deliver(echo_message)

    def do_bracha_deliver(self, payload: DolevMessage) -> None:
        self.bracha_delivered.add(payload.bracha_key)
        print(f"[Bracha] Node {self.node_id}: Broadcast is delivered [{payload.broadcast_sender_id}: \"{payload.content}\"]")


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