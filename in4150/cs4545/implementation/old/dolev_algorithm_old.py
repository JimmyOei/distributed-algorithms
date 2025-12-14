from typing import Tuple
from cs4545.system.da_types import *
import os
import random
import asyncio

@dataclass(msg_id=3)
class MyMessage:
    sender_id: int
    message: str
    path: Tuple[int]


class DolevAlgorithm(DistributedAlgorithm):
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)
        self.add_message_handler(MyMessage, self.on_message)

        self.f = int(os.getenv('FAULTS', '0'))
        self.min_message_delay = float(os.getenv('MIN_MESSAGE_DELAY', '0.01')) # default 10ms
        self.max_message_delay = float(os.getenv('MAX_MESSAGE_DELAY', '0.1'))  # default 100ms

        # Byzantine behavior can be 'none', 'drop', 'alter_path', 'alter_sender', 'send_empty'
        self.byzantine_behavior = os.getenv('BYZANTINE_BEHAVIOR', 'none')

        self.num_messages_to_broadcast = int(os.getenv('NUM_BROADCASTS', '1'))

        self.delivered = {} # Dict mapping (message, sender_id, message_id) to node_ids that delivered it
        self.paths = {} # Dict mapping (message, sender_id, message_id) to set of paths (tuples of node_ids)

    async def on_start(self):
        print(f"Node {self.node_id} initialized with f={self.f}, message delay [{self.min_message_delay}, {self.max_message_delay}], NUM_BROADCASTS={self.num_messages_to_broadcast}")
        # Make sure to call this one last in this function
        await super().on_start()

        if self.num_messages_to_broadcast > 0:
            await self.broadcast_messages()

    async def broadcast_messages(self):
        """Broadcast messages from this node"""
        print(f"[Node {self.node_id}] Broadcasting {self.num_messages_to_broadcast} message(s)")
        for msg in range(self.num_messages_to_broadcast):
            message = f"Message {msg} from node {self.node_id}"
            await self.send_message_to_peers(MyMessage(self.node_id, message, ()), ())
            self.do_deliver(message, self.node_id)
        
    async def send_message_to_peers(self, payload: MyMessage, path) -> None:
        async def send_with_delay(peer):
            delay = random.uniform(self.min_message_delay, self.max_message_delay)
            await asyncio.sleep(delay)
            self.ez_send(peer, MyMessage(payload.sender_id, payload.message, path))
            
        peers = self.get_peers()
        key = (payload.message, payload.sender_id)
        tasks = []
        for peer in peers:
            peer_id = self.node_id_from_peer(peer)
            if (peer_id in path) \
                or (key in self.delivered and peer_id in self.delivered[key]):
                continue
            tasks.append(asyncio.create_task(send_with_delay(peer)))
        
        await asyncio.gather(*tasks)
        
    async def do_byzantine(self, peer: Peer, payload: MyMessage) -> None:
        sender_id = self.node_id_from_peer(peer)
        if self.byzantine_behavior == 'drop':
            print(f"[Node {self.node_id}] Byzantine: Dropping message from node {sender_id}.")
            return
        elif self.byzantine_behavior == 'alter_path':
            print(f"[Node {self.node_id}] Byzantine: Altering path of message from node {sender_id}.")
            # Alter the path by adding a random amount of random node id
            altered_path = list(payload.path) + [random.randint(0, 10 - 1) for _ in range(random.randint(1,3))]
            payload.path = tuple(altered_path)
        elif self.byzantine_behavior == 'alter_sender':
            print(f"[Node {self.node_id}] Byzantine: Altering sender of message from node {sender_id}.")
            # Alter the sender id to a random node id
            payload.sender_id = random.randint(0, 10 - 1)
        elif self.byzantine_behavior == 'send_empty':
            # send altered message with empty path
            print(f"[Node {self.node_id}] Byzantine: Sending empty path message instead of message from node {sender_id}.")
            payload = MyMessage(payload.sender_id, "byzantine message", ())
            
        await self.send_message_to_peers(payload, payload.path) 

    @message_wrapper(MyMessage)
    async def on_message(self, peer: Peer, payload: MyMessage) -> None:
        try:
            if self.byzantine_behavior != 'none':
                await self.do_byzantine(peer, payload)
                return
            
            sender_id = self.node_id_from_peer(peer)
            key = (payload.message, payload.sender_id)
            print(
                f"[Node {self.node_id}] Got a message from node: {sender_id}.\t")

            if key in self.delivered and self.node_id in self.delivered[key]:
                return

            if payload.path == ():
                if key in self.delivered:
                    self.delivered[key].add(sender_id)
                else:
                    self.delivered[key] = set([sender_id])

            self.append_output(f"{sender_id}-{payload}")
            path = tuple(payload.path) + (sender_id,)

            nodes_that_delivered = self.delivered.get(key, set())
            path_contains_delivered_node = any(node in nodes_that_delivered for node in path)

            if path_contains_delivered_node:
                return
            
            if key not in self.paths:
                self.paths[key] = set()
            self.paths[key].add(path)
                
            num_disjoint_paths = count_disjoint_paths(self.paths[key]) if key in self.paths else 0
            if (num_disjoint_paths >= self.f + 1 and key not in self.delivered) \
                or (sender_id == payload.sender_id):
                self.do_deliver(payload.message, payload.sender_id)
                
                await self.send_message_to_peers(payload, tuple())
            else:
                await self.send_message_to_peers(payload, path)

        except Exception as e:
            print(f"Error in on_message: {e}")
            raise e
    
    def do_deliver(self, message, sender_id):
        print(f"Node {self.node_id}: Message is delivered from sender {sender_id} [\"{message}\"]")
        key = (message, sender_id)
        self.delivered[key] = set([self.node_id])

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