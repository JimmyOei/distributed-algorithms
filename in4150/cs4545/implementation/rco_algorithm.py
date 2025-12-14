import json
from typing import Tuple
from cs4545.system.da_types import *
import os
import random
import asyncio
from collections import defaultdict
import math

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
            content=obj['content'],
            msg_type=obj['msg_type'],
            sender_id=obj['sender_id']
        )

@dataclass(msg_id=6)
class RCOMessage:
    """RCO layer message - for Reliable Causal Order broadcast"""
    sender_id: int
    content: str
    vector_clock: Tuple[int, ...]
    
    def __init__(self, sender_id: int, content: str, vector_clock: Tuple[int, ...]):
        self.sender_id = sender_id
        self.content = content
        self.vector_clock = vector_clock
    
    @property
    def key(self):
        return (self.sender_id, self.content, self.vector_clock)

    def to_json(self) -> str:
        """Serialize for passing to Bracha layer"""
        return json.dumps({
            'content': self.content,
            'vector_clock': list(self.vector_clock),
            'sender_id': self.sender_id
        })

    @staticmethod
    def from_json(json_str: str) -> 'RCOMessage':
        """Deserialize from Bracha layer"""
        obj = json.loads(json_str)
        return RCOMessage(
            content=obj['content'],
            vector_clock=tuple(obj['vector_clock']),
            sender_id=obj['sender_id']
        )


class RCOAlgorithm(DistributedAlgorithm):
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)
        self.add_message_handler(DolevMessage, self.on_ipv8_message)

        self.f = int(os.getenv('FAULTS', '0'))
        self.min_message_delay = float(os.getenv('MIN_MESSAGE_DELAY', '0.01')) # default 10ms
        self.max_message_delay = float(os.getenv('MAX_MESSAGE_DELAY', '0.1'))  # default 100ms
        self.num_nodes = int(os.getenv('NUM_NODES', '1'))
        self.num_messages_to_broadcast = int(os.getenv('NUM_BROADCASTS', '1'))

        self.byzantine_behavior = os.getenv('BYZANTINE_BEHAVIOR', 'none')
        self.limited_neighbors = int(os.getenv('LIMITED_NEIGHBORS', '1'))  # For limited_broadcast behavior


        self.dolev_delivered = {} # Dict mapping (message, sender_id, message_id) to node_ids that delivered it
        self.dolev_paths = {} # Dict mapping (message, sender_id, message_id) to set of paths (tuples of node_ids)

        self.sent_echo = set()
        self.sent_ready = set()
        self.bracha_delivered = set()
        self.echos = defaultdict(set)
        self.readys = defaultdict(set)

        # RCO configurations
        self.vector_clock = [0] * self.num_nodes
        self.pending = set()  # Pending set of RCOMMessage.keys {(sender_id, content, vector_clock)}
        self.rco_delivered = set()  # Delivered set of RCOMessage.keys

    async def on_start(self) -> None:
        # Make sure to call this one last in this function
        await super().on_start()
        
    async def rb_broadcast(self, rco_msg: RCOMessage) -> None:
        """Reliable Broadcast: Placeholder for Bracha broadcast"""
        pass

    async def rb_deliver(self, rco_msg: RCOMessage) -> None:
        """
        upon event ( rbDeliver | pi, [DATA, VCm, m] ) do
            if pi ≠ self then
                pending := pending U (pi, [DATA, VCm, m])
                deliver-pending
        """
        if rco_msg.sender_id != self.node_id:
            self.pending.add(rco_msg.key)
            print(f"[RB] Node {self.node_id}: Received from {rco_msg.sender_id}, added to pending | VC_msg={rco_msg.vector_clock}, VC_local={self.vector_clock}")
            await self.deliver_pending()
            
    async def rco_broadcast(self, msg_content: str) -> None:
        """
        Upon event < rcoBroadcast | m > do
            1. trigger < rcoDeliver | self, m >
            2. trigger < rbBroadcast | [DATA, VC, m] >
            3. VC[rank(self)] := VC[rank(self)] + 1
        """
        print(f"[RCO] Node {self.node_id}: Broadcasting message [{self.node_id}: \"{msg_content}\"]")
        await self.rco_deliver(self.node_id, msg_content)

        rco_msg = RCOMessage(
            sender_id=self.node_id,
            content=msg_content,
            vector_clock=tuple(self.vector_clock),
        )
        await self.rb_broadcast(rco_msg)

        self.vector_clock[self.node_id] += 1

    async def rco_deliver(self, msg_sender_id: int, msg_content: str) -> None:
        # This commented out code *might* not be needed, but want to test it first
        # msg_key = (msg_sender_id, msg_content)

        # # Check for no-duplication property
        # if msg_key in self.rco_delivered:
        #     return

        # self.rco_delivered.add(msg_key)
        print(f"[RCO] Node {self.node_id}: Delivered message from sender {msg_sender_id}: \"{msg_content}\" | VC={self.vector_clock}")

    async def deliver_pending(self) -> None:
        """
        Deliver-Pending Procedure: Checks if any pending messages can be delivered.
        
        while exists (sx, [DATA, VCx, x]) ∈ pending such that
            ∀pj : VC[rank(pj)] ≥ VCx[rank(pj)] do
                pending := pending \ {(sx, [DATA, VCx, x])}
                trigger ( rcoDeliver | sx, x )
                VC[rank(sx)] := VC[rank(sx)] + 1
        """
        delivered_any = True

        # keep trying to deliver, because delivering a message increases our VC
        # which might allow other messages to be delivered as well
        while delivered_any:
            delivered_any = False

            # Find a message that can be delivered
            for pending_item in list(self.pending):
                msg_sender_id, msg_content, msg_vector_clock  = pending_item
                msg_vector_clock = list(msg_vector_clock)

                # Check if all entries in local VC are >= corresponding entries in message VC
                can_deliver = all(
                    self.vector_clock[i] >= msg_vector_clock[i]
                    for i in range(len(self.vector_clock))
                )

                if can_deliver:
                    self.pending.remove(pending_item)

                    await self.rco_deliver(msg_sender_id, msg_content)

                    self.vector_clock[msg_sender_id] += 1

                    delivered_any = True
                    break