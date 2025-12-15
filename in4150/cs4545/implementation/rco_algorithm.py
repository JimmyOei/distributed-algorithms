import json
from typing import Tuple
from cs4545.system.da_types import *
from cs4545.implementation.bracha_algorithm import BrachaAlgorithm, BrachaMessage

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
        return (self.sender_id, self.content)

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


class RCOAlgorithm(BrachaAlgorithm):
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)
        
        self.vector_clock: list[int] = [0] * self.num_nodes
        self.pending = set()   # Pending set of RCOMessages (sender_id, content, vector_clock)
        self.rco_delivered: Dict[Tuple[int, str], bool] = {}

    async def on_start(self) -> None:
        # Make sure to call this one last in this function
        await super().on_start()
        
        for i in range(self.num_messages_to_broadcast):
            message_content = f"Message-{i}"
            await self.rco_broadcast(message_content)

    async def brb_deliver(self, msg: BrachaMessage) -> None:
        """
        upon event ( rbDeliver | pi, [DATA, VCm, m] ) do
            if pi ≠ self then
                pending := pending U (pi, [DATA, VCm, m])
                deliver-pending
        """
        # Deserialize rco message
        try:
            rco_msg = RCOMessage.from_json(msg.content)
        except:
            # If not a Bracha message, call parent's brb_deliver
            await super().brb_deliver(msg)
            return

        # Byzantine Behavior: Message Dropping at RCO layer
        if self.byzantine_behavior == 'rco_drop_messages':
            if self.debug_mode >= 1:
                print(f"[BYZANTINE-RCO] Node {self.node_id}: Dropping message from {rco_msg.sender_id}: \"{rco_msg.content}\"")
            return  # Drop the message

        if rco_msg.sender_id != self.node_id and not self.rco_delivered.get(rco_msg.key, False):
            self.pending.add((rco_msg.sender_id, rco_msg.content, rco_msg.vector_clock))
            if self.debug_mode >= 2 and self.debug_algorithm in ['all', 'rco']:
                print(f"[RCO-RECEIVE] Node {self.node_id}: Received message from {rco_msg.sender_id}: \"{rco_msg.content}\" | VC_msg={rco_msg.vector_clock}, VC_local={self.vector_clock}")
            await self.deliver_pending()
            
    async def rco_broadcast(self, msg_content: str) -> None:
        """
        Upon event < rcoBroadcast | m > do
            1. trigger < rcoDeliver | self, m >
            2. trigger < rbBroadcast | [DATA, VC, m] >
            3. VC[rank(self)] := VC[rank(self)] + 1
        """
        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'rco']:
            print(f"[RCO-BROADCAST] Node {self.node_id}: Broadcasting message [{self.node_id}: \"{msg_content}\"]")
        await self.rco_deliver(self.node_id, msg_content)

        vector_clock_to_send = tuple(self.vector_clock)

        # Byzantine Behavior: Vector Clock Inflation
        # Send messages with inflated vector clocks to delay delivery at correct nodes
        if self.byzantine_behavior == 'vc_inflation':
            inflated_vc = [vc + 10 for vc in self.vector_clock]
            vector_clock_to_send = tuple(inflated_vc)
            if self.debug_mode >= 1:
                print(f"[BYZANTINE-RCO] Node {self.node_id}: Inflating VC from {self.vector_clock} to {inflated_vc}")

        # Byzantine Behavior: Vector Clock Deflation/Forgery
        # Send messages with zero or minimal vector clocks to bypass causal order
        elif self.byzantine_behavior == 'vc_deflation':
            deflated_vc = [0] * self.num_nodes
            vector_clock_to_send = tuple(deflated_vc)
            if self.debug_mode >= 1:
                print(f"[BYZANTINE-RCO] Node {self.node_id}: Deflating VC from {self.vector_clock} to {deflated_vc}")

        rco_msg = RCOMessage(
            sender_id=self.node_id,
            content=msg_content,
            vector_clock=vector_clock_to_send,
        )
        await self.brb_broadcast(rco_msg.to_json())

        self.vector_clock[self.node_id] += 1

    async def rco_deliver(self, msg_sender_id: int, msg_content: str) -> None:
        if self.debug_mode >= 1 and self.debug_algorithm in ['all', 'rco']:
            print(f"[RCO-DELIVER] Node {self.node_id}: Delivered message from sender {msg_sender_id}: \"{msg_content}\" | VC={self.vector_clock}")

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
                msg_sender_id, msg_content, msg_vector_clock = pending_item
                msg_vector_clock = list(msg_vector_clock)

                # Check if all entries in local VC are >= corresponding entries in message VC
                can_deliver = all(
                    self.vector_clock[i] >= msg_vector_clock[i]
                    for i in range(len(self.vector_clock))
                )

                if can_deliver:
                    self.pending.remove(pending_item)

                    await self.rco_deliver(msg_sender_id, msg_content)
                    self.rco_delivered[(msg_sender_id, msg_content)] = True

                    self.vector_clock[msg_sender_id] += 1

                    delivered_any = True
                    break