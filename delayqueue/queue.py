"""DelayQueue: scheduling core.

The queue holds no timers and no threads. Callers drive it by invoking
claim(); all time comparisons use the injected clock, so a clock
rollback can only delay delivery, never trigger it early.
"""

from typing import List, Optional

from .clock import Clock
from .message import Message, MessageState
from .storage import Storage

_TERMINAL = (MessageState.CANCELLED, MessageState.ACKED, MessageState.DEAD)


class DelayQueue:
    def __init__(self, storage: Storage, clock: Clock) -> None:
        self._storage = storage
        self._clock = clock

    def enqueue(self, message: Message) -> str:
        """Schedule a message for delivery at message.delivery_time."""
        message.state = MessageState.PENDING
        self._storage.put(message)
        return message.message_id

    def cancel(self, message_id: str) -> bool:
        """Cancel a message before it is acked.

        Returns True if the message was cancelled, False if it does not
        exist or is already in a terminal state.
        """
        msg = self._storage.get(message_id)
        if msg is None or msg.state in _TERMINAL:
            return False
        msg.cancelled = True
        msg.state = MessageState.CANCELLED
        msg.lease_expires_at = None
        return True

    def claim(self, lease_duration: float, max_messages: Optional[int] = None) -> List[Message]:
        """Lease due messages to the calling worker.

        A message is eligible when it is PENDING and
        delivery_time <= now. Per key, only the lowest-sequence
        non-terminal message is considered, and a key is skipped
        entirely while one of its messages is in flight: due messages
        of the same key are therefore delivered in sequence order.

        Each claimed message becomes IN_FLIGHT with a lease expiring at
        now + lease_duration. If the worker crashes, the lease lapses
        and the message is redelivered (subject to its retry policy).
        """
        now = self._clock.now()
        self._expire_leases(now)

        heads = {}
        for msg in self._storage.all():
            if msg.state in _TERMINAL:
                continue
            head = heads.get(msg.key)
            if head is None or msg.sequence < head.sequence:
                heads[msg.key] = msg

        claimed: List[Message] = []
        for head in sorted(heads.values(), key=lambda m: (m.delivery_time, m.key, m.sequence)):
            if max_messages is not None and len(claimed) >= max_messages:
                break
            if head.state != MessageState.PENDING or head.delivery_time > now:
                continue
            head.state = MessageState.IN_FLIGHT
            head.attempts += 1
            head.lease_expires_at = now + lease_duration
            claimed.append(head)
        return claimed

    def ack(self, message_id: str) -> bool:
        """Acknowledge successful processing of a leased message."""
        msg = self._storage.get(message_id)
        if msg is None or msg.state != MessageState.IN_FLIGHT:
            return False
        msg.state = MessageState.ACKED
        msg.lease_expires_at = None
        return True

    def nack(self, message_id: str) -> bool:
        """Report failure for a leased message.

        The message is rescheduled with backoff per its retry policy,
        or moved to DEAD when attempts are exhausted.
        """
        msg = self._storage.get(message_id)
        if msg is None or msg.state != MessageState.IN_FLIGHT:
            return False
        self._reschedule(msg, self._clock.now())
        return True

    def _expire_leases(self, now: float) -> None:
        for msg in self._storage.all():
            if (
                msg.state == MessageState.IN_FLIGHT
                and msg.lease_expires_at is not None
                and msg.lease_expires_at <= now
            ):
                self._reschedule(msg, now)

    def _reschedule(self, msg: Message, now: float) -> None:
        msg.lease_expires_at = None
        if msg.attempts >= msg.retry_policy.max_attempts:
            msg.state = MessageState.DEAD
        else:
            msg.state = MessageState.PENDING
            msg.delivery_time = now + msg.retry_policy.backoff_for(msg.attempts)
