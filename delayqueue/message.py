"""Message model and retry policy."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class MessageState(Enum):
    PENDING = "pending"        # waiting for its delivery time
    IN_FLIGHT = "in_flight"    # leased to a worker, not yet acked
    CANCELLED = "cancelled"    # cancelled before delivery, terminal
    ACKED = "acked"            # consumed successfully, terminal
    DEAD = "dead"              # retries exhausted, terminal


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff between redeliveries.

    max_attempts counts delivery attempts (claims). A message is dead
    once it has been claimed max_attempts times without an ack.
    """

    max_attempts: int = 3
    initial_backoff: float = 1.0
    multiplier: float = 2.0
    max_backoff: float = 300.0

    def backoff_for(self, attempt: float) -> float:
        """Delay before the next redelivery after `attempt` attempts."""
        delay = self.initial_backoff * (self.multiplier ** max(0, attempt - 1))
        return min(self.max_backoff, delay)


@dataclass
class Message:
    """A delayed message.

    - delivery_time: absolute time (on the injected clock) at or after
      which the message may be delivered.
    - sequence: ordering position within `key`; due messages of the same
      key are delivered strictly in ascending sequence order.
    - key: ordering/identity group supplied by the caller.
    - cancelled: set via DelayQueue.cancel(); terminal once observed.
    - retry_policy: governs redelivery after nack or lease expiry.
    """

    message_id: str
    key: str
    sequence: int
    delivery_time: float
    payload: Any = None
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    cancelled: bool = False
    state: MessageState = MessageState.PENDING
    attempts: int = 0
    lease_expires_at: Optional[float] = None
