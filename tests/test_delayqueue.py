import unittest

from delayqueue import (
    DelayQueue,
    InMemoryStorage,
    ManualClock,
    Message,
    MessageState,
    RetryPolicy,
)

LEASE = 30.0


def make_message(mid, key="k", seq=1, delivery_time=100.0, **kwargs):
    return Message(
        message_id=mid,
        key=key,
        sequence=seq,
        delivery_time=delivery_time,
        **kwargs,
    )


class DelayQueueTestCase(unittest.TestCase):
    def setUp(self):
        self.clock = ManualClock(start=0.0)
        self.storage = InMemoryStorage()
        self.queue = DelayQueue(self.storage, self.clock)

    def claim_ids(self, lease_duration=LEASE, max_messages=None):
        return [m.message_id for m in self.queue.claim(lease_duration, max_messages)]


class TestOnTimeDelivery(DelayQueueTestCase):
    def test_not_delivered_before_delivery_time(self):
        self.queue.enqueue(make_message("m1", delivery_time=100.0))
        self.clock.set(99.9)
        self.assertEqual(self.claim_ids(), [])

    def test_delivered_at_delivery_time(self):
        self.queue.enqueue(make_message("m1", delivery_time=100.0))
        self.clock.set(100.0)
        self.assertEqual(self.claim_ids(), ["m1"])

    def test_delivered_after_delivery_time(self):
        self.queue.enqueue(make_message("m1", delivery_time=100.0))
        self.clock.set(1_000_000.0)
        self.assertEqual(self.claim_ids(), ["m1"])

    def test_due_messages_of_same_key_arrive_in_sequence_order(self):
        for seq in (3, 1, 2):
            self.queue.enqueue(make_message(f"m{seq}", key="order", seq=seq, delivery_time=10.0))
        self.clock.set(10.0)
        self.assertEqual(self.claim_ids(), ["m1"])
        self.queue.ack("m1")
        self.assertEqual(self.claim_ids(), ["m2"])
        self.queue.ack("m2")
        self.assertEqual(self.claim_ids(), ["m3"])

    def test_later_sequence_waits_for_undue_head_of_same_key(self):
        self.queue.enqueue(make_message("head", key="k", seq=1, delivery_time=500.0))
        self.queue.enqueue(make_message("tail", key="k", seq=2, delivery_time=10.0))
        self.clock.set(100.0)
        self.assertEqual(self.claim_ids(), [])
        self.clock.set(500.0)
        self.assertEqual(self.claim_ids(), ["head"])
        self.queue.ack("head")
        self.assertEqual(self.claim_ids(), ["tail"])

    def test_keys_are_independent(self):
        self.queue.enqueue(make_message("a1", key="a", seq=1, delivery_time=10.0))
        self.queue.enqueue(make_message("b1", key="b", seq=1, delivery_time=10.0))
        self.clock.set(10.0)
        self.assertEqual(sorted(self.claim_ids()), ["a1", "b1"])


class TestCancel(DelayQueueTestCase):
    def test_cancel_before_due_prevents_delivery(self):
        self.queue.enqueue(make_message("m1", delivery_time=100.0))
        self.assertTrue(self.queue.cancel("m1"))
        self.clock.set(200.0)
        self.assertEqual(self.claim_ids(), [])
        self.assertEqual(self.storage.get("m1").state, MessageState.CANCELLED)

    def test_cancelled_message_is_marked(self):
        msg = make_message("m1", delivery_time=100.0)
        self.queue.enqueue(msg)
        self.queue.cancel("m1")
        self.assertTrue(msg.cancelled)

    def test_cancel_of_terminal_or_missing_message_fails(self):
        self.assertFalse(self.queue.cancel("nope"))
        self.queue.enqueue(make_message("m1", delivery_time=100.0))
        self.queue.cancel("m1")
        self.assertFalse(self.queue.cancel("m1"))

    def test_cancel_unblocks_next_sequence_of_same_key(self):
        self.queue.enqueue(make_message("m1", key="k", seq=1, delivery_time=100.0))
        self.queue.enqueue(make_message("m2", key="k", seq=2, delivery_time=100.0))
        self.queue.cancel("m1")
        self.clock.set(100.0)
        self.assertEqual(self.claim_ids(), ["m2"])


class TestClockRollback(DelayQueueTestCase):
    def test_rollback_never_triggers_early_delivery(self):
        self.queue.enqueue(make_message("m1", delivery_time=100.0))
        self.clock.set(150.0)
        self.clock.set(50.0)  # NTP step backwards
        self.assertEqual(self.claim_ids(), [])
        self.clock.set(100.0)
        self.assertEqual(self.claim_ids(), ["m1"])

    def test_rollback_does_not_expire_leases(self):
        self.queue.enqueue(make_message("m1", delivery_time=10.0))
        self.clock.set(10.0)
        self.assertEqual(self.claim_ids(lease_duration=30.0), ["m1"])
        self.clock.set(5.0)  # jump backwards past the lease window
        self.assertEqual(self.claim_ids(lease_duration=30.0), [])
        self.clock.set(41.0)  # lease (expires at 40) lapsed; backoff 1s applies
        self.assertEqual(self.claim_ids(lease_duration=30.0), [])
        self.clock.set(42.0)
        self.assertEqual(self.claim_ids(lease_duration=30.0), ["m1"])

    def test_rollback_before_any_schedule_is_safe(self):
        self.clock.set(-1000.0)
        self.queue.enqueue(make_message("m1", delivery_time=0.0))
        self.assertEqual(self.claim_ids(), [])
        self.clock.set(0.0)
        self.assertEqual(self.claim_ids(), ["m1"])


class TestDuplicateClaim(DelayQueueTestCase):
    def test_in_flight_message_is_not_claimed_twice(self):
        self.queue.enqueue(make_message("m1", delivery_time=10.0))
        self.clock.set(10.0)
        self.assertEqual(self.claim_ids(), ["m1"])
        self.assertEqual(self.claim_ids(), [])

    def test_second_message_of_same_key_blocked_while_first_in_flight(self):
        self.queue.enqueue(make_message("m1", key="k", seq=1, delivery_time=10.0))
        self.queue.enqueue(make_message("m2", key="k", seq=2, delivery_time=10.0))
        self.clock.set(10.0)
        self.assertEqual(self.claim_ids(), ["m1"])
        self.assertEqual(self.claim_ids(), [])

    def test_lease_expiry_redelivers_after_crash(self):
        self.queue.enqueue(make_message("m1", delivery_time=10.0))
        self.clock.set(10.0)
        first = self.queue.claim(lease_duration=30.0)
        self.assertEqual([m.message_id for m in first], ["m1"])
        # worker "crashes": no ack, lease lapses
        self.clock.set(41.0)  # lease (expires at 40) lapsed; default backoff 1s
        self.assertEqual(self.queue.claim(lease_duration=30.0), [])
        self.clock.set(42.0)
        second = self.queue.claim(lease_duration=30.0)
        self.assertEqual([m.message_id for m in second], ["m1"])
        self.assertEqual(second[0].attempts, 2)

    def test_acked_message_is_never_redelivered(self):
        self.queue.enqueue(make_message("m1", delivery_time=10.0))
        self.clock.set(10.0)
        self.queue.claim(lease_duration=30.0)
        self.assertTrue(self.queue.ack("m1"))
        self.clock.set(10_000.0)
        self.assertEqual(self.claim_ids(), [])

    def test_ack_requires_in_flight_state(self):
        self.queue.enqueue(make_message("m1", delivery_time=10.0))
        self.assertFalse(self.queue.ack("m1"))


class TestRetryBackoff(DelayQueueTestCase):
    def enqueue_with_policy(self, **policy_kwargs):
        policy = RetryPolicy(**policy_kwargs)
        self.queue.enqueue(make_message("m1", delivery_time=0.0, retry_policy=policy))
        return policy

    def test_nack_reschedules_with_backoff(self):
        self.enqueue_with_policy(max_attempts=5, initial_backoff=10.0, multiplier=2.0)
        self.queue.claim(lease_duration=30.0)
        self.assertTrue(self.queue.nack("m1"))
        # backoff after attempt 1 is 10s: not claimable at t=9.9
        self.clock.set(9.9)
        self.assertEqual(self.claim_ids(), [])
        self.clock.set(10.0)
        self.assertEqual(self.claim_ids(), ["m1"])

    def test_backoff_grows_exponentially_and_caps(self):
        policy = self.enqueue_with_policy(
            max_attempts=10, initial_backoff=1.0, multiplier=2.0, max_backoff=8.0
        )
        expected = [1.0, 2.0, 4.0, 8.0, 8.0]
        now = 0.0
        for delay in expected:
            self.clock.set(now)
            self.assertEqual(self.claim_ids(), ["m1"])
            self.queue.nack("m1")
            msg = self.storage.get("m1")
            self.assertAlmostEqual(msg.delivery_time, now + policy.backoff_for(msg.attempts))
            self.assertAlmostEqual(msg.delivery_time, now + delay)
            now += delay

    def test_lease_expiry_counts_as_failed_attempt(self):
        self.enqueue_with_policy(max_attempts=5, initial_backoff=10.0)
        self.clock.set(0.0)
        self.queue.claim(lease_duration=5.0)
        self.clock.set(6.0)  # lease lapsed; backoff 10s from expiry check
        self.assertEqual(self.claim_ids(), [])
        self.clock.set(16.0)
        self.assertEqual(self.claim_ids(), ["m1"])

    def test_message_dies_after_max_attempts(self):
        self.enqueue_with_policy(max_attempts=2, initial_backoff=1.0)
        self.clock.set(0.0)
        self.queue.claim(lease_duration=1.0)
        self.queue.nack("m1")  # attempt 1 failed
        self.clock.set(1.0)
        self.assertEqual(self.claim_ids(), ["m1"])  # attempt 2
        self.queue.nack("m1")
        self.assertEqual(self.storage.get("m1").state, MessageState.DEAD)
        self.clock.set(10_000.0)
        self.assertEqual(self.claim_ids(), [])

    def test_nack_requires_in_flight_state(self):
        self.enqueue_with_policy(max_attempts=3)
        self.assertFalse(self.queue.nack("m1"))


if __name__ == "__main__":
    unittest.main()
