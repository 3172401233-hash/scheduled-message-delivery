# scheduled-message-delivery

Python 延迟消息投递核心库，处理定时队列、重试、取消和时钟漂移。

## 设计

- **时间注入**：队列内部不持有任何定时器或线程，也不读取系统时间。
  所有时间比较都通过注入的 `Clock`（生产用 `SystemClock`，测试用
  `ManualClock`）完成，调度由调用方以 `claim()` 驱动。
- **存储注入**：持久化是调用方的职责。任何实现 `Storage` 协议的后端
  （SQL、Redis、文件……）均可注入；库内置 `InMemoryStorage` 供测试和
  单进程使用。节点重启后只要存储还在，消息就不会丢。
- **消息模型**：`Message` 包含 `delivery_time`（绝对投递时间）、
  `sequence`（key 内序号）、`key`（有序分组）、`cancelled`（取消标记）
  和 `retry_policy`（指数退避重试策略）。

## 投递语义

- **不早于约定时间**：消息仅在注入时钟的 `now >= delivery_time` 后才
  可被领取。时钟回拨（如 NTP 校时）只会推迟投递，绝不会提前触发，
  也不会让已持有的租约意外过期。
- **同 key 严格有序**：同一 `key` 的消息按 `sequence` 升序逐条投递。
  前一条处于 in-flight（已领取未确认）状态时，后续消息不会被领取；
  前一条未到投递时间时，后续消息会被队头阻塞。取消某条消息后，下一
  序号自动成为新的队头。不同 key 之间完全独立。
- **至少一次投递**：`claim()` 返回的消息进入 in-flight 并携带租约
  （`now + lease_duration`）。worker 崩溃或忘记确认时，租约到期后消息
  按重试策略重新进入待投递状态，可被再次领取——因此同一消息可能被
  投递多次，消费方需自行幂等。
- **确认与取消**：`ack()` 后消息终止，永不重投；到期前调用 `cancel()`
  可阻止投递（取消标记置位，状态终态化）。
- **重试退避**：`nack()` 或租约过期计为一次失败，消息按
  `RetryPolicy`（`initial_backoff * multiplier^(n-1)`，封顶
  `max_backoff`）延迟重投；领取次数达到 `max_attempts` 后进入 DEAD
  死信状态，不再投递。

## 最大延迟精度

库本身没有后台定时器，**投递精度等于调用方调用 `claim()` 的轮询间隔**
（以及注入时钟的分辨率）。消息保证不早于 `delivery_time` 投递；实际
投递时刻为 `delivery_time` 之后第一次 `claim()` 调用，最大额外延迟为
一个轮询周期。例如每 100ms 轮询一次，则最大延迟误差为 100ms（时钟回拨
等异常会进一步推迟，但绝不提前）。

## 使用示例

```python
from delayqueue import DelayQueue, InMemoryStorage, ManualClock, Message, RetryPolicy

clock = ManualClock(start=0.0)
queue = DelayQueue(InMemoryStorage(), clock)

queue.enqueue(Message(
    message_id="order-1-timeout",
    key="order-1",
    sequence=1,
    delivery_time=1800.0,          # 30 分钟后关闭未支付订单
    payload={"action": "cancel_unpaid"},
    retry_policy=RetryPolicy(max_attempts=5, initial_backoff=10.0),
))

clock.advance(1800.0)              # 生产中由 SystemClock 自然推进
for msg in queue.claim(lease_duration=60.0):
    try:
        handle(msg.payload)
        queue.ack(msg.message_id)
    except Exception:
        queue.nack(msg.message_id)  # 按退避策略重投
```

## 测试

```
python -m unittest
```

测试覆盖按时投递、提前取消、时钟回拨、重复领取（租约/崩溃重投）和
重试退避。时间推进全部通过 `ManualClock` 完成，不使用 `sleep`。
