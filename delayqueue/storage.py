"""Storage abstraction. Persistence is the caller's concern: any backend
satisfying the Storage protocol can be injected (SQL, Redis, files...).
InMemoryStorage is provided for tests and single-process use."""

from abc import ABC, abstractmethod
from typing import Dict, Iterable, Optional

from .message import Message


class Storage(ABC):
    @abstractmethod
    def put(self, message: Message) -> None: ...

    @abstractmethod
    def get(self, message_id: str) -> Optional[Message]: ...

    @abstractmethod
    def delete(self, message_id: str) -> None: ...

    @abstractmethod
    def all(self) -> Iterable[Message]: ...


class InMemoryStorage(Storage):
    def __init__(self) -> None:
        self._messages: Dict[str, Message] = {}

    def put(self, message: Message) -> None:
        self._messages[message.message_id] = message

    def get(self, message_id: str) -> Optional[Message]:
        return self._messages.get(message_id)

    def delete(self, message_id: str) -> None:
        self._messages.pop(message_id, None)

    def all(self) -> Iterable[Message]:
        return list(self._messages.values())
