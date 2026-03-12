"""Abstract base class for all event generators."""

import uuid
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any


class BaseGenerator(ABC):
    """Base class for synthetic event generators.

    All generators produce events with a consistent envelope:
    {
        "event_id": "uuid",
        "event_type": "string",
        "timestamp": "ISO-8601",
        "user_id": "string",
        "payload": { ... event-specific fields ... }
    }
    """

    def __init__(self, topic: str):
        self.topic = topic

    def _make_event(self, event_type: str, user_id: str, payload: dict[str, Any]) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "payload": payload,
        }

    def serialize(self, event: dict) -> bytes:
        return json.dumps(event, default=str).encode("utf-8")

    @abstractmethod
    def generate(self, user_id: str, user_profile: dict) -> list[dict]:
        """Generate one or more events for a given user. Returns a list of event dicts."""
        ...
