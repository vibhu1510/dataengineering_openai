"""Generate conversation events: session start, messages, ratings, session end."""

import random
import uuid

from src.generators.base import BaseGenerator
from src.config.settings import generator_config

MODEL_TIER_MAP = {
    "free": ["gpt-4o-mini", "gpt-3.5-turbo"],
    "plus": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "o1-mini"],
    "enterprise": ["gpt-4o", "gpt-4-turbo", "o1-preview", "o1-mini"],
}

SAMPLE_TOPICS = [
    "coding", "writing", "math", "science", "creative", "business",
    "education", "translation", "analysis", "brainstorming",
]


class ConversationEventGenerator(BaseGenerator):
    def __init__(self):
        super().__init__(topic="conversation_events")

    def generate(self, user_id: str, user_profile: dict) -> list[dict]:
        tier = user_profile["tier"]
        available_models = MODEL_TIER_MAP.get(tier, MODEL_TIER_MAP["free"])
        model = random.choice(available_models)
        session_id = f"sess_{uuid.uuid4().hex[:16]}"
        conversation_id = f"conv_{uuid.uuid4().hex[:16]}"
        platform = user_profile["platform"]

        events = []

        # Session start
        events.append(self._make_event("session_start", user_id, {
            "session_id": session_id,
            "conversation_id": conversation_id,
            "model_id": model,
            "platform": platform,
            "topic_hint": random.choice(SAMPLE_TOPICS),
        }))

        # Generate 1-15 message exchanges per session
        num_exchanges = random.choices(
            range(1, 16),
            weights=[15, 12, 10, 9, 8, 7, 6, 5, 4, 3, 3, 3, 2, 2, 1],
            k=1,
        )[0]

        for turn in range(num_exchanges):
            input_tokens = random.randint(10, 2000)
            output_tokens = random.randint(50, 4000)
            latency_ms = int(random.lognormvariate(6.5, 0.8))  # median ~660ms

            events.append(self._make_event("message_sent", user_id, {
                "session_id": session_id,
                "conversation_id": conversation_id,
                "model_id": model,
                "message_role": "user",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
                "latency_ms": min(latency_ms, 60000),
                "turn_number": turn + 1,
                "platform": platform,
                "is_streaming": random.random() < 0.85,
                "has_code_block": random.random() < 0.3,
                "has_image": random.random() < 0.05,
            }))

        # Rating (30% of sessions get rated)
        if random.random() < 0.3:
            events.append(self._make_event("conversation_rated", user_id, {
                "session_id": session_id,
                "conversation_id": conversation_id,
                "rating": random.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 30, 50], k=1)[0],
                "feedback_text_length": random.randint(0, 500) if random.random() < 0.2 else 0,
            }))

        # Session end
        events.append(self._make_event("session_end", user_id, {
            "session_id": session_id,
            "conversation_id": conversation_id,
            "total_turns": num_exchanges,
            "session_duration_seconds": random.randint(30, 7200),
        }))

        return events
