"""Generate API usage events: API calls with token counts, latency, errors."""

import random
import uuid

from src.generators.base import BaseGenerator

ENDPOINTS = [
    "/v1/chat/completions",
    "/v1/completions",
    "/v1/embeddings",
    "/v1/images/generations",
    "/v1/audio/transcriptions",
    "/v1/moderations",
]
ENDPOINT_WEIGHTS = [0.50, 0.10, 0.20, 0.08, 0.07, 0.05]

HTTP_STATUS_CODES = [200, 200, 200, 200, 200, 400, 401, 429, 500, 503]


class APIUsageEventGenerator(BaseGenerator):
    def __init__(self):
        super().__init__(topic="api_usage_events")

    def generate(self, user_id: str, user_profile: dict) -> list[dict]:
        # API users are mostly enterprise/plus
        if user_profile["tier"] == "free" and random.random() < 0.9:
            return []

        endpoint = random.choices(ENDPOINTS, weights=ENDPOINT_WEIGHTS, k=1)[0]
        model = random.choice(["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"])
        status_code = random.choice(HTTP_STATUS_CODES)

        input_tokens = random.randint(50, 8000)
        output_tokens = random.randint(100, 4000) if status_code == 200 else 0
        latency_ms = (
            int(random.lognormvariate(6.0, 0.7)) if status_code == 200
            else random.randint(10, 200)
        )

        # Simulate rate limiting for high-volume users
        is_rate_limited = status_code == 429

        return [self._make_event("api_call", user_id, {
            "api_key_hash": f"sk_{uuid.uuid4().hex[:12]}",
            "endpoint": endpoint,
            "model_id": model,
            "http_status_code": status_code,
            "input_tokens": input_tokens if status_code == 200 else 0,
            "output_tokens": output_tokens,
            "total_tokens": (input_tokens + output_tokens) if status_code == 200 else 0,
            "latency_ms": min(latency_ms, 120000),
            "is_streaming": random.random() < 0.6,
            "organization_id": user_profile.get("organization_id"),
            "is_rate_limited": is_rate_limited,
            "request_id": f"req_{uuid.uuid4().hex[:16]}",
        })]
