"""Generate safety/trust events: abuse signals, prompt injection attempts, content flags."""

import random
import uuid

from src.generators.base import BaseGenerator

VIOLATION_TYPES = [
    "prompt_injection",
    "jailbreak_attempt",
    "content_policy_violation",
    "rate_limit_abuse",
    "credential_stuffing",
    "automated_scraping",
    "harassment_content",
    "illegal_content_request",
]
VIOLATION_WEIGHTS = [0.20, 0.15, 0.20, 0.15, 0.08, 0.10, 0.07, 0.05]

SEVERITY_LEVELS = ["low", "medium", "high", "critical"]
SEVERITY_WEIGHTS = [0.40, 0.30, 0.20, 0.10]


class SafetyEventGenerator(BaseGenerator):
    def __init__(self):
        super().__init__(topic="safety_events")

    def generate(self, user_id: str, user_profile: dict) -> list[dict]:
        # ~5% of interactions trigger a safety event
        if random.random() < 0.95:
            return []

        violation_type = random.choices(VIOLATION_TYPES, weights=VIOLATION_WEIGHTS, k=1)[0]
        severity = random.choices(SEVERITY_LEVELS, weights=SEVERITY_WEIGHTS, k=1)[0]

        payload = {
            "violation_type": violation_type,
            "severity": severity,
            "confidence_score": round(random.uniform(0.3, 1.0), 3),
            "session_id": f"sess_{uuid.uuid4().hex[:16]}",
            "platform": user_profile["platform"],
            "detection_method": random.choice([
                "rule_based", "ml_classifier", "anomaly_detection", "user_report",
            ]),
            "action_taken": _determine_action(severity, violation_type),
            "user_violation_count_30d": random.choices(
                [1, 2, 3, 5, 10, 20],
                weights=[0.50, 0.20, 0.12, 0.08, 0.06, 0.04],
                k=1,
            )[0],
        }

        # Add violation-specific details
        if violation_type == "prompt_injection":
            payload["injection_pattern"] = random.choice([
                "ignore_instructions", "system_prompt_leak", "role_override",
                "delimiter_attack", "encoding_bypass",
            ])
        elif violation_type == "rate_limit_abuse":
            payload["requests_per_minute"] = random.randint(100, 5000)
            payload["normal_rpm_baseline"] = random.randint(10, 50)
        elif violation_type == "content_policy_violation":
            payload["content_category"] = random.choice([
                "violence", "sexual", "hate_speech", "self_harm",
                "illegal_activity", "deception",
            ])

        return [self._make_event("safety_signal", user_id, payload)]


def _determine_action(severity: str, violation_type: str) -> str:
    if severity == "critical":
        return "account_suspended"
    elif severity == "high":
        return random.choice(["account_quarantined", "rate_limited", "warning_issued"])
    elif severity == "medium":
        return random.choice(["rate_limited", "warning_issued", "content_filtered"])
    else:
        return random.choice(["content_filtered", "logged_only"])
