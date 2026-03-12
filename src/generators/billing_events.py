"""Generate billing events: subscriptions, payments, upgrades, refunds."""

import random
import uuid
from datetime import datetime, timezone, timedelta

from src.generators.base import BaseGenerator

PLAN_PRICES = {
    "free": 0.0,
    "plus": 20.0,
    "enterprise": 60.0,  # per seat
}


class BillingEventGenerator(BaseGenerator):
    def __init__(self):
        super().__init__(topic="billing_events")

    def generate(self, user_id: str, user_profile: dict) -> list[dict]:
        tier = user_profile["tier"]

        # Free users rarely generate billing events
        if tier == "free" and random.random() < 0.95:
            return []

        event_type = random.choices(
            ["subscription_created", "payment_processed", "upgrade", "downgrade", "refund"],
            weights=[0.15, 0.50, 0.15, 0.10, 0.10],
            k=1,
        )[0]

        now = datetime.now(timezone.utc)
        period_start = now.replace(day=1)
        period_end = (period_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        base_payload = {
            "transaction_id": f"txn_{uuid.uuid4().hex[:16]}",
            "plan_id": f"plan_{tier}",
            "currency": "USD",
            "billing_period_start": period_start.date().isoformat(),
            "billing_period_end": period_end.date().isoformat(),
        }

        if event_type == "subscription_created":
            base_payload.update({
                "amount_usd": PLAN_PRICES.get(tier, 20.0),
                "payment_method": random.choice(["credit_card", "paypal", "invoice"]),
                "is_trial": random.random() < 0.3,
            })
        elif event_type == "payment_processed":
            base_payload.update({
                "amount_usd": PLAN_PRICES.get(tier, 20.0),
                "payment_method": random.choice(["credit_card", "paypal", "invoice"]),
                "payment_status": random.choices(
                    ["succeeded", "failed", "pending"],
                    weights=[0.92, 0.05, 0.03],
                    k=1,
                )[0],
            })
        elif event_type == "upgrade":
            new_tier = "enterprise" if tier == "plus" else "plus"
            base_payload.update({
                "old_plan": f"plan_{tier}",
                "new_plan": f"plan_{new_tier}",
                "amount_usd": PLAN_PRICES.get(new_tier, 20.0),
                "prorated_amount_usd": round(random.uniform(5.0, 40.0), 2),
            })
        elif event_type == "downgrade":
            new_tier = "free" if tier == "plus" else "plus"
            base_payload.update({
                "old_plan": f"plan_{tier}",
                "new_plan": f"plan_{new_tier}",
                "effective_date": period_end.date().isoformat(),
                "reason": random.choice(["too_expensive", "not_enough_features", "switching_provider", "temporary"]),
            })
        elif event_type == "refund":
            base_payload.update({
                "amount_usd": round(random.uniform(5.0, PLAN_PRICES.get(tier, 20.0)), 2),
                "reason": random.choice(["billing_error", "unsatisfied", "duplicate_charge", "fraud"]),
                "original_transaction_id": f"txn_{uuid.uuid4().hex[:16]}",
            })

        return [self._make_event(event_type, user_id, base_payload)]
