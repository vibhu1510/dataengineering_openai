"""Generate user lifecycle events: signups, logins, profile updates, tier changes."""

import random
from faker import Faker

from src.generators.base import BaseGenerator

fake = Faker()

COUNTRIES = ["US", "GB", "DE", "JP", "BR", "IN", "FR", "CA", "AU", "KR"]
COUNTRY_WEIGHTS = [0.35, 0.10, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.05, 0.08]


class UserEventGenerator(BaseGenerator):
    def __init__(self):
        super().__init__(topic="user_events")

    def generate_signup(self, user_id: str, user_profile: dict) -> dict:
        return self._make_event("user_signup", user_id, {
            "email_hash": fake.sha256()[:16],
            "display_name": fake.user_name(),
            "country_code": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0],
            "user_tier": user_profile["tier"],
            "platform": user_profile["platform"],
            "referral_source": random.choice([
                "organic", "google", "twitter", "friend_referral",
                "blog_post", "youtube", "product_hunt",
            ]),
            "organization_id": (
                f"org_{fake.lexify(text='????????')}" if user_profile["tier"] == "enterprise" else None
            ),
        })

    def generate_login(self, user_id: str, user_profile: dict) -> dict:
        return self._make_event("user_login", user_id, {
            "platform": user_profile["platform"],
            "country_code": user_profile.get("country_code", "US"),
            "ip_hash": fake.sha256()[:12],
            "user_agent_hash": fake.sha256()[:8],
            "auth_method": random.choice(["email", "google_sso", "microsoft_sso", "apple_sso"]),
        })

    def generate_tier_change(self, user_id: str, old_tier: str, new_tier: str) -> dict:
        return self._make_event("user_tier_change", user_id, {
            "old_tier": old_tier,
            "new_tier": new_tier,
            "reason": random.choice(["upgrade", "downgrade", "trial_expired", "enterprise_contract"]),
        })

    def generate(self, user_id: str, user_profile: dict) -> list[dict]:
        event_type = random.choices(
            ["login", "signup", "tier_change"],
            weights=[0.85, 0.10, 0.05],
            k=1,
        )[0]

        if event_type == "signup":
            return [self.generate_signup(user_id, user_profile)]
        elif event_type == "login":
            return [self.generate_login(user_id, user_profile)]
        else:
            tiers = ["free", "plus", "enterprise"]
            old_tier = user_profile["tier"]
            new_tier = random.choice([t for t in tiers if t != old_tier])
            return [self.generate_tier_change(user_id, old_tier, new_tier)]
