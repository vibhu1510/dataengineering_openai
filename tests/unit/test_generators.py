"""Tests for synthetic data generators."""

from src.generators.user_events import UserEventGenerator
from src.generators.conversation_events import ConversationEventGenerator
from src.generators.api_usage_events import APIUsageEventGenerator
from src.generators.billing_events import BillingEventGenerator
from src.generators.safety_events import SafetyEventGenerator


def _make_user_profile(tier="free", platform="web"):
    return {
        "tier": tier,
        "platform": platform,
        "country_code": "US",
        "organization_id": "org_test" if tier == "enterprise" else None,
    }


class TestUserEventGenerator:
    def test_generates_events(self):
        gen = UserEventGenerator()
        events = gen.generate("usr_test001", _make_user_profile())
        assert len(events) >= 1
        assert events[0]["user_id"] == "usr_test001"
        assert "event_id" in events[0]
        assert "timestamp" in events[0]

    def test_signup_has_required_fields(self):
        gen = UserEventGenerator()
        event = gen.generate_signup("usr_test001", _make_user_profile())
        assert event["event_type"] == "user_signup"
        assert "country_code" in event["payload"]
        assert "platform" in event["payload"]


class TestConversationEventGenerator:
    def test_generates_session_with_messages(self):
        gen = ConversationEventGenerator()
        events = gen.generate("usr_test001", _make_user_profile("plus"))
        assert len(events) >= 3  # At least: session_start + 1 message + session_end
        assert events[0]["event_type"] == "session_start"
        assert events[-1]["event_type"] == "session_end"

    def test_messages_have_token_counts(self):
        gen = ConversationEventGenerator()
        events = gen.generate("usr_test001", _make_user_profile())
        messages = [e for e in events if e["event_type"] == "message_sent"]
        for msg in messages:
            assert msg["payload"]["input_tokens"] > 0
            assert msg["payload"]["output_tokens"] > 0


class TestAPIUsageEventGenerator:
    def test_api_event_has_status_code(self):
        gen = APIUsageEventGenerator()
        # API events mainly for enterprise/plus users
        events = gen.generate("usr_test001", _make_user_profile("enterprise"))
        if events:
            assert "http_status_code" in events[0]["payload"]
            assert "endpoint" in events[0]["payload"]


class TestBillingEventGenerator:
    def test_billing_event_has_amount(self):
        gen = BillingEventGenerator()
        events = gen.generate("usr_test001", _make_user_profile("plus"))
        if events:
            assert "amount_usd" in events[0]["payload"] or "old_plan" in events[0]["payload"]


class TestSafetyEventGenerator:
    def test_safety_event_has_violation_type(self):
        gen = SafetyEventGenerator()
        # Force generation by calling many times (5% chance per call)
        for _ in range(100):
            events = gen.generate("usr_test001", _make_user_profile())
            if events:
                assert "violation_type" in events[0]["payload"]
                assert "severity" in events[0]["payload"]
                break
