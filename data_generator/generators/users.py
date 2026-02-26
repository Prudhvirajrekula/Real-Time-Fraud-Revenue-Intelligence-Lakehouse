"""User event generator - registration and profile updates."""

import random
from typing import Any

from .base import BaseGenerator, REFERRAL_SOURCES, fake


class UserGenerator(BaseGenerator):

    def generate(self) -> dict[str, Any]:
        user = self.random_user()
        now = self.now_ms()
        return {
            **user,
            "is_active": True,
            "last_login_at": now,
            "ingest_timestamp": now,
        }

    def generate_new_registration(self) -> dict[str, Any]:
        """Simulate a brand-new user registration event."""
        country = self.random_country()
        now = self.now_ms()
        return {
            "user_id": self.new_id(),
            "email": fake.email(),
            "username": fake.user_name(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "country": country,
            "account_age_days": 0,
            "is_verified": False,
            "is_active": True,
            "risk_tier": "low",
            "total_orders_lifetime": 0,
            "total_spend_lifetime": 0.0,
            "chargeback_count": 0,
            "referral_source": random.choice(REFERRAL_SOURCES),
            "created_at": now,
            "last_login_at": now,
            "ingest_timestamp": now,
        }
