"""
Base generator providing shared state: user pool, product catalog, fraud profiles.
All domain generators inherit from this class.
"""

import hashlib
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import numpy as np
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Country configuration with risk weights
# ---------------------------------------------------------------------------
COUNTRY_WEIGHTS = {
    "US": {"weight": 0.35, "risk": 0.02, "currency": "USD"},
    "GB": {"weight": 0.12, "risk": 0.025, "currency": "GBP"},
    "DE": {"weight": 0.10, "risk": 0.015, "currency": "EUR"},
    "FR": {"weight": 0.08, "risk": 0.02, "currency": "EUR"},
    "CA": {"weight": 0.07, "risk": 0.02, "currency": "CAD"},
    "AU": {"weight": 0.06, "risk": 0.025, "currency": "AUD"},
    "JP": {"weight": 0.05, "risk": 0.01, "currency": "JPY"},
    "BR": {"weight": 0.04, "risk": 0.055, "currency": "BRL"},
    "IN": {"weight": 0.04, "risk": 0.04, "currency": "INR"},
    "NG": {"weight": 0.01, "risk": 0.18, "currency": "NGN"},
    "RU": {"weight": 0.015, "risk": 0.12, "currency": "RUB"},
    "CN": {"weight": 0.03, "risk": 0.06, "currency": "CNY"},
    "MX": {"weight": 0.025, "risk": 0.07, "currency": "MXN"},
    "ZA": {"weight": 0.015, "risk": 0.08, "currency": "ZAR"},
    "PH": {"weight": 0.01, "risk": 0.09, "currency": "PHP"},
}

HIGH_RISK_COUNTRIES = {k for k, v in COUNTRY_WEIGHTS.items() if v["risk"] > 0.05}

COUNTRY_CODES = list(COUNTRY_WEIGHTS.keys())
COUNTRY_WEIGHTS_LIST = [v["weight"] for v in COUNTRY_WEIGHTS.values()]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer", "crypto"]
PAYMENT_METHOD_WEIGHTS = [0.40, 0.25, 0.15, 0.08, 0.07, 0.04, 0.01]

GATEWAYS = ["stripe", "braintree", "adyen", "worldpay", "paypal", "square"]

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
    "Beauty", "Toys", "Automotive", "Food", "Jewelry", "Software", "Gaming"
]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_TYPE_WEIGHTS = [0.55, 0.35, 0.10]

OS_BY_DEVICE = {
    "mobile": [("iOS", "17.2"), ("iOS", "16.6"), ("Android", "14"), ("Android", "13")],
    "desktop": [("Windows", "11"), ("Windows", "10"), ("macOS", "14"), ("macOS", "13"), ("Ubuntu", "22.04")],
    "tablet": [("iPadOS", "17.2"), ("Android", "13"), ("Windows", "11")],
}

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet", "Opera"]
BROWSER_WEIGHTS = [0.65, 0.19, 0.04, 0.04, 0.04, 0.04]

REFERRAL_SOURCES = ["organic", "email", "social", "paid_search", "affiliate", "direct", "influencer"]


class BaseGenerator:
    """
    Shared state pool for all generators.
    Pre-generates a pool of users, products, and sessions to create
    realistic referential integrity across events.
    """

    def __init__(self, user_pool_size: int = 10_000, product_pool_size: int = 1_000):
        self.user_pool = self._build_user_pool(user_pool_size)
        self.product_catalog = self._build_product_catalog(product_pool_size)
        self._active_sessions: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Pool builders
    # ------------------------------------------------------------------

    def _build_user_pool(self, size: int) -> list[dict]:
        users = []
        for _ in range(size):
            country = random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS_LIST)[0]
            risk_tier = self._assign_risk_tier(country)
            account_age = random.randint(0, 2000)
            users.append({
                "user_id": str(uuid.uuid4()),
                "email": fake.email(),
                "username": fake.user_name(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "country": country,
                "account_age_days": account_age,
                "is_verified": account_age > 30,
                "risk_tier": risk_tier,
                "total_orders_lifetime": random.randint(0, 500),
                "total_spend_lifetime": round(random.uniform(0, 50000), 2),
                "chargeback_count": random.choices([0, 1, 2, 3], weights=[0.90, 0.07, 0.02, 0.01])[0],
                "referral_source": random.choice(REFERRAL_SOURCES),
                "created_at": int(fake.date_time_between(start_date="-5y").timestamp() * 1000),
            })
        return users

    def _build_product_catalog(self, size: int) -> list[dict]:
        products = []
        for _ in range(size):
            category = random.choice(PRODUCT_CATEGORIES)
            products.append({
                "product_id": str(uuid.uuid4()),
                "product_name": fake.catch_phrase(),
                "category": category,
                "base_price": round(random.uniform(5, 2000), 2),
            })
        return products

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _assign_risk_tier(country: str) -> str:
        base_risk = COUNTRY_WEIGHTS.get(country, {}).get("risk", 0.05)
        roll = random.random()
        if roll < base_risk * 3:
            return "high"
        elif roll < base_risk * 10:
            return "medium"
        return "low"

    @staticmethod
    def now_ms() -> int:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def fingerprint(user_id: str, device_type: str, os: str, browser: str) -> str:
        raw = f"{user_id}:{device_type}:{os}:{browser}:{random.randint(0, 9999)}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def random_user(self) -> dict[str, Any]:
        return random.choice(self.user_pool)

    def random_product(self) -> dict[str, Any]:
        return random.choice(self.product_catalog)

    def random_country(self) -> str:
        return random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS_LIST)[0]

    def is_fraud_scenario(self, user: dict, fraud_ratio: float) -> bool:
        """Determine if this event should simulate fraud."""
        risk_multiplier = {"high": 5.0, "medium": 2.0, "low": 0.5}.get(user["risk_tier"], 1.0)
        effective_rate = min(fraud_ratio * risk_multiplier, 0.8)
        return random.random() < effective_rate

    @staticmethod
    def random_ip(high_risk: bool = False) -> str:
        if high_risk:
            # Use ranges associated with VPN/proxy providers (simulated)
            prefixes = ["185.220", "194.165", "45.142", "23.129"]
            prefix = random.choice(prefixes)
            return f"{prefix}.{random.randint(1, 254)}.{random.randint(1, 254)}"
        return fake.ipv4()

    @staticmethod
    def random_user_agent() -> str:
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 Safari/604.1",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Safari/605.1.15",
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile",
            "python-requests/2.31.0",  # Occasional bot UA
        ]
        weights = [0.40, 0.30, 0.15, 0.14, 0.01]
        return random.choices(agents, weights=weights)[0]
