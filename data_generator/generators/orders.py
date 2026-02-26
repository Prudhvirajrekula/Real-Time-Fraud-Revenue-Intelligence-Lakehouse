"""Order event generator with realistic fraud patterns."""

import random
from typing import Any

from .base import (
    BaseGenerator,
    COUNTRY_WEIGHTS,
    PAYMENT_METHODS,
    PAYMENT_METHOD_WEIGHTS,
    fake,
)


class OrderGenerator(BaseGenerator):

    def generate(self, fraud_ratio: float = 0.03) -> dict[str, Any]:
        user = self.random_user()
        is_fraud = self.is_fraud_scenario(user, fraud_ratio)
        return self._build_order(user, is_fraud)

    def _build_order(self, user: dict, is_fraud: bool) -> dict[str, Any]:
        session_id = self.new_id()
        device_id = self.new_id()
        now = self.now_ms()

        # Fraud orders tend to have suspicious characteristics
        if is_fraud:
            return self._build_fraud_order(user, session_id, device_id, now)
        return self._build_legit_order(user, session_id, device_id, now)

    def _build_legit_order(self, user: dict, session_id: str, device_id: str, now: int) -> dict:
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.50, 0.25, 0.12, 0.08, 0.05])[0]
        items = [self._build_item() for _ in range(num_items)]
        total = round(sum(i["unit_price"] * i["quantity"] for i in items), 2)
        country = user["country"]
        currency = COUNTRY_WEIGHTS.get(country, {}).get("currency", "USD")

        return {
            "order_id": self.new_id(),
            "user_id": user["user_id"],
            "session_id": session_id,
            "device_id": device_id,
            "items": items,
            "total_amount": total,
            "currency": currency,
            "order_status": random.choices(
                ["pending", "confirmed", "shipped", "delivered", "cancelled"],
                weights=[0.10, 0.25, 0.30, 0.30, 0.05]
            )[0],
            "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0],
            "shipping_country": country,
            "shipping_city": fake.city(),
            "billing_country": country,
            "ip_address": self.random_ip(high_risk=False),
            "user_agent": self.random_user_agent(),
            "is_fraud": False,
            "created_at": now,
            "ingest_timestamp": now,
        }

    def _build_fraud_order(self, user: dict, session_id: str, device_id: str, now: int) -> dict:
        fraud_pattern = random.choices(
            ["high_value_single", "multiple_expensive", "mismatch_geo", "velocity_burst"],
            weights=[0.35, 0.25, 0.25, 0.15]
        )[0]

        if fraud_pattern == "high_value_single":
            # Single very expensive item
            item = self._build_item(force_high_value=True)
            items = [item]
            total = round(item["unit_price"] * item["quantity"], 2)
        elif fraud_pattern == "multiple_expensive":
            # Many expensive items
            items = [self._build_item(force_high_value=True) for _ in range(random.randint(3, 8))]
            total = round(sum(i["unit_price"] * i["quantity"] for i in items), 2)
        else:
            items = [self._build_item() for _ in range(random.randint(1, 3))]
            total = round(sum(i["unit_price"] * i["quantity"] for i in items), 2)

        # Fraud orders often have billing/shipping mismatch
        billing_country = user["country"]
        shipping_country = self.random_country() if fraud_pattern == "mismatch_geo" else billing_country
        currency = COUNTRY_WEIGHTS.get(billing_country, {}).get("currency", "USD")

        return {
            "order_id": self.new_id(),
            "user_id": user["user_id"],
            "session_id": session_id,
            "device_id": device_id,
            "items": items,
            "total_amount": total,
            "currency": currency,
            "order_status": "pending",  # Fraud orders often stay pending
            "payment_method": random.choices(
                PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS
            )[0],
            "shipping_country": shipping_country,
            "shipping_city": fake.city(),
            "billing_country": billing_country,
            "ip_address": self.random_ip(high_risk=True),
            "user_agent": self.random_user_agent(),
            "is_fraud": True,
            "created_at": now,
            "ingest_timestamp": now,
        }

    def _build_item(self, force_high_value: bool = False) -> dict:
        product = self.random_product()
        price = product["base_price"]
        if force_high_value:
            price = round(random.uniform(500, 5000), 2)
        return {
            "product_id": product["product_id"],
            "product_category": product["category"],
            "quantity": 1 if force_high_value else random.randint(1, 5),
            "unit_price": price,
        }
