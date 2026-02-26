"""Payment event generator with realistic failure patterns."""

import random
from typing import Any

from .base import BaseGenerator, GATEWAYS, fake


class PaymentGenerator(BaseGenerator):

    RESPONSE_CODES_SUCCESS = ["00", "85", "08", "10"]
    RESPONSE_CODES_FAILURE = ["05", "14", "51", "54", "57", "61", "62", "63", "91"]
    RESPONSE_CODES_FRAUD   = ["59", "63", "78"]

    def generate_from_order(self, order: dict) -> dict[str, Any]:
        is_fraud = order.get("is_fraud", False)
        if is_fraud:
            return self._build_fraud_payment(order)
        return self._build_legit_payment(order)

    def _build_legit_payment(self, order: dict) -> dict:
        now = self.now_ms()
        method = order["payment_method"]
        status = random.choices(
            ["completed", "processing", "failed"],
            weights=[0.90, 0.06, 0.04]
        )[0]

        card_last_four = fake.numerify("####") if "card" in method else None
        card_brand = random.choice(["Visa", "Mastercard", "Amex", "Discover"]) if "card" in method else None
        card_country = order.get("billing_country", "US") if "card" in method else None

        return {
            "payment_id": self.new_id(),
            "order_id": order["order_id"],
            "user_id": order["user_id"],
            "amount": order["total_amount"],
            "currency": order["currency"],
            "payment_status": status,
            "payment_method": method,
            "card_last_four": card_last_four,
            "card_brand": card_brand,
            "card_country": card_country,
            "gateway": random.choice(GATEWAYS),
            "gateway_transaction_id": self.new_id(),
            "gateway_response_code": random.choice(self.RESPONSE_CODES_SUCCESS if status == "completed" else self.RESPONSE_CODES_FAILURE),
            "risk_score": round(random.uniform(0.01, 0.35), 4),
            "is_3ds_authenticated": random.random() > 0.3,
            "processor_fee": round(order["total_amount"] * 0.029 + 0.30, 4),
            "is_fraud": False,
            "created_at": now,
            "ingest_timestamp": now,
        }

    def _build_fraud_payment(self, order: dict) -> dict:
        now = self.now_ms()
        method = order["payment_method"]

        # Fraud payments often fail initially or get declined
        status = random.choices(
            ["failed", "completed", "processing"],
            weights=[0.45, 0.40, 0.15]
        )[0]

        # Stolen card scenarios
        card_last_four = fake.numerify("####")
        card_brand = random.choice(["Visa", "Mastercard", "Amex"])

        # Card country often mismatches billing country for fraud
        card_country = self.random_country()

        return {
            "payment_id": self.new_id(),
            "order_id": order["order_id"],
            "user_id": order["user_id"],
            "amount": order["total_amount"],
            "currency": order["currency"],
            "payment_status": status,
            "payment_method": method,
            "card_last_four": card_last_four,
            "card_brand": card_brand,
            "card_country": card_country,
            "gateway": random.choice(GATEWAYS),
            "gateway_transaction_id": self.new_id(),
            "gateway_response_code": random.choice(
                self.RESPONSE_CODES_FRAUD if status == "failed" else self.RESPONSE_CODES_SUCCESS
            ),
            "risk_score": round(random.uniform(0.65, 0.99), 4),
            "is_3ds_authenticated": False,  # Fraud rarely passes 3DS
            "processor_fee": round(order["total_amount"] * 0.029 + 0.30, 4),
            "is_fraud": True,
            "created_at": now,
            "ingest_timestamp": now,
        }
