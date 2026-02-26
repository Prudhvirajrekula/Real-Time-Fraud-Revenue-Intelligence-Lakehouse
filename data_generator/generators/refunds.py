"""Refund event generator."""

import random
from typing import Any

from .base import BaseGenerator

REFUND_REASONS = ["fraud", "customer_request", "quality_issue", "not_received", "duplicate_charge", "wrong_item"]
REFUND_REASON_WEIGHTS = [0.15, 0.40, 0.20, 0.15, 0.05, 0.05]

REFUND_STATUSES = ["requested", "approved", "rejected", "processed"]


class RefundGenerator(BaseGenerator):

    def generate_from_order(self, order: dict, payment: dict) -> dict[str, Any]:
        is_fraud = order.get("is_fraud", False)
        now = self.now_ms()

        if is_fraud:
            reason = random.choices(
                ["fraud", "not_received", "duplicate_charge"],
                weights=[0.60, 0.30, 0.10]
            )[0]
            status = "requested"
        else:
            reason = random.choices(REFUND_REASONS, weights=REFUND_REASON_WEIGHTS)[0]
            status = random.choices(
                REFUND_STATUSES,
                weights=[0.10, 0.50, 0.10, 0.30]
            )[0]

        refund_amount = order["total_amount"]
        if reason == "quality_issue":
            refund_amount = round(refund_amount * random.uniform(0.3, 1.0), 2)

        processed_at = None
        if status == "processed":
            processed_at = now + random.randint(86_400_000, 259_200_000)  # 1-3 days later

        reason_detail_map = {
            "fraud": "Unauthorized transaction reported by customer",
            "customer_request": "Customer changed mind / no longer needed",
            "quality_issue": "Product did not match description or was defective",
            "not_received": "Order not delivered within expected timeframe",
            "duplicate_charge": "Customer was charged multiple times for same order",
            "wrong_item": "Wrong product was shipped to customer",
        }

        return {
            "refund_id": self.new_id(),
            "order_id": order["order_id"],
            "payment_id": payment["payment_id"],
            "user_id": order["user_id"],
            "amount": refund_amount,
            "currency": order["currency"],
            "reason": reason,
            "reason_detail": reason_detail_map.get(reason, ""),
            "status": status,
            "initiated_by": "customer" if not is_fraud else random.choice(["customer", "system"]),
            "is_fraud_related": is_fraud or reason == "fraud",
            "created_at": now,
            "processed_at": processed_at,
            "ingest_timestamp": now,
        }
