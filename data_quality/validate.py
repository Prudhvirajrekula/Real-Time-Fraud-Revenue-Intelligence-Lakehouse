"""
Great Expectations Validation Runner

Validates data quality across bronze and gold layers:
- Completeness (no null critical fields)
- Uniqueness (no duplicate primary keys)
- Validity (amounts > 0, rates between 0-1)
- Freshness (data is recent)
- Volume (row counts within expected ranges)

Run: python data_quality/validate.py
"""

import os
import sys
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import psycopg2
import structlog

logger = structlog.get_logger(__name__)

POSTGRES_CONN = os.environ["POSTGRES_CONN_STRING"]

# ---------------------------------------------------------------------------
# Lightweight validation without GE dependency (standalone mode)
# ---------------------------------------------------------------------------

class ValidationResult:
    def __init__(self, table: str, check: str):
        self.table = table
        self.check = check
        self.passed = True
        self.message = ""
        self.row_count = 0
        self.failed_count = 0

    def fail(self, message: str, failed_count: int = 0):
        self.passed = False
        self.message = message
        self.failed_count = failed_count
        return self

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.table}.{self.check}: {self.message or 'OK'}"


class DataQualityRunner:

    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self.results: list[ValidationResult] = []

    def _execute(self, query: str) -> list:
        with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

    def _scalar(self, query: str):
        rows = self._execute(query)
        return rows[0][0] if rows else None

    def validate_not_null(self, table: str, column: str, schema: str = "gold") -> ValidationResult:
        r = ValidationResult(table, f"not_null({column})")
        try:
            count = self._scalar(
                f"SELECT COUNT(*) FROM {schema}.{table} WHERE {column} IS NULL"
            )
            if count > 0:
                r.fail(f"{count} NULL values found in {column}", count)
        except Exception as e:
            r.fail(f"Query error: {e}")
        self.results.append(r)
        return r

    def validate_unique(self, table: str, column: str, schema: str = "gold") -> ValidationResult:
        r = ValidationResult(table, f"unique({column})")
        try:
            count = self._scalar(
                f"SELECT COUNT(*) - COUNT(DISTINCT {column}) FROM {schema}.{table}"
            )
            if count > 0:
                r.fail(f"{count} duplicate values found in {column}", count)
        except Exception as e:
            r.fail(f"Query error: {e}")
        self.results.append(r)
        return r

    def validate_range(
        self, table: str, column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        schema: str = "gold"
    ) -> ValidationResult:
        r = ValidationResult(table, f"range({column}, [{min_val}, {max_val}])")
        try:
            conditions = []
            if min_val is not None:
                conditions.append(f"{column} < {min_val}")
            if max_val is not None:
                conditions.append(f"{column} > {max_val}")
            if conditions:
                where = " OR ".join(conditions)
                count = self._scalar(
                    f"SELECT COUNT(*) FROM {schema}.{table} WHERE {where}"
                )
                if count > 0:
                    r.fail(f"{count} values outside [{min_val}, {max_val}]", count)
        except Exception as e:
            r.fail(f"Query error: {e}")
        self.results.append(r)
        return r

    def validate_freshness(self, table: str, max_hours: int = 3, schema: str = "gold") -> ValidationResult:
        r = ValidationResult(table, f"freshness(max_{max_hours}h)")
        try:
            result = self._execute(
                f"SELECT MAX(_computed_at) FROM {schema}.{table}"
            )
            if not result or result[0][0] is None:
                r.fail("No data found - table may be empty")
                self.results.append(r)
                return r
            latest = result[0][0]
            if latest.tzinfo is None:
                from datetime import timezone
                latest = latest.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
            if age_hours > max_hours:
                r.fail(f"Data is {age_hours:.1f}h old (max {max_hours}h)")
        except Exception as e:
            r.fail(f"Query error: {e}")
        self.results.append(r)
        return r

    def validate_min_rows(self, table: str, min_rows: int, schema: str = "gold") -> ValidationResult:
        r = ValidationResult(table, f"min_rows({min_rows})")
        try:
            count = self._scalar(f"SELECT COUNT(*) FROM {schema}.{table}")
            r.row_count = count or 0
            if (count or 0) < min_rows:
                r.fail(f"Only {count} rows found (expected >= {min_rows})", 0)
        except Exception as e:
            r.fail(f"Query error: {e}")
        self.results.append(r)
        return r

    def validate_fraud_rate_sanity(self) -> ValidationResult:
        """Fraud rate should be between 0.001 and 0.50 globally."""
        r = ValidationResult("fraud_summary", "fraud_rate_sanity")
        try:
            result = self._execute(
                "SELECT AVG(fraud_rate) FROM gold.fraud_summary WHERE event_date >= CURRENT_DATE - 7"
            )
            avg_rate = result[0][0] if result else None
            if avg_rate is None:
                r.fail("No recent fraud summary data")
            elif avg_rate < 0.001:
                r.fail(f"Fraud rate suspiciously low: {avg_rate:.4f}")
            elif avg_rate > 0.50:
                r.fail(f"Fraud rate suspiciously high: {avg_rate:.4f}")
            else:
                r.message = f"Avg 7d fraud rate: {avg_rate:.4f}"
        except Exception as e:
            r.fail(f"Query error: {e}")
        self.results.append(r)
        return r

    def run_all(self) -> dict:
        logger.info("starting_data_quality_validation")
        self.results.clear()

        # ---- revenue_daily ----
        self.validate_not_null("revenue_daily", "event_date")
        self.validate_not_null("revenue_daily", "total_orders")
        self.validate_not_null("revenue_daily", "gmv")
        self.validate_range("revenue_daily", "gmv", min_val=0)
        self.validate_range("revenue_daily", "fraud_rate", min_val=0, max_val=1)
        self.validate_min_rows("revenue_daily", min_rows=1)
        self.validate_freshness("revenue_daily", max_hours=6)

        # ---- fraud_summary ----
        self.validate_not_null("fraud_summary", "event_date")
        self.validate_range("fraud_summary", "fraud_rate", min_val=0, max_val=1)
        self.validate_fraud_rate_sanity()
        self.validate_min_rows("fraud_summary", min_rows=1)

        # ---- user_fraud_scores ----
        self.validate_not_null("user_fraud_scores", "user_id")
        self.validate_unique("user_fraud_scores", "user_id")
        self.validate_range("user_fraud_scores", "composite_risk_score", min_val=0, max_val=1)
        self.validate_min_rows("user_fraud_scores", min_rows=10)

        # Summary
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed

        for r in self.results:
            level = logger.info if r.passed else logger.warning
            level("validation_result",
                  table=r.table, check=r.check,
                  passed=r.passed, message=r.message)

        logger.info("validation_summary",
                    total=total, passed=passed, failed=failed,
                    pass_rate=f"{100*passed/total:.1f}%")

        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": passed / total if total > 0 else 0,
            "results": [
                {"table": r.table, "check": r.check, "passed": r.passed, "message": r.message}
                for r in self.results
            ],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


if __name__ == "__main__":
    runner = DataQualityRunner(POSTGRES_CONN)
    report = runner.run_all()

    print(f"\n{'='*60}")
    print(f"DATA QUALITY REPORT - {report['timestamp']}")
    print(f"{'='*60}")
    for r in runner.results:
        print(r)
    print(f"{'='*60}")
    print(f"PASSED: {report['passed']}/{report['total']} ({100*report['pass_rate']:.1f}%)")
    print(f"{'='*60}\n")

    if report["failed"] > 0:
        sys.exit(1)
