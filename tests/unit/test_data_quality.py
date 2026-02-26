"""
Unit tests for data quality validation logic.
Uses in-memory SQLite to simulate PostgreSQL validations.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


class TestValidationResult:

    def test_default_is_passed(self):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "data_quality"))
        from validate import ValidationResult
        r = ValidationResult("my_table", "my_check")
        assert r.passed is True
        assert r.message == ""

    def test_fail_marks_as_failed(self):
        from validate import ValidationResult
        r = ValidationResult("my_table", "my_check")
        r.fail("Something broke", failed_count=5)
        assert r.passed is False
        assert r.failed_count == 5
        assert "Something broke" in r.message

    def test_repr_shows_status(self):
        from validate import ValidationResult
        r = ValidationResult("orders", "not_null(order_id)")
        assert "[PASS]" in repr(r)
        r.fail("nulls found")
        assert "[FAIL]" in repr(r)
