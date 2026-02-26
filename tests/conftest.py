"""
Shared pytest fixtures for unit and integration tests.
"""

import sys
import os
import pytest

# Ensure imports resolve correctly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data_generator"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml"))


@pytest.fixture(scope="session")
def base_generator():
    """Shared BaseGenerator instance (expensive to create - reuse across tests)."""
    from data_generator.generators.base import BaseGenerator
    return BaseGenerator(user_pool_size=500, product_pool_size=100)


@pytest.fixture(scope="session")
def order_generator(base_generator):
    from data_generator.generators.orders import OrderGenerator
    gen = OrderGenerator(user_pool_size=500, product_pool_size=100)
    gen.user_pool = base_generator.user_pool
    gen.product_catalog = base_generator.product_catalog
    return gen


@pytest.fixture(scope="session")
def payment_generator(base_generator):
    from data_generator.generators.payments import PaymentGenerator
    gen = PaymentGenerator(user_pool_size=500)
    gen.user_pool = base_generator.user_pool
    return gen


@pytest.fixture(scope="session")
def user_generator(base_generator):
    from data_generator.generators.users import UserGenerator
    gen = UserGenerator(user_pool_size=500)
    gen.user_pool = base_generator.user_pool
    return gen


@pytest.fixture(scope="session")
def device_generator(base_generator):
    from data_generator.generators.devices import DeviceGenerator
    gen = DeviceGenerator(user_pool_size=500)
    gen.user_pool = base_generator.user_pool
    return gen


@pytest.fixture(scope="session")
def sample_order(order_generator):
    return order_generator.generate(fraud_ratio=0.0)


@pytest.fixture(scope="session")
def sample_fraud_order(order_generator):
    order = order_generator.generate(fraud_ratio=1.0)
    order["is_fraud"] = True
    return order


@pytest.fixture(scope="session")
def sample_payment(payment_generator, sample_order):
    return payment_generator.generate_from_order(sample_order)


@pytest.fixture(scope="session")
def fraud_detector():
    """Fraud detector trained on synthetic data."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml"))
    from models.fraud_detector import FraudDetector
    from models.train import generate_synthetic_training_data

    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        df = generate_synthetic_training_data(n=10_000)
        detector = FraudDetector(model_registry_path=tmpdir, fraud_threshold=0.70)
        detector.fit(df, use_smote=True, optimize_hyperparams=False)
        yield detector
