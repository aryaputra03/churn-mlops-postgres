"""
Pytest configuration file - PRODUCTION READY

This file is placed in the tests/ directory to configure pytest options.
Fixed to ensure safe password handling.
"""

import pytest
import os


# Set testing environment variable
os.environ["TESTING"] = "true"
os.environ["DISABLE_RATE_LIMIT"] = "true"

# ==========================================
# CRITICAL: Safe password constants
# ==========================================

# Use SHORT passwords (under 60 characters for bcrypt safety)
TEST_PASSWORD = "Test123!"      # 8 chars - SAFE
ADMIN_PASSWORD = "Admin123!"    # 9 chars - SAFE
USER_PASSWORD = "Pass123!"      # 8 chars - SAFE


def pytest_addoption(parser):
    """Add custom pytest command line options"""
    parser.addoption(
        "--ci",
        action="store_true",
        default=False,
        help="Run in CI mode (skip slow tests)"
    )
    parser.addoption(
        "--redis",
        action="store_true",
        default=False,
        help="Run Redis tests (requires Redis server)"
    )


def pytest_configure(config):
    """Configure pytest markers"""
    config.addinivalue_line(
        "markers", "slow: mark test as slow (skipped in CI mode)"
    )
    config.addinivalue_line(
        "markers", "redis: mark test as requiring Redis"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options"""
    # Skip slow tests in CI mode
    if config.getoption("--ci"):
        skip_slow = pytest.mark.skip(reason="Slow test skipped in CI mode")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)
    
    # Skip Redis tests if Redis not available or not requested
    if not config.getoption("--redis"):
        skip_redis = pytest.mark.skip(
            reason="Redis tests skipped (use --redis to enable)"
        )
        for item in items:
            if "redis" in item.keywords:
                item.add_marker(skip_redis)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment"""
    # Disable rate limiting during tests
    os.environ["DISABLE_RATE_LIMIT"] = "true"
    os.environ["TESTING"] = "true"
    
    # Set safe password environment variables for tests
    os.environ["TEST_PASSWORD"] = TEST_PASSWORD
    os.environ["ADMIN_PASSWORD"] = ADMIN_PASSWORD
    os.environ["USER_PASSWORD"] = USER_PASSWORD
    
    print("\n" + "=" * 60)
    print("TEST ENVIRONMENT SETUP")
    print("=" * 60)
    print("Testing mode: ENABLED")
    print("Rate limiting: DISABLED")
    print(f"Test password length: {len(TEST_PASSWORD)} chars (SAFE)")
    print("=" * 60 + "\n")
    
    yield
    
    # Cleanup
    for key in ["DISABLE_RATE_LIMIT", "TESTING", "TEST_PASSWORD", 
                "ADMIN_PASSWORD", "USER_PASSWORD"]:
        if key in os.environ:
            del os.environ[key]


@pytest.fixture(scope="function")
def safe_passwords():
    """Provide safe passwords for tests"""
    return {
        "test": TEST_PASSWORD,
        "admin": ADMIN_PASSWORD,
        "user": USER_PASSWORD
    }