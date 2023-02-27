from lease_version_reliability.common.logging import (
    configure_logging,
    initialize_logging,
)


def test_initialization_without_pre_configuration() -> None:
    """Initialize logging configuration."""
    initialize_logging(is_configured=False)


def test_initialization_with_pre_configuration() -> None:
    """Initialize logging configuration"""
    initialize_logging(is_configured=True)


def test_atty_configuration() -> None:
    """Configure logging for atty"""
    configure_logging(isatty=True)


def test_non_atty_configuration() -> None:
    """Configure logging for non-atty"""
    configure_logging(isatty=False)
