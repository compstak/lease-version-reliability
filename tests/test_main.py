import pytest
from typer.testing import CliRunner

from lease_version_reliability.main import app


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


def test_main_succeeds(runner: CliRunner) -> None:
    """Exit with status code zero."""
    result = runner.invoke(app, ["main"])
    assert result.exit_code == 0


def test_main_prints_welcome_message(runner: CliRunner) -> None:
    """Print welcome message"""
    result = runner.invoke(app, ["main"])
    assert "Welcome to lease version reliability cli" in result.stdout
