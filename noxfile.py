import nox
from nox_poetry import Session, session

nox.options.error_on_external_run = True
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["type_check", "test"]


@session(venv_backend="none")
def test(s: Session) -> None:
    s.run(
        "python",
        "-m",
        "pytest",
        "--cov=lease_version_reliability",
        "--cov-report=html",
        "--cov-report=term",
        "--junitxml=test-results/junit.xml",
        "tests",
        *s.posargs,
    )


@session(venv_backend="none")
def type_check(s: Session) -> None:
    s.run("mypy", "src", "tests", "noxfile.py")
