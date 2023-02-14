import nox
from nox_poetry import Session, session

nox.options.error_on_external_run = True
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["type_check", "test"]


@session()
def test(s: Session) -> None:
    s.install(".", "pytest", "pytest-cov", "mypy")
    s.run(
        "python",
        "-m",
        "pytest",
        "--cov=owner_relationship",
        "--cov-report=html",
        "--cov-report=term",
        "--junitxml=test-results/junit.xml",
        "tests",
        *s.posargs,
    )


@session(venv_backend="none")
def type_check(s: Session) -> None:
    s.run("mypy", "src", "tests", "noxfile.py")
