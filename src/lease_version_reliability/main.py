#!/usr/bin/env python3
import asyncio

from rich.console import Console
import structlog
from typer import Option, Typer

from lease_version_reliability.common.logging import initialize_logging
from lease_version_reliability.models.train import train_model

initialize_logging()
logger = structlog.get_logger()
app = Typer(add_completion=False)
console = Console()


@app.command()
def main() -> None:
    """Run training script"""
    console.print("Welcome to owner relationship cli")


@app.command()
def train(
    model: bool = Option(False, help="Upload model."),
) -> None:
    """Run training script"""
    logger.info("Training inference model start.")

    async def _train(model: bool) -> None:
        await train_model(model)

    asyncio.run(_train(model))


if __name__ == "__main__":  # pragma: no cover
    app()
