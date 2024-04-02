#!/usr/bin/env python3
import asyncio

from rich.console import Console
import structlog
from typer import Option, Typer

from lease_version_reliability.common.logging import initialize_logging
from lease_version_reliability.models.inference import run_inference
from lease_version_reliability.models.train import train_model

initialize_logging()
logger = structlog.get_logger()
app = Typer(add_completion=False)
console = Console()


@app.command()
def main() -> None:
    """Run cli main"""
    console.print("Welcome to lease version reliability CLI")


@app.command()
def train(
    model: bool = Option(False, help="Upload model."),
) -> None:
    """Run training script"""
    logger.info("Start training inference model.")

    async def _train(model: bool) -> None:
        await train_model(model)

    asyncio.run(_train(model))


@app.command()
def inference(
    model: bool = Option(False, help="Download model."),
) -> None:
    """Run inference script"""
    logger.info("Start calculating inference.")

    async def _inference(model: bool) -> None:
        await run_inference(model)

    asyncio.run(_inference(model))


if __name__ == "__main__":  # pragma: no cover
    app()
