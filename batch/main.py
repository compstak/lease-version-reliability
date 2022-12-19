import asyncio

import numpy as np
import pandas as pd
import structlog

from batch.common.file_io import download_dataset, download_models
from batch.common.logging import initialize_logging
from batch.config.settings import settings
from batch.data.database_io import (
    get_split_columns,
    get_submitter_info,
    write_submitter_df_snowflake,
    write_version_realiability_df_snowflake,
)
from batch.data.output_data import (
    get_submitter_reliability,
    get_version_reliability,
)

logger = structlog.get_logger()
initialize_logging(settings.ENV)


async def main() -> None:
    logger.info("Downloading processed dataset from S3")
    download_dataset("processed")
    df = pd.read_pickle(f"{settings.DATA_DIR}/processed/reliable_data")
    df_all = pd.read_pickle(f"{settings.DATA_DIR}/processed/all_data")

    logger.info("Downloading lease-reliability classifiers from S3")
    download_models()
    model_dict = pd.read_pickle(f"{settings.MODEL_DIR}/{settings.MODEL_NAME}")

    # submitter name for display purposes when exporting validation data
    submitter_name = get_submitter_info()
    x_cols, y_cols = get_split_columns(df.columns)

    attributes = settings.ATTRIBUTES

    logger.info("Calculating Submitter Results")
    submitter_df, _ = await get_submitter_reliability(
        df,
        x_cols,
        y_cols,
        model_dict,
        submitter_name,
    )

    logger.info("Calculating Version Results")
    version_reliability_df = get_version_reliability(
        df_all,
        attributes,
        x_cols,
        y_cols,
        model_dict,
    )

    # export submitter result to Snowflake
    logger.info("Exporting <SUBMITTER RELIABILITY> into Snowflake")
    write_submitter_df_snowflake(
        submitter_df,
        "ML_PIPELINE_DEV_DB.LEASE_VERSION_RELIABILITY",
        "SUBMITTER",
    )

    # export version result to Snowflake
    logger.info("Exporting <VERSION RELIABILITY> into Snowflake")
    logger.info(f"Total len of {len(version_reliability_df)}")

    for i, chunk in enumerate(np.array_split(version_reliability_df, 10)):
        logger.info(f"processing batch: {i + 1}/10")
        write_version_realiability_df_snowflake(
            chunk,
            "ML_PIPELINE_DEV_DB.LEASE_VERSION_RELIABILITY",
            "VERSION",
        )


if __name__ == "__main__":
    asyncio.run(main())
