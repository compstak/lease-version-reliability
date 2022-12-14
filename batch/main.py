import asyncio

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

# import typing


logger = structlog.get_logger()
initialize_logging(settings.ENV)


async def main() -> None:
    download_dataset("processed")
    df = pd.read_pickle(f"{settings.DATA_DIR}/reliable_data")
    df_all = pd.read_pickle(f"{settings.DATA_DIR}/all_data")

    download_models()
    model_dict = pd.read_pickle(f"{settings.MODEL_DIR}/{settings.MODEL_NAME}")

    submitter_name = get_submitter_info()
    x_cols, y_cols = get_split_columns(df.columns)

    attributes = settings.ATTRIBUTES

    logger.info("Exporting Submitter Results")
    submitter_df, _ = await get_submitter_reliability(
        df,
        x_cols,
        y_cols,
        model_dict,
        submitter_name,
    )

    logger.info("Exporting Version Results")
    version_reliability_df = get_version_reliability(
        df_all,
        attributes,
        x_cols,
        y_cols,
        model_dict,
    )

    # export submitter result to Snowflake
    logger.info("exporting submitter results into Snowflake")
    write_submitter_df_snowflake(
        submitter_df,
        "internal_analytics.experiments",
        "submitter_reliability",
    )
    # export version result to Snowflake
    logger.info("exporting submitter results into Snowflake")
    write_version_realiability_df_snowflake(
        version_reliability_df,
        "internal_analytics.experiments",
        "version_reliability",
    )


if __name__ == "__main__":
    asyncio.run(main())
