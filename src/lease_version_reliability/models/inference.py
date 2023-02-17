import numpy as np
import pandas as pd
import structlog

from lease_version_reliability.common.file_io import download_models, read_model
from lease_version_reliability.config.settings import settings
from lease_version_reliability.data.database import CompstakServicesMySQL
from lease_version_reliability.data.database import cs_mysql_instance as mysql
from lease_version_reliability.data.database_io import (
    get_all_data,
    get_column_names,
    get_labels,
    get_reliable_data,
    get_split_columns,
    write_submitter_df_snowflake,
    write_version_realiability_df_snowflake,
)
from lease_version_reliability.data.output_data import (
    get_submitter_reliability,
    get_version_reliability,
)
from lease_version_reliability.features.build_features import (
    feature_engineering,
)

logger = structlog.get_logger()


async def load_data(
    db: CompstakServicesMySQL,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get reliable data (with more than 3 versions) and all version data
    Retrun datasets after performing feature engineering
    """
    # training data (masters with >3 versions within it)
    logger.info("Reading Reliable Data from MySQL")
    reliable_data = await get_reliable_data(db)

    # all version data needed to export a reliability score
    logger.info("Reading All Data from MySQL")
    all_data = await get_all_data(db)

    attributes = settings.ATTRIBUTES

    col_names_correct, col_names_filled, col_names_label = get_column_names(
        attributes,
    )

    logger.info("Data Labels - Reliable Data")
    data = get_labels(reliable_data, attributes)
    logger.info("Data Labels - All Data")
    all_data = get_labels(all_data, attributes)

    logger.info("Feature Engineering - Reliable Data")
    df = feature_engineering(
        data,
        col_names_label,
        col_names_filled,
        col_names_correct,
        attributes,
    )

    logger.info("Feature Engineering - All Data")
    df_all = feature_engineering(
        all_data,
        col_names_label,
        col_names_filled,
        col_names_correct,
        attributes,
    )

    return df, df_all


async def run_inference(download: bool) -> None:
    """
    Calculate reliability score of submitters and all versions
    Insert inference into Snowflake
    """
    logger.info("Connecting to MySQL")
    await mysql.connect()
    if download:
        download_models()
    model_dict = read_model(settings.TRAIN_CONFIG.MODEL_FILENAME)

    df, df_all = await load_data(mysql)
    x_cols, y_cols = get_split_columns(df.columns)

    attributes = settings.ATTRIBUTES

    logger.info("Calculating Submitter Results")
    submitter_df, _ = get_submitter_reliability(df, x_cols, y_cols, model_dict)

    logger.info("Calculating Version Results")
    version_reliability_df = get_version_reliability(
        df_all,
        attributes,
        x_cols,
        y_cols,
        model_dict,
    )

    logger.info("Exporting <SUBMITTER RELIABILITY> into Snowflake")
    write_submitter_df_snowflake(
        submitter_df,
        "ML_PIPELINE_DEV_DB.LEASE_VERSION_RELIABILITY",
        "SUBMITTER",
    )

    logger.info("Exporting <VERSION RELIABILITY> into Snowflake")
    logger.info(f"Total len of {len(version_reliability_df)}")

    for i, chunk in enumerate(
        np.array_split(
            version_reliability_df,
            settings.BATCH_CONFIG.BATCH_SIZE,
        ),
    ):
        logger.info(f"processing batch: {i + 1}/10")
        write_version_realiability_df_snowflake(
            chunk,
            "ML_PIPELINE_DEV_DB.LEASE_VERSION_RELIABILITY",
            "VERSION",
        )

    logger.info(submitter_df.head())
    logger.info(version_reliability_df.head())

    logger.info("Disconnecting to MySQL")
    await mysql.disconnect()
