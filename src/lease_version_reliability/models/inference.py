import gc
import logging

import pandas as pd
from sqlalchemy.dialects import registry
import structlog

from lease_version_reliability.common.file_io import download_models, read_model
from lease_version_reliability.config.settings import settings
from lease_version_reliability.data.database import cs_mysql_instance as mysql
from lease_version_reliability.data.database_io import (
    get_all_data,
    get_column_names,
    get_labels,
    get_reliable_data_version_ids,
    get_split_columns,
    modify_submitter_df,
    modify_version_df,
    write_into_snowflake,
)
from lease_version_reliability.data.output_data import (
    get_submitter_reliability,
    get_version_reliability,
)
from lease_version_reliability.features.build_features import (
    feature_engineering,
)

logger = structlog.get_logger()
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)


async def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get reliable data (with more than 3 versions) and all version data
    Retrun datasets after performing feature engineering
    """
    # training data (masters with >3 versions within it)
    # logger.info("Reading Reliable Data from MySQL")
    # reliable_data = await get_reliable_data()

    # all version data needed to export a reliability score

    logger.info("Get Reliable Version Ids")
    reliable_version_ids = await get_reliable_data_version_ids()

    logger.info("Reading All Data from MySQL")
    all_data = await get_all_data()

    attributes = settings.ATTRIBUTES

    col_names_correct, col_names_filled, col_names_label = get_column_names(
        attributes,
    )

    logger.info("Data Labels - All Data")
    all_data = get_labels(all_data, attributes)

    logger.info("Feature Engineering - All Data")
    df_all = feature_engineering(
        all_data,
        col_names_label,
        col_names_filled,
        col_names_correct,
        attributes,
    )

    df_reliable = df_all[df_all.id.isin(reliable_version_ids)]
    logger.info(df_reliable.columns.tolist())
    logger.info(df_all.columns.tolist())

    return df_reliable, df_all


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

    df_reliable, df_all = await load_data()
    _, y_cols = get_split_columns(df_reliable.columns)

    attributes = settings.ATTRIBUTES

    logger.info("Calculating Submitter Results")
    submitter_export = get_submitter_reliability(
        df_reliable,
        y_cols,
        model_dict,
    )

    del df_reliable
    gc.collect()

    logger.info("Calculating Version Results")
    version_export = get_version_reliability(
        df_all,
        attributes,
        y_cols,
        model_dict,
    )

    del df_all
    gc.collect()

    submitter_export = modify_submitter_df(submitter_export)
    version_export = modify_version_df(version_export)

    registry.register("snowflake", "snowflake.sqlalchemy", "dialect")

    logger.info("Exporting <SUBMITTER RELIABILITY> into Snowflake")
    write_into_snowflake(
        submitter_export,
        "LEASE_VERSION_RELIABILITY",
        "submitter",
    )

    logger.info("Exporting <VERSION RELIABILITY> into Snowflake")
    logger.info(f"Total len of {len(version_export)}")

    write_into_snowflake(
        version_export,
        "LEASE_VERSION_RELIABILITY",
        "version",
    )

    logger.info(submitter_export.head())
    logger.info(version_export.head())

    logger.info("Disconnecting to MySQL")
    await mysql.disconnect()
