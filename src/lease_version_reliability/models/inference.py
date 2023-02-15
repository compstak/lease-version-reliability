import numpy as np
import pandas as pd
import structlog

from lease_version_reliability.common.file_io import download_models, read_model
from lease_version_reliability.config.settings import settings
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


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """"""
    # training data (masters with >3 versions within it)
    reliable_data = get_reliable_data()

    # all version data needed to export a reliability score
    all_data = get_all_data()

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
    """"""
    if download:
        download_models()
    model_dict = read_model(settings.TRAIN_CONFIG.MODEL_FILENAME)

    df, df_all = load_data()
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

    logger.debug(submitter_df.head())
    logger.debug(version_reliability_df.head())
