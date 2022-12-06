import asyncio

import structlog

from train.common.logging import initialize_logging
from train.config.settings import settings
from train.data.dataset import (
    get_all_data,
    get_labels,
    get_reliable_data,
    get_submitter_info,
)
from train.data.output_data import (
    get_submitter_reliability,
    get_version_reliability,
)
from train.features.features import feature_engineering
from train.model.model import (
    get_column_names,
    get_split_columns,
    train_multioutput_classifiers,
)


async def main() -> None:
    initialize_logging(settings.ENV)
    logger = structlog.get_logger()

    logger.info("<<< Getting Data >>>")
    # training data (masters with >3 versions within it)
    reliable_data = get_reliable_data()

    # all version data needed to export a reliability score
    all_data = get_all_data()

    # submitter name for display purposes when exporting validation data
    submitter_name = get_submitter_info()

    print(len(reliable_data))
    print(len(all_data))

    attributes = [
        "tenant_name",
        "space_type_id",
        "transaction_size",
        "starting_rent",
        "execution_date",
        "commencement_date",
        "lease_term",
        "expiration_date",
        "work_value",
        "free_months",
        "transaction_type_id",
        "rent_bumps_percent_bumps",
        "rent_bumps_dollar_bumps",
        "lease_type_id",
    ]

    col_names_correct, col_names_filled, col_names_label = get_column_names(
        attributes,
    )

    logger.info("Creating Data Labels")
    data = get_labels(reliable_data, attributes)
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

    logger.info("Model Training")
    x_cols, y_cols = get_split_columns(df.columns)
    model_dict = train_multioutput_classifiers(df, x_cols, y_cols)

    logger.info("Exporting Submitter Results")
    submitter_df, submitter_info = await get_submitter_reliability(
        df,
        x_cols,
        y_cols,
        model_dict,
        submitter_name,
    )
    submitter_df.to_csv("data/processed/submitter_reliability.csv", index=False)

    logger.info("Exporting Version Results")
    version_reliability_df = get_version_reliability(
        df_all,
        attributes,
        x_cols,
        y_cols,
        model_dict,
    )
    version_reliability_df.to_csv(
        "data/processed/version_reliability.csv",
        index=False,
    )


if __name__ == "__main__":
    asyncio.run(main())
