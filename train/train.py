import asyncio

import structlog

from train.common.logging import initialize_logging
from train.config.settings import settings
from train.data.database import (  # get_snowflake_connection,
    cs_mysql_instance as mysql,
)
from train.data.dataset import (  # get_reliable_data,
    get_Dataset,
    get_labels,
    get_submitter_info,
)
from train.data.output_data import (
    get_submitter_reliability,
    get_version_reliability,
)
from train.features.features import (
    combine_features,
    get_features_by_entity,
    get_rate_features,
)
from train.model.model import (
    get_column_names,
    get_split_columns,
    train_multioutput_classifiers,
)


async def main() -> None:

    initialize_logging(settings.ENV)
    logger = structlog.get_logger()

    logger.info("<<< Getting Data >>>")
    await mysql.connect()
    # # snowflake =  get_snowflake_connection()
    # df = get_reliable_data()
    original_df = await get_Dataset(mysql)
    submitter_name = await get_submitter_info(mysql)

    holdout_df = original_df.tail(1000)
    data_df = original_df.head(len(original_df) - 1000)

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
    data = get_labels(data_df, attributes)

    logger.info("Feature Engineering")
    df_submitter_features = get_features_by_entity(
        data,
        "submitter_person_id",
        col_names_label,
        col_names_filled,
    )
    df_logo_features = get_features_by_entity(
        data,
        "logo",
        col_names_label,
        col_names_filled,
    )
    df = combine_features(
        data,
        df_submitter_features,
        "submitter_person_id",
        "inner",
        col_names_correct,
        col_names_filled,
    )
    df = combine_features(
        df,
        df_logo_features,
        "logo",
        "left",
        col_names_correct,
        col_names_filled,
    )
    df = get_rate_features(df, attributes)

    logger.info("Model Training")
    x_cols, y_cols = get_split_columns(df.columns)
    model_dict = train_multioutput_classifiers(df, x_cols, y_cols)

    logger.info("Exporting Submitter Results")
    anal_df, submitter_info = await get_submitter_reliability(
        df,
        x_cols,
        y_cols,
        model_dict,
        submitter_name,
    )
    anal_df.to_csv("data/processed/submitter_reliability.csv", index=False)

    logger.info("Exporting Version Results")

    # select which df of versions to evaluate reliability
    if settings.ENV != "local":
        versions_to_evaluate = holdout_df.copy()
    else:
        versions_to_evaluate = df.copy()

    version_reliability_df = get_version_reliability(
        versions_to_evaluate,
        attributes,
        x_cols,
        y_cols,
        model_dict,
    )
    version_reliability_df.to_csv(
        "data/processed/version_reliability.csv",
        index=False,
    )

    await mysql.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
