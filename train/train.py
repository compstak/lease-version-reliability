import asyncio
import pickle

import structlog

from train.common.file_io import upload_models
from train.common.logging import initialize_logging
from train.config.settings import settings
from train.data.database_io import get_labels, get_reliable_data
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

    attributes = settings.ATTRIBUTES

    col_names_correct, col_names_filled, col_names_label = get_column_names(
        attributes,
    )

    logger.info("Data Labels - Reliable Data")
    data = get_labels(reliable_data, attributes)

    logger.info("Feature Engineering - Reliable Data")
    df = feature_engineering(
        data,
        col_names_label,
        col_names_filled,
        col_names_correct,
        attributes,
    )

    logger.info("Model Training")
    x_cols, y_cols = get_split_columns(df.columns)
    model_dict = train_multioutput_classifiers(df, x_cols, y_cols)

    # upload classifier to S3
    with open(f"{settings.MODEL_DIR}/{settings.MODEL_NAME}", "wb") as handle:
        pickle.dump(model_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Uploading lease-reliability classifiers to S3")
    upload_models()


if __name__ == "__main__":
    asyncio.run(main())
