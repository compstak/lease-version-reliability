from typing import Any

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
import structlog

from lease_version_reliability.common.file_io import save_model, upload_models
from lease_version_reliability.config.settings import settings
from lease_version_reliability.data.database_io import (
    get_labels,
    get_reliable_data,
    get_reliable_data_by_attribute,
)
from lease_version_reliability.features.build_features import (
    feature_engineering,
)

logger = structlog.get_logger()


def get_column_names(attributes: Any) -> Any:
    correct = []
    filled = []
    label = []
    for att in attributes:
        correct.append(f"{att}_correct")
        filled.append(f"{att}_filled")
        label.append(f"{att}_label")
    return correct, filled, label


def get_split_columns(columns: Any) -> Any:
    X_cols = []
    y_cols = []
    for col in columns:
        if col.endswith("count") or col.endswith("rate"):
            X_cols.append(col)
        elif col.endswith("label"):
            y_cols.append(col)

    return X_cols, y_cols


def train_multioutput_classifiers(
    df: pd.DataFrame,
    X_cols: list[Any],
    y_cols: list[Any],
) -> Any:
    model_dict = {}
    df_reliable_attributes = get_reliable_data_by_attribute()
    for col in y_cols:
        # Remove null attributes
        attribute = col.replace("_label", "")
        ids_to_keep = df_reliable_attributes[
            df_reliable_attributes[f"{attribute}_count"] >= 3
        ]["comp_version_id"].tolist()

        temp = df[(df["id"].isin(ids_to_keep)) & (df[col] != -1)]
        X = temp[X_cols]
        y = temp[col]
        x_train, x_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=10,
        )
        class_weight = "balanced"
        clf = RandomForestClassifier(
            class_weight=class_weight,
            random_state=1,
            n_jobs=-1,
        )
        clf.fit(x_train, y_train)
        test_preds = clf.predict(x_test)
        acc = accuracy_score(y_test, test_preds)
        f1 = f1_score(y_test, test_preds)

        logger.debug(attribute)
        logger.debug(f"Training Data Size: {len(x_train)}")
        logger.debug(f"{col} - Accuracy : {acc}")
        logger.debug(f"{col} - F1 : {f1}")
        logger.debug(y.value_counts())
        logger.debug("----------------------------------")

        model_dict[col] = clf

    return model_dict


async def train_model(upload: bool) -> None:
    """"""
    logger.info("Lease version reliability model training start.")
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

    save_model(model_dict, settings.TRAIN_CONFIG.MODEL_FILENAME)

    if upload:
        upload_models()

    logger.info("Lease version reliability model training end.")
