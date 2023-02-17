import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
import structlog

from lease_version_reliability.common.file_io import save_model, upload_models
from lease_version_reliability.config.settings import settings
from lease_version_reliability.data.database import CompstakServicesMySQL
from lease_version_reliability.data.database import cs_mysql_instance as mysql
from lease_version_reliability.data.database_io import (
    get_labels,
    get_reliable_data,
    get_reliable_data_by_attribute,
)
from lease_version_reliability.features.build_features import (
    feature_engineering,
)

logger = structlog.get_logger()


def get_column_names(
    attributes: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """
    Return correct, filled, and labeled column names for every attribute
    """
    correct = []
    filled = []
    label = []
    for att in attributes:
        correct.append(f"{att}_correct")
        filled.append(f"{att}_filled")
        label.append(f"{att}_label")
    return (correct, filled, label)


def get_split_columns(columns: list[str]) -> tuple[list[str], list[str]]:
    """
    Split into input and target columns
    """
    X_cols = []
    y_cols = []
    for col in columns:
        if col.endswith("count") or col.endswith("rate"):
            X_cols.append(col)
        elif col.endswith("label"):
            y_cols.append(col)

    return X_cols, y_cols


async def train_multioutput_classifiers(
    db: CompstakServicesMySQL,
    df: pd.DataFrame,
    X_cols: list[str],
    y_cols: list[str],
) -> dict[str, RandomForestClassifier()]:
    """
    Return training model dictionary
    """
    model_dict = {}
    logger.info("Get reliable versions by attribute")
    df_reliable_attributes = await get_reliable_data_by_attribute(db)
    for col in y_cols:
        logger.info(f"Attribute: {col}")
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
        class_weight = "balanced_subsample"
        clf = RandomForestClassifier(
            class_weight=class_weight,
            random_state=1,
            n_jobs=-1,
        )
        clf.fit(x_train, y_train)
        test_preds = clf.predict(x_test)
        acc = accuracy_score(y_test, test_preds)
        f1 = f1_score(y_test, test_preds)

        logger.info(attribute)
        logger.info(f"Training Data Size: {len(x_train)}")
        logger.info(f"{col} - Accuracy : {acc}")
        logger.info(f"{col} - F1 : {f1}")
        logger.info(y.value_counts())
        logger.info("----------------------------------")

        model_dict[col] = clf

    return model_dict


async def train_model(upload: bool) -> None:
    """
    Train model and upload into S3
    """
    logger.info("Connecting to MySQL")
    await mysql.connect()

    logger.info("Get Reliable Data")
    # training data (masters with >3 versions within it)
    reliable_data = await get_reliable_data(mysql)

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
    model_dict = await train_multioutput_classifiers(mysql, df, x_cols, y_cols)

    logger.info("Saving Models")
    save_model(model_dict, settings.TRAIN_CONFIG.MODEL_FILENAME)

    if upload:
        upload_models()

    logger.info("Disconnecting to MySQL")
    await mysql.disconnect()
