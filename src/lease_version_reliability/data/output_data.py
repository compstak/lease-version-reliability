from typing import Any

import pandas as pd
import structlog

from lease_version_reliability.config.settings import settings

logger = structlog.get_logger()


def get_submitter_reliability(
    df: pd.DataFrame,
    y_cols: Any,
    model_dict: dict[Any, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate reliability score for every submitter
    """
    for col in df:
        if ("logo" in col) and ("submitter" not in col):
            df[col] = 0
    df["n_support"] = df.submitter_person_id.value_counts()[
        df["submitter_person_id"]
    ].to_list()

    df = df.drop_duplicates(
        subset="submitter_person_id",
    )
    anal_df = df[["submitter_person_id", "n_support"]]
    reliability_cols = []
    for x in df:
        logger.info(x)

    for col in y_cols:
        clf = model_dict[col]

        X = df[clf.feature_names_in_]
        prob = clf.predict_proba(X)[:, 1]
        reliability_col = col.replace("label", "reliability")
        anal_df[reliability_col] = prob
        reliability_cols.append(reliability_col)

    cols_to_average = [
        f"{x}_reliability" for x in settings.GENERAL_RELIABILITY_ATTRIBUTES
    ]

    anal_df["general_reliability"] = anal_df[cols_to_average].mean(axis=1)
    anal_df = anal_df.sort_values(
        by=["general_reliability", "n_support"],
        ascending=False,
    ).reset_index(drop=True)

    return anal_df


def get_version_reliability(
    data: pd.DataFrame,
    attributes: Any,
    y_cols: Any,
    model_dict: dict[Any, Any],
) -> pd.DataFrame:
    """
    Calculate reliability score for every version
    """
    val_df = pd.DataFrame()
    val_df["comp_data_id_version"] = data["comp_data_id_version"]
    val_df["comp_data_id_master"] = data["comp_data_id_master"]

    for i in range(0, len(attributes)):
        model_name = y_cols[i]
        clf = model_dict[model_name]
        X = data[clf.feature_names_in_]
        probs = clf.predict_proba(X)[:, 1]
        val_df[f"{attributes[i]}_prob"] = probs

    cols_to_average = [
        f"{x}_prob" for x in settings.GENERAL_RELIABILITY_ATTRIBUTES
    ]

    val_df["general_reliability"] = val_df[cols_to_average].mean(axis=1)

    val_df = val_df.sort_values(by="comp_data_id_master")

    return val_df
