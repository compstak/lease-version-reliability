import gc
from typing import Any

import numpy as np
import pandas as pd
import structlog

logger = structlog.get_logger()


def get_features_by_entity(
    data: pd.DataFrame,
    name: str,
    label: list[Any],
    fill: list[Any],
) -> pd.DataFrame:
    """
    Processes all data, grouped by name (submiitter or brokerage logo).
    Calculated correct submissions, total submissions, and filled submissions.
    """
    df_metrics = pd.DataFrame()
    ids = list(data[name].unique())
    if np.NaN in ids:
        ids.remove(np.NaN)
    df_metrics[name] = ids

    logger.info("Get Correct Count")
    for col in label:
        temp1 = data[[name, col]].copy()
        temp1[col] = data[col].replace(-1, 0)

        s_correct = (
            temp1.groupby(name)[col]
            .sum()
            .rename(f"{col.replace('label', 'correct')}_{name}")
        ).astype(int)

        s_correct_dict = s_correct.to_dict()
        df_metrics[f"{col.replace('label', 'correct')}_{name}"] = (
            df_metrics[col].map(s_correct_dict).fillna(0)
        )
        # df_metrics = df_metrics.merge(
        #     right=s_correct,
        #     how="inner",
        #     left_on=name,
        #     right_index=True,
        # )

        del s_correct
        del temp1
        del s_correct_dict
        gc.collect()

        temp2 = data[[name, col]].copy()
        temp2[col] = data[col].replace([-1, 0], [1, 1])

        s_total = (
            temp2.groupby(name)[col]
            .sum()
            .rename(f"{col.replace('label', 'total')}_{name}")
        ).astype(int)

        s_total_dict = s_total.to_dict()
        df_metrics[f"{col.replace('label', 'total')}_{name}"] = (
            df_metrics[col].map(s_total_dict).fillna(0)
        )

        # df_metrics = df_metrics.merge(
        #     right=s_total,
        #     how="inner",
        #     left_on=name,
        #     right_index=True,
        # )

        del s_total
        del temp2
        del s_total_dict
        gc.collect()

    for col in fill:
        temp3 = data[[name, col]].copy()
        temp3[col] = temp3[col].replace([-1, 0, 1], [0, 1, 1])
        s_filled = (
            temp3.groupby(name)[col].sum().rename(f"{col}_{name}").astype(int)
        )

        s_filled_dict = s_filled.to_dict()
        df_metrics[f"{col}_{name}"] = df_metrics[col].map(s_filled).fillna(0)

        # df_metrics = df_metrics.merge(
        #     right=s_filled,
        #     how="inner",
        #     left_on=name,
        #     right_index=True,
        # )

        del s_filled
        del temp3
        del s_filled_dict
        gc.collect()

    cols = list(df_metrics.columns)
    cols.remove(name)

    df_metrics = df_metrics[[name] + sorted(cols)]

    return df_metrics


def combine_features(
    data: pd.DataFrame,
    agg_data: pd.DataFrame,
    name: str,
    how: str,
    correct: list[Any],
    filled: list[Any],
    left_on: str,
    right_on: str,
) -> pd.DataFrame:
    """
    Function to merge data with features by name (submitter or brokerage logo)

    """

    logger.info("Merge Start")
    for col in agg_data:
        if col != name:
            temp_dict = dict(zip(agg_data[name], agg_data[col]))
            data[col] = data[name].map(temp_dict).fillna(0)
    del temp_dict
    gc.collect()
    logger.info("Merge End")

    # df = data.merge(
    #     agg_data,
    #     how=how,
    #     left_on=left_on,
    #     right_on=right_on,
    # )
    #
    logger.info("Start with correct")
    cols_added = []
    for c in correct:
        replace_total = c.replace("correct", "total")
        replace_label = c.replace("correct", "label")

        col_label = f"{c}_{name}_hist"
        col_total = f"{replace_total}_{name}_hist"

        data[col_label] = (
            data[f"{c}_{name}"] - data[f"{replace_label}"]
        ).astype(int)
        data[col_total] = (data[f"{replace_total}_{name}"] - 1).astype(int)

        cols_added.append(col_label)
        cols_added.append(col_total)

    logger.info("Start with filled")
    for f in filled:
        col_filled = f"{f}_{name}_hist"
        data[col_filled] = (data[f"{f}_{name}"] - data[f"{f}"]).astype(int)
        cols_added.append(col_filled)

    return data


def get_rate_features(
    data: pd.DataFrame,
    attributes: dict[Any, Any],
) -> pd.DataFrame:
    """
    Function to take count features (correct submissions, total submissions,
    and filled submissions) and convert them into a rate.
    i.e. Fill rate = number submissions filled / total number of submissions
    """
    df = data
    for att in attributes:
        df[f"{att}_submitter_correct_rate"] = np.where(
            df[f"{att}_filled_submitter_person_id"] > 0,
            df[f"{att}_correct_submitter_person_id"]
            / df[f"{att}_filled_submitter_person_id"],
            0,
        )
        df[f"{att}_submitter_fill_rate"] = np.where(
            df[f"{att}_total_submitter_person_id"] > 0,
            df[f"{att}_filled_submitter_person_id"]
            / df[f"{att}_total_submitter_person_id"],
            0,
        )

        df[f"{att}_logo_correct_rate"] = np.where(
            df[f"{att}_filled_logo"] > 0,
            df[f"{att}_correct_logo"] / df[f"{att}_filled_logo"],
            0,
        )
        df[f"{att}_logo_fill_rate"] = np.where(
            df[f"{att}_total_logo"] > 0,
            df[f"{att}_filled_logo"] / df[f"{att}_total_logo"],
            0,
        )

    return df


def feature_engineering(
    data: pd.DataFrame,
    col_names_label: Any,
    col_names_filled: Any,
    col_names_correct: Any,
    attributes: Any,
) -> pd.DataFrame:
    """
    Get combined submitter and brokerage logo based features by version.
    """
    logger.info("Get Submitter Features")
    df_submitter_features = get_features_by_entity(
        data,
        "submitter_person_id",
        col_names_label,
        col_names_filled,
    )

    logger.info("Combine Submitter Features")
    df = combine_features(
        data,
        df_submitter_features,
        "submitter_person_id",
        "inner",
        col_names_correct,
        col_names_filled,
        "submitter_person_id",
        "submitter_person_id",
    )

    del data
    del (df_submitter_features,)
    gc.collect()

    logger.info("Done combining submitter features")

    logger.info("Get brokerage features")
    df_logo_features = get_features_by_entity(
        df,
        "logo",
        col_names_label,
        col_names_filled,
    )

    logger.info("Combine brokerage features")
    df = combine_features(
        df,
        df_logo_features,
        "logo",
        "left",
        col_names_correct,
        col_names_filled,
        "logo",
        "logo",
    )
    logger.info("Done combining brokerage features")

    del df_logo_features
    gc.collect()

    logger.info("Converting features into rate")
    df = get_rate_features(df, attributes)

    return df
