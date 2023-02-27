from typing import Any

import numpy as np
import pandas as pd


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

    for col in label:
        temp = data.copy()
        temp[col] = data[col].replace(-1, 0)
        s_correct = (
            temp.groupby(name)[col]
            .sum()
            .rename(f"{col.replace('label', 'correct')}_{name}")
        )

        df_metrics = df_metrics.merge(
            right=s_correct,
            how="inner",
            left_on=name,
            right_index=True,
        )

        temp = data.copy()
        temp[col] = data[col].replace([-1, 0], [1, 1])
        s_total = (
            temp.groupby(name)[col]
            .sum()
            .rename(f"{col.replace('label', 'total')}_{name}")
        )
        df_metrics = df_metrics.merge(
            right=s_total,
            how="inner",
            left_on=name,
            right_index=True,
        )

    for col in fill:
        temp = data.copy()
        temp[col] = data[col].replace([-1, 0, 1], [0, 1, 1])

        s_filled = temp.groupby(name)[col].sum().rename(f"{col}_{name}")
        df_metrics = df_metrics.merge(
            right=s_filled,
            how="inner",
            left_on=name,
            right_index=True,
        )

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
) -> pd.DataFrame:
    """
    Function to merge data with features by name (submitter or brokerage logo)
    """
    df = data.merge(agg_data, how=how)

    for c in correct:
        replace_total = c.replace("correct", "total")
        replace_label = c.replace("correct", "label")

        df[f"{c}_{name}_hist"] = df[f"{c}_{name}"] - df[f"{replace_label}"]
        df[f"{replace_total}_{name}_hist"] = df[f"{replace_total}_{name}"] - 1

    for f in filled:
        df[f"{f}_{name}_hist"] = df[f"{f}_{name}"] - df[f"{f}"]

    return df.fillna(0)


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

    return df
