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

    for col in label:
        logger.info(f"Get Correct Count: {col}")
        s_correct = data[data[col].isin([1])].groupby(name)[col].count()
        if df_metrics.empty:
            df_metrics[name] = s_correct.index.tolist()
        s_correct_dict = s_correct.to_dict()
        df_metrics[f"{col.replace('label', 'correct')}_{name}"] = (
            df_metrics[name].map(s_correct_dict).fillna(0)
        )

        del s_correct
        del s_correct_dict
        gc.collect()

        logger.info(f"Get Total Count: {col}")
        s_total = data.groupby(name)[col].count()

        s_total_dict = s_total.to_dict()
        df_metrics[f"{col.replace('label', 'total')}_{name}"] = (
            df_metrics[name].map(s_total_dict).fillna(0)
        )

        del s_total
        del s_total_dict
        gc.collect()

    for col in fill:
        logger.info(f"Get Correct Count: {col}")
        s_filled = data[data[col].isin([0, 1])].groupby(name)[col].count()
        s_filled_dict = s_filled.to_dict()
        df_metrics[f"{col}_{name}"] = df_metrics[name].map(s_filled).fillna(0)

        del s_filled
        # del temp3
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

        data[col_label] = data[f"{c}_{name}"] - data[f"{replace_label}"]
        data[col_total] = data[f"{replace_total}_{name}"] - 1

        cols_added.append(col_label)
        cols_added.append(col_total)

    logger.info("Start with filled")
    for f in filled:
        col_filled = f"{f}_{name}_hist"
        data[col_filled] = data[f"{f}_{name}"] - data[f"{f}"]
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
    new_cols = []
    for att in attributes:
        new_cols.append(f"{att}_submitter_correct_rate")
        new_cols.append(f"{att}_submitter_fill_rate")
        new_cols.append(f"{att}_logo_correct_rate")
        new_cols.append(f"{att}_logo_fill_rate")

    df = pd.DataFrame(columns=new_cols)

    for att in attributes:
        df[f"{att}_submitter_correct_rate"] = np.where(
            data[f"{att}_filled_submitter_person_id"] > 0,
            data[f"{att}_correct_submitter_person_id"]
            / data[f"{att}_filled_submitter_person_id"],
            0,
        ).astype(float)
        df[f"{att}_submitter_fill_rate"] = np.where(
            data[f"{att}_total_submitter_person_id"] > 0,
            data[f"{att}_filled_submitter_person_id"]
            / data[f"{att}_total_submitter_person_id"],
            0,
        ).astype(float)

        df[f"{att}_logo_correct_rate"] = np.where(
            data[f"{att}_filled_logo"] > 0,
            data[f"{att}_correct_logo"] / data[f"{att}_filled_logo"],
            0,
        ).astype(float)
        df[f"{att}_logo_fill_rate"] = np.where(
            data[f"{att}_total_logo"] > 0,
            data[f"{att}_filled_logo"] / data[f"{att}_total_logo"],
            0,
        ).astype(float)

    return pd.concat([data, df], axis=1)


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

    logger.info("Get Brokerage Features")
    df_logo_features = get_features_by_entity(
        data[~data.logo.isin([np.NaN, "others"])],
        "logo",
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
    del df_submitter_features
    gc.collect()

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
