from datetime import datetime, timedelta
import typing

import dateutil.parser as parser
import jellyfish
import numpy as np
import pandas as pd
import structlog

from lease_version_reliability.config.settings import settings
from lease_version_reliability.data.database import (
    get_snowflake_connection,
    get_snowflake_ml_pipeline_connection,
)

logger = structlog.get_logger()


def read_file(path: str) -> str:
    """
    Read query file.
    """
    with open(f"{settings.QUERY_DIR}/{path}") as fd:
        query = fd.read()

    return query


def get_reliable_data() -> pd.DataFrame:
    query = read_file("reliable_data.sql")
    df = pd.read_sql(
        query,
        get_snowflake_connection(),
    )
    df.columns = [x.lower() for x in df.columns]
    return df


def get_all_data() -> pd.DataFrame:
    query = read_file("all_data.sql")
    df = pd.read_sql(
        query,
        get_snowflake_connection(),
    )
    df.columns = [x.lower() for x in df.columns]
    return df


def get_reliable_data_by_attribute() -> pd.DataFrame:
    query = read_file("reliable_data_by_attribute.sql")
    df = pd.read_sql(
        query,
        get_snowflake_connection(),
    )
    df.columns = [x.lower() for x in df.columns]
    return df


def label_strict_equality(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    return 0


def label_tenant_name(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    else:
        if jellyfish.jaro_winkler_similarity(subject, target) > 0.9:
            return 1
    return 0


def label_transaction_size(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject >= target * 0.95 and subject <= target * 1.05:
        return 1
    return 0


def label_execution_date(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    subject = str(subject)
    target = str(target)
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_commencement_date(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    subject = str(subject)
    target = str(target)
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_expiration_date(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    subject = str(subject)
    target = str(target)
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_lease_term(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject >= target * 0.92 and subject <= target * 1.08:
        return 1
    return 0


attribute_to_label_dict = {
    "tenant_name": label_tenant_name,
    "space_type_id": label_strict_equality,
    "transaction_size": label_transaction_size,
    "starting_rent": label_strict_equality,
    "execution_date": label_execution_date,
    "commencement_date": label_commencement_date,
    "lease_term": label_lease_term,
    "expiration_date": label_expiration_date,
    "work_value": label_strict_equality,
    "free_months": label_strict_equality,
    "transaction_type_id": label_strict_equality,
    "rent_bumps_percent_bumps": label_strict_equality,
    "rent_bumps_dollar_bumps": label_strict_equality,
    "lease_type_id": label_strict_equality,
}


def get_labels(data: pd.DataFrame, attributes: typing.Any) -> pd.DataFrame:
    for att in attributes:
        print(f"Calculating Labels: {att}")
        data[att + "_filled"] = np.where(
            (pd.notnull(data[att + "_version"])),
            1,
            0,
        )

        data[att + "_label"] = data.apply(
            lambda x: attribute_to_label_dict[att](
                x[att + "_version"],
                x[att + "_master"],
            ),
            axis=1,
        )

    return data


def write_submitter_df_snowflake(
    df: pd.DataFrame,
    schema: str,
    table: str,
) -> None:
    """
    Inserts submitter-reliability table into Snowflake
    """
    if not (df.empty):
        logger.info(f"Processing {len(df)}")
        conn = get_snowflake_ml_pipeline_connection()
        values = list(
            zip(
                df["submitter_person_id"].tolist(),
                df["tenant_name_reliability"],
                df["space_type_id_reliability"],
                df["transaction_size_reliability"].tolist(),
                df["starting_rent_reliability"].tolist(),
                df["execution_date_reliability"].tolist(),
                df["commencement_date_reliability"].tolist(),
                df["lease_term_reliability"].tolist(),
                df["expiration_date_reliability"].tolist(),
                df["work_value_reliability"].tolist(),
                df["free_months_reliability"].tolist(),
                df["transaction_type_id_reliability"].tolist(),
                df["rent_bumps_percent_bumps_reliability"].tolist(),
                df["rent_bumps_dollar_bumps_reliability"].tolist(),
                df["lease_type_id_reliability"].tolist(),
                df["general_reliability"].tolist(),
                [datetime.now() for _ in range(len(df))],
            ),
        )

        try:
            cur = conn.cursor()
            temp_table_query = read_file("submitter_df.sql").format(
                schema,
            )
            insert_query = read_file("insert_submitter_df.sql").format(
                schema,
            )

            merge_query = read_file("merge_submitter_df.sql").format(
                schema=schema,
                table=table,
            )

            cur.execute(temp_table_query)
            cur.executemany(insert_query, values)
            cur.execute(merge_query)

        except Exception as e:
            logger.error(e)

        finally:
            cur.close()
            conn.commit()
            logger.info("done")


def write_version_realiability_df_snowflake(
    df: pd.DataFrame,
    schema: str,
    table: str,
) -> None:
    """
    Inserts version-reliability table into Snowflake
    """
    if not (df.empty):
        logger.info(f"Processing {len(df)}")
        conn = get_snowflake_ml_pipeline_connection()
        values = list(
            zip(
                df["comp_data_id_version"].tolist(),
                df["tenant_name_prob"].tolist(),
                df["space_type_id_prob"].tolist(),
                df["transaction_size_prob"].tolist(),
                df["starting_rent_prob"].tolist(),
                df["execution_date_prob"].tolist(),
                df["commencement_date_prob"].tolist(),
                df["lease_term_prob"].tolist(),
                df["expiration_date_prob"].tolist(),
                df["work_value_prob"].tolist(),
                df["free_months_prob"].tolist(),
                df["transaction_type_id_prob"].tolist(),
                df["rent_bumps_percent_bumps_prob"].tolist(),
                df["rent_bumps_dollar_bumps_prob"].tolist(),
                df["lease_type_id_prob"].tolist(),
                [datetime.now() for _ in range(len(df))],
            ),
        )

        try:
            cur = conn.cursor()
            temp_table_query = read_file("version_df.sql").format(
                schema,
            )
            insert_query = read_file("insert_version_df.sql").format(
                schema,
            )

            merge_query = read_file("merge_version_df.sql").format(
                schema=schema,
                table=table,
            )

            cur.execute(temp_table_query)
            cur.executemany(insert_query, values)
            cur.execute(merge_query)

        except Exception as e:
            logger.error(e)

        finally:
            cur.close()
            conn.commit()
            logger.info("done")


def get_column_names(attributes: typing.Any) -> typing.Any:
    correct = []
    filled = []
    label = []
    for att in attributes:
        correct.append(f"{att}_correct")
        filled.append(f"{att}_filled")
        label.append(f"{att}_label")
    return correct, filled, label


def get_split_columns(
    columns: typing.Any,
) -> typing.Any:
    X_cols = []
    y_cols = []
    for col in columns:
        if col.endswith("count") or col.endswith("rate"):
            X_cols.append(col)
        elif col.endswith("label"):
            y_cols.append(col)

    return X_cols, y_cols
