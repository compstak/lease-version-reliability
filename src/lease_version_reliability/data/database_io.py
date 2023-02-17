from datetime import datetime, timedelta
import typing

import dateutil.parser as parser
import jellyfish
import numpy as np
import pandas as pd
import structlog

from lease_version_reliability.config.settings import settings
from lease_version_reliability.data.database import (
    CompstakServicesMySQL,
    get_snowflake_connection,
)

logger = structlog.get_logger()


def read_file(path: str) -> str:
    """
    Read query file.
    """
    with open(f"{settings.QUERY_DIR}/{path}") as fd:
        query = fd.read()

    return query


async def get_logo_df(data: pd.DataFrame) -> pd.DataFrame:
    """
    Return logorithm data from Snowflake
    """
    snowflake_conn = get_snowflake_connection()

    query = read_file("submission_logo.sql")
    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    logo_data = cursor.fetch_pandas_all()
    cursor.close()

    logo_data.columns = [x.lower() for x in logo_data.columns]
    data = data.merge(
        right=logo_data,
        how="left",
        left_on="specialk_id",
        right_on="submission_id",
    )
    data = data.drop(["submission_id"], axis=1)
    return data


async def get_reliable_data(db: CompstakServicesMySQL) -> pd.DataFrame:
    """
    Return reliable data (more than 3 submitted versions) from MySQL
    """
    query = read_file("reliable_data.sql")
    data = [dict(item) for item in await db.fetch_all(query)]
    data = pd.DataFrame(data)

    data = await get_logo_df(data)

    return data


async def get_all_data(db: CompstakServicesMySQL) -> pd.DataFrame:
    """
    Return all versions from MySQL
    """
    query = read_file("all_data.sql")
    data = [dict(item) for item in await db.fetch_all(query)]
    data = pd.DataFrame(data)

    data = await get_logo_df(data)

    return data


async def get_reliable_data_by_attribute(
    db: CompstakServicesMySQL,
) -> pd.DataFrame:
    """
    Get count of each attribute in reliable data
    """
    query = read_file("reliable_data_by_attribute.sql")
    data = [dict(item) for item in await db.fetch_all(query)]
    data = pd.DataFrame(data)
    return data


def label_strict_equality(
    subject: str,
    target: str,
) -> float:
    """
    Replace attribute columns with indicator values
    Based on strict equality for masters and versions
    """
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    return 0


def label_tenant_name(
    subject: str,
    target: str,
) -> float:
    """
    Replace tenant_name attribute columns with indicator values
    Based on string similarity between masters and versions
    """
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    else:
        if jellyfish.jaro_winkler_similarity(subject, target) > 0.9:
            return 1
    return 0


def label_transaction_size(
    subject: float,
    target: float,
) -> float:
    """
    Replace transaction_size attribute column with indicator values
    Given size threshold for masters and versions
    """
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject >= target * 0.95 and subject <= target * 1.05:
        return 1
    return 0


def label_execution_date(
    subject: str,
    target: str,
) -> float:
    """
    Replace execution_date attribute column with indicator values
    Given date threshold for masters and versions
    """
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
    subject: str,
    target: str,
) -> float:
    """
    Replace commencement_date attribute column with indicator values
    Given date threshold for masters and versions
    """
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
    """
    Replace expiration_date attribute column with indicator values
    Given date threshold for masters and versions
    """
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
    subject: float,
    target: float,
) -> float:
    """
    Replace lease_term attribute column with indicator values
    Given term threshold for masters and versions
    """
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


def get_labels(data: pd.DataFrame, attributes: list[str]) -> pd.DataFrame:
    """
    Populate each attribute column based on label calculation rules
    """
    for att in attributes:
        logger.info(f"Calculating Labels: {att}")
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
    Insert submitter-reliability table into Snowflake
    """
    if not (df.empty):
        logger.info(f"Processing {len(df)}")
        conn = get_snowflake_connection()
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
    Insert version-reliability table into Snowflake
    """
    if not (df.empty):
        logger.info(f"Processing {len(df)}")
        conn = get_snowflake_connection()
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
    """
    Retrun correct, filled, and label columns for each attribute
    """
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
