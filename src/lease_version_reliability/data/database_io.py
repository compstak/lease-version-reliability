from datetime import timedelta
import gc
from importlib import resources
from typing import Any

import jellyfish
import numpy as np
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
import structlog

from lease_version_reliability.config.settings import settings
from lease_version_reliability.data.database import CompstakServicesMySQL
from lease_version_reliability.data.database import cs_mysql_instance as mysql
from lease_version_reliability.data.database import get_snowflake_connection

logger = structlog.get_logger()


def read_file(package: str, filename: str) -> str:
    """
    Read file.
    """
    with resources.open_text(package, filename) as fd:
        query = fd.read()

    return query


async def get_logo_df(data: pd.DataFrame) -> pd.DataFrame:
    """
    Return logorithm data from Snowflake
    """
    snowflake_conn = get_snowflake_connection()

    query = read_file(settings.SQL_QUERY, "submission_logo.sql")
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
    df = data.drop(["submission_id"], axis=1)
    del data
    gc.collect()
    return df


async def get_version_max_id(db: CompstakServicesMySQL) -> Any:
    """
    Retrun max id of comp_version table
    """

    query = read_file(settings.SQL_QUERY, "version_max_id.sql")

    return await db.fetch_val(query)


async def batch_reliable_data(
    db: CompstakServicesMySQL,
    min: int,
    max: int,
) -> pd.DataFrame:
    """
    Batch process reliable data (more than 3 submitted versions) from MySQL
    """
    query = read_file(settings.SQL_QUERY, "reliable_data.sql").format(
        min=min,
        max=max,
    )
    data = [dict(item) for item in await db.fetch_all(query)]
    return pd.DataFrame(data)


async def batch_all_data(
    db: CompstakServicesMySQL,
    min: int,
    max: int,
) -> pd.DataFrame:
    """
    Batch process all versions data from MySQL
    """
    query = read_file(settings.SQL_QUERY, "all_data.sql").format(
        min=min,
        max=max,
    )

    data = [dict(item) for item in await db.fetch_all(query)]
    return pd.DataFrame(data)


async def get_reliable_data_version_ids() -> list[int]:
    """
    Batch process all versions data from MySQL
    """
    query = read_file(settings.SQL_QUERY, "reliable_data_version_ids.sql")

    data = [dict(item) for item in await mysql.fetch_all(query)]
    return pd.DataFrame(data).id.tolist()


async def get_reliable_data() -> pd.DataFrame:
    """
    Return reliable data (more than 3 submitted versions) and logorithm data
    """
    id = await get_version_max_id(mysql)
    df = pd.DataFrame()
    counter = 1
    for i in range(0, id, settings.BATCH_CONFIG.BATCH_SIZE):
        if (counter == 1) or (counter % 10) == 0:
            logger.info(
                f"Processed {i + settings.BATCH_CONFIG.BATCH_SIZE}/{id}",
            )
        data = await batch_reliable_data(
            mysql,
            i,
            i + settings.BATCH_CONFIG.BATCH_SIZE,
        )
        df = pd.concat([df, data], ignore_index=True)
        counter += 1
        del data
        gc.collect()
    df = await get_logo_df(df)

    return df


async def get_all_data() -> pd.DataFrame:
    """
    Return all versions and logorithm data
    """

    id = await get_version_max_id(mysql)
    df = pd.DataFrame()
    counter = 1
    for i in range(0, id, settings.BATCH_CONFIG.BATCH_SIZE):
        if (counter == 1) or (counter % 10) == 0:
            logger.info(
                f"Processing {i + settings.BATCH_CONFIG.BATCH_SIZE}/{id}",
            )
        data = await batch_all_data(
            mysql,
            i,
            i + settings.BATCH_CONFIG.BATCH_SIZE,
        )
        df = pd.concat([df, data], ignore_index=True)
        del data
        gc.collect()

        counter += 1
    df = await get_logo_df(df)

    return df


async def get_reliable_data_by_attribute(
    db: CompstakServicesMySQL,
) -> pd.DataFrame:
    """
    Get count of each attribute in reliable data
    """
    query = read_file(settings.SQL_QUERY, "reliable_data_by_attribute.sql")
    data = [dict(item) for item in await db.fetch_all(query)]
    data = pd.DataFrame(data)
    return data


def label_strict_equality(
    data: pd.DataFrame,
    att: str,
) -> float:
    """
    Replace attribute columns with indicator values
    Based on strict equality for masters and versions
    """
    idx_null = np.where(
        (data[att + "_version"].isnull()) | (data[att + "_master"].isnull()),
    )[0]
    idx_equality = np.where(data[att + "_version"] == data[att + "_master"])[0]

    data[att + "_label"] = 0
    data.loc[idx_null, att + "_label"] = -1
    data.loc[idx_equality, att + "_label"] = 1

    return data


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
    data: pd.DataFrame,
    att: str,
) -> float:
    """
    Replace transaction_size attribute column with indicator values
    Given size threshold for masters and versions
    """
    idx_null = np.where(
        (data[att + "_version"].isnull()) | (data[att + "_master"].isnull()),
    )[0]
    idx_transaction_size = np.where(
        (data[att + "_version"] >= 0.95 * data[att + "_master"])
        & (data[att + "_version"] <= 1.05 * data[att + "_master"]),
    )[0]

    data[att + "_label"] = 0
    data.loc[idx_null, att + "_label"] = -1
    data.loc[idx_transaction_size, att + "_label"] = 1

    return data


def label_date(data, att):
    idx_null = np.where(
        (data[att + "_version"].isnull()) | (data[att + "_master"].isnull()),
    )[0]
    idx_date = np.where(
        (data[att + "_version"] <= data[att + "_master"] + timedelta(days=90))
        & (
            data[att + "_version"] >= data[att + "_master"] - timedelta(days=90)
        ),
    )[0]

    data[att + "_label"] = 0
    data.loc[idx_null, att + "_label"] = -1
    data.loc[idx_date, att + "_label"] = 1

    return data


def label_lease_term(
    data: pd.DataFrame,
    att: str,
) -> float:
    """
    Replace lease_term attribute column with indicator values
    Given term threshold for masters and versions
    """
    idx_null = np.where(
        (data[att + "_version"].isnull()) | (data[att + "_master"].isnull()),
    )[0]
    idx_equality = np.where(
        (data[att + "_version"] >= 0.92 * data[att + "_master"])
        & (data[att + "_version"] <= 1.08 * data[att + "_master"]),
    )[0]

    data[att + "_label"] = 0
    data.loc[idx_null, att + "_label"] = -1
    data.loc[idx_equality, att + "_label"] = 1

    return data


attribute_to_label_dict = {
    "tenant_name": label_tenant_name,
    "space_type_id": label_strict_equality,
    "transaction_size": label_transaction_size,
    "starting_rent": label_strict_equality,
    "execution_date": label_date,
    "commencement_date": label_date,
    "lease_term": label_lease_term,
    "expiration_date": label_date,
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

    drop_columns = []
    for att in attributes:
        logger.info(f"Calculating Labels: {att}")
        data[att + "_filled"] = np.where(
            (pd.notnull(data[att + "_version"])),
            1,
            0,
        )

        if att == "tenant_name":
            data[att + "_label"] = data.apply(
                lambda x: attribute_to_label_dict[att](
                    x[att + "_version"],
                    x[att + "_master"],
                ),
                axis=1,
            )
        else:
            data = attribute_to_label_dict[att](data, att)

        drop_columns.append(att + "_version")
        drop_columns.append(att + "_master")
    df = data.drop(drop_columns, axis=1)
    del data
    gc.collect()
    return df


def get_column_names(attributes: Any) -> Any:
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
    columns: Any,
) -> Any:
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


def modify_submitter_df(df: pd.DataFrame):
    """
    Change column names for submitter df
    to match column names in Snowflake
    """
    col = settings.ATTRIBUTES.copy()
    col_reliability = [s + "_reliability" for s in col]

    col.insert(0, "submitter_person_id")
    col_reliability.insert(0, "submitter_person_id")

    col.insert(len(col), "general_reliability")
    col_reliability.insert(len(col), "general_reliability")

    df = df[col_reliability]
    df = df.set_axis(col, axis=1)

    df["date_created"] = pd.Timestamp.now()
    df["date_created"] = df["date_created"].dt.strftime("%Y-%m-%d %X")
    df.columns = map(lambda x: str(x).upper(), df.columns)

    return df


def modify_version_df(df: pd.DataFrame):
    """
    Change column names for version df
    to match column names in Snowflake
    """
    col = settings.ATTRIBUTES.copy()
    col_reliability = [s + "_prob" for s in col]

    col.insert(0, "comp_data_id_version")
    col_reliability.insert(0, "comp_data_id_version")

    df = df[col_reliability]
    df = df.set_axis(col, axis=1)

    df["date_created"] = pd.Timestamp.now()
    df["date_created"] = df["date_created"].dt.strftime("%Y-%m-%d %X")
    df.columns = map(lambda x: str(x).upper(), df.columns)

    return df


def write_into_snowflake(
    df: pd.DataFrame,
    schema: str,
    table: str,
) -> None:
    """
    Insert table into Snowflake
    """
    if not (df.empty):
        logger.info(f"Processing {len(df)}")
        conn = get_snowflake_connection()
        engine = create_engine(
            f"snowflake://{settings.SNOWFLAKE_ACCOUNT}."
            f"{settings.SNOWFLAKE_REGION}."
            f"snowflakecomputing.com",
            creator=lambda: conn,
        )
    with engine.connect():
        df.to_sql(
            table,
            engine,
            schema=schema,
            index=False,
            if_exists="append",
            chunksize=settings.BATCH_CONFIG.BATCH_SIZE,
            method=pd_writer,
        )
