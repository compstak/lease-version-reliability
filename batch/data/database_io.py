import datetime

import pandas as pd
import structlog

from batch.config.settings import settings
from batch.data.database import get_snowflake_connection

# import itertools
# import time
# import typing


logger = structlog.get_logger()


def read_file(path: str) -> str:
    """
    Read query file.
    """
    with open(f"{settings.QUERY_DIR}/{path}") as fd:
        query = fd.read()

    return query


def write_submitter_df_snowflake(df, schema, table):
    """
    Inserts submitter-reliability table into Snowflake
    """
    if len(df) > 1:
        logger.info("Writing into Snowflake")
        conn = get_snowflake_connection()
        df["date_created"] = datetime.datetime.now()
        values = list(df.itertuples(index=False, name=None))

        try:
            cur = conn.cursor()
            temp_table_query = read_file("submitter_df.sql").format(
                schema,
            )
            insert_query = read_file("insert_submitter_df.sql ").format(
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


def write_version_realiability_df_snowflake(df, schema, table):
    """
    Inserts version-reliability table into Snowflake
    """
    if len(df) > 1:
        logger.info("Writing into Snowflake")
        conn = get_snowflake_connection()
        df["date_created"] = datetime.datetime.now()
        values = list(df.itertuples(index=False, name=None))

        try:
            cur = conn.cursor()
            temp_table_query = read_file("version_df.sql").format(
                schema,
            )
            insert_query = read_file("insert_version_df.sql ").format(
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


def get_submitter_info():
    query = read_file("submitter_info.sql")
    df = pd.read_sql(
        query,
        get_snowflake_connection(),
    )
    df.columns = [x.lower() for x in df.columns]

    return df


def get_split_columns(columns):
    X_cols = []
    y_cols = []
    for col in columns:
        if col.endswith("count") or col.endswith("rate"):
            X_cols.append(col)
        elif col.endswith("label"):
            y_cols.append(col)

    return X_cols, y_cols
