import datetime

import structlog

from batch.config.settings import settings
from batch.data.database import get_snowflake_ml_pipeline_connection

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
                [datetime.datetime.now() for _ in range(len(df))],
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


def write_version_realiability_df_snowflake(df, schema, table):
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
                [datetime.datetime.now() for _ in range(len(df))],
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


def get_column_names(attributes):
    correct = []
    filled = []
    label = []
    for att in attributes:
        correct.append(f"{att}_correct")
        filled.append(f"{att}_filled")
        label.append(f"{att}_label")
    return correct, filled, label


def get_split_columns(columns):
    X_cols = []
    y_cols = []
    for col in columns:
        if col.endswith("count") or col.endswith("rate"):
            X_cols.append(col)
        elif col.endswith("label"):
            y_cols.append(col)

    return X_cols, y_cols
