import typing

import snowflake.connector

from lease_version_reliability.config.settings import settings


def get_snowflake_connection() -> typing.Any:
    """
    Get Snowflake cursor
    """
    connection = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASS,
        account=settings.SNOWFLAKE_ACCOUNT,
        region=settings.SNOWFLAKE_REGION,
        warehouse=settings.SNOWFLAKE_WH,
        database=settings.SNOWFLAKE_DB,
        autocommit=False,
    )

    return connection


def get_snowflake_ml_pipeline_connection() -> typing.Any:
    """
    Get Snowflake cursor
    """
    connection = snowflake.connector.connect(
        user=settings.SNOWFLAKE_ML_USER,
        password=settings.SNOWFLAKE_ML_PASS,
        role=settings.SNOWFLAKE_ML_ROLE,
        account=settings.SNOWFLAKE_ML_ACCOUNT,
        region=settings.SNOWFLAKE_ML_REGION,
        database=settings.SNOWFLAKE_ML_DB,
        autocommit=False,
    )

    return connection
