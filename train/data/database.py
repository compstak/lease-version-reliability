import typing

import snowflake.connector

from train.config.settings import settings


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
