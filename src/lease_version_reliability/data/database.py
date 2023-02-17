import logging

from databases import Database
import snowflake.connector

from src.lease_version_reliability.config.settings import settings

logging.basicConfig(level=logging.INFO)


class CompstakServicesMySQL(Database):
    """
    Get MySQL connection
    """

    def __init__(self) -> None:
        super().__init__(
            "mysql://{}:{}@{}:{}/{}".format(
                settings.MYSQL_USER,
                settings.MYSQL_PASS,
                settings.MYSQL_HOST,
                settings.MYSQL_PORT,
                settings.MYSQL_DB,
            ),
        )


cs_mysql_instance = CompstakServicesMySQL()


def get_snowflake_connection() -> snowflake.connector:
    """
    Get Snowflake cursor
    """
    connection = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASS,
        role=settings.SNOWFLAKE_ROLE,
        account=settings.SNOWFLAKE_ACCOUNT,
        region=settings.SNOWFLAKE_REGION,
        database=settings.SNOWFLAKE_DB,
        autocommit=False,
    )

    return connection
