import logging

from databases import Database
import snowflake.connector

from lease_version_reliability.config.settings import settings

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


def get_snowflake_ml_pipeline_connection() -> snowflake.connector:
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
