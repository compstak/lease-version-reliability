import typing

from databases import Database
import snowflake.connector

from batch.config.settings import settings

snowflake.connector.paramstyle = "qmark"


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


class CompstakServicesMySQL(Database):
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
