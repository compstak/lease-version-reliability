from typing import TYPE_CHECKING

from databases import Database
import snowflake.connector

from train.config.settings import settings

if TYPE_CHECKING:
    from snowflake.connector.connection import SnowflakeConnection

snowflake.connector.paramstyle = "qmark"


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


def get_snowflake_connection():
    """
    Get Snowflake cursor
    """
    connection = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USERNAME,
        password=settings.SNOWFLAKE_PASSWORD,
        account=settings.SNOWFLAKE_ACCOUNT,
        region="us-east-1",
        warehouse="PC_PERISCOPE_WH",
        database="INTERNAL_ANALYTICS",
        autocommit=False,
    )
    return connection


cs_mysql_instance = CompstakServicesMySQL()
