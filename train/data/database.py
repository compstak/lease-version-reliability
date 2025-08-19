import snowflake.connector

from train.config.settings import settings


def get_snowflake_connection():
    """
    Get Snowflake cursor
    """
    connection = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USERNAME,
        private_key=settings.SNOWFLAKE_PRIVATE_KEY_DECRYPTED,
        account=settings.SNOWFLAKE_ACCOUNT,
        region="us-east-1",
        warehouse="PC_PERISCOPE_WH",
        database="INTERNAL_ANALYTICS",
        autocommit=False,
    )
    return connection
