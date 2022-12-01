import asyncio

import structlog

from batch.common.logging import initialize_logging
from batch.config.settings import settings
from batch.data.database import (
    cs_mongo_instance as mongo_client,
    cs_mysql_instance as mysql,
)

logger = structlog.get_logger()
initialize_logging(settings.ENV)


async def main() -> None:
    # MySQL connection
    await mysql.connect()
    await mysql.disconnect()

    # Mongo connection
    mongo_client.get_io_loop = asyncio.get_running_loop
    mongodb = mongo_client[settings.MONGO_DB]  # noqa: F841


if __name__ == "__main__":
    asyncio.run(main())
