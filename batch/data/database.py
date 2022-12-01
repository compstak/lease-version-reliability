from databases import Database
from motor.motor_asyncio import AsyncIOMotorClient

from batch.config.settings import settings


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


class CompstakServicesMongoDB(AsyncIOMotorClient):
    def __init__(self) -> None:
        super().__init__(settings.MONGO_HOST)


cs_mysql_instance = CompstakServicesMySQL()
cs_mongo_instance = CompstakServicesMongoDB()
