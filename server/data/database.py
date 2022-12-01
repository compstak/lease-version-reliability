from databases import Database

from server.config.settings import settings


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
