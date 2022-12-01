from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    ENV: str

    MYSQL_HOST: str
    MYSQL_USER: str
    MYSQL_PASS: str
    MYSQL_PORT: str
    MYSQL_DB: str

    MONGO_HOST: str
    MONGO_DB: str

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
