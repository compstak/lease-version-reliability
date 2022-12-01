from functools import lru_cache
from typing import Optional

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    ENV: Optional[str] = Field(None, env="ENV")
    PROJECT_NAME: str = "Demo"
    API_V1_STR: str = "/api/v1"

    DATA_RAW_DIR: str = "data/raw/"
    DATA_DIR: str = "data/processed/"
    MODEL_DIR: str = "models/"

    MYSQL_HOST: str
    MYSQL_USER: str
    MYSQL_PASS: str
    MYSQL_PORT: str
    MYSQL_DB: str

    CS_AUTH_URL: str
    CS_COMP_PROCESSING_URL: str
    CS_EXCHANGE_URL: str
    CS_CLIENT_ID: str
    CS_CLIENT_SECRET: str
    CS_SCOPE: str
    MAX_TOKEN_ATTEMPTS: int = 5

    AWS_ROLE_ARN: Optional[str] = None
    AWS_WEB_IDENTITY_TOKEN_FILE: Optional[str] = None
    MODELS_S3_BUCKET: str

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> "Settings":
    return Settings()


settings = get_settings()
