from functools import lru_cache
from typing import Optional

from pydantic import BaseModel, BaseSettings

from lease_version_reliability.config.attributes import attributes


class TrainConfig(BaseModel):
    """Application configurations."""

    MODEL_FILENAME: str = "lease_reliability_clf.pickle"


class Settings(BaseSettings):
    ENV: str = "dev"
    PROJECT_NAME: str = "lease-version-reliability"
    TRAIN_CONFIG: TrainConfig = TrainConfig()

    ATTRIBUTES: list[str] = attributes

    DATA_RAW_DIR: str = "data/raw"
    DATA_DIR: str = "data/processed"
    MODEL_DIR: str = "models"
    QUERY_DIR: str = "src/lease_version_reliability/data/query"
    MODEL_NAME: str = "lease_reliability_clf.pickle"

    SNOWFLAKE_USER: str = ""
    SNOWFLAKE_PASS: str = ""
    SNOWFLAKE_ACCOUNT: str = ""
    SNOWFLAKE_REGION: str = ""
    SNOWFLAKE_WH: str = ""
    SNOWFLAKE_DB: str = ""

    AWS_ROLE_ARN: Optional[str] = None
    AWS_WEB_IDENTITY_TOKEN_FILE: Optional[str] = None
    MODELS_S3_BUCKET: str = "compstak-machine-learning"

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
