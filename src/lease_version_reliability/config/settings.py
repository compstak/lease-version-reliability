from functools import lru_cache
from typing import Optional

from pydantic import BaseModel, BaseSettings

from lease_version_reliability.config.attributes import attributes


class BatchConfig(BaseModel):
    """Application configurations."""

    BATCH_SIZE: int = 250000


class TrainConfig(BaseModel):
    """Application configurations."""

    MODEL_FILENAME: str = "lease_reliability_clf.pickle"


class Settings(BaseSettings):
    ENV: str = "stage"
    PROJECT_NAME: str = "lease-version-reliability"
    BATCH_CONFIG: BatchConfig = BatchConfig()
    TRAIN_CONFIG: TrainConfig = TrainConfig()

    ATTRIBUTES: list[str] = attributes

    MODEL_DIR: str = "models"
    SQL_QUERY: str = "lease_version_reliability.data.query"

    MYSQL_HOST: str = ""
    MYSQL_USER: str = ""
    MYSQL_PASS: str = ""
    MYSQL_PORT: str = ""
    MYSQL_DB: str = ""

    # Batch
    # SNOWFLAKE ML_PIPELINE_STAGE
    SNOWFLAKE_USER = ""
    SNOWFLAKE_PASS = ""
    SNOWFLAKE_ROLE = ""
    SNOWFLAKE_ACCOUNT = ""
    SNOWFLAKE_REGION = ""
    SNOWFLAKE_DB = "g"

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
