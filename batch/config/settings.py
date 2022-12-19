from functools import lru_cache
from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    ENV: str
    PROJECT_NAME: str = "lease-version-reliability"

    ATTRIBUTES = [
        "tenant_name",
        "space_type_id",
        "transaction_size",
        "starting_rent",
        "execution_date",
        "commencement_date",
        "lease_term",
        "expiration_date",
        "work_value",
        "free_months",
        "transaction_type_id",
        "rent_bumps_percent_bumps",
        "rent_bumps_dollar_bumps",
        "lease_type_id",
    ]

    QUERY_DIR: str = "batch/data/query"
    MODEL_DIR: str = "models"
    DATA_DIR: str = "data"
    MODEL_NAME: str = "lease_reliability_clf.pickle"

    SNOWFLAKE_USER: str
    SNOWFLAKE_PASS: str
    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_REGION: str
    SNOWFLAKE_WH: str
    SNOWFLAKE_DB: str

    SNOWFLAKE_ML_USER: str
    SNOWFLAKE_ML_PASS: str
    SNOWFLAKE_ML_ROLE: str
    SNOWFLAKE_ML_ACCOUNT: str
    SNOWFLAKE_ML_DB: str

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
