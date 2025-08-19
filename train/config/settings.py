from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    ENV: str = "local"
    DATA_PROCESSED_DIR: str = "data/processed"
    DATA_RAW_DIR: str = "data/raw"
    MODEL_DIR: str = "models"

    MYSQL_HOST: str
    MYSQL_USER: str
    MYSQL_PASS: str
    MYSQL_PORT: str
    MYSQL_DB: str
    SNOWFLAKE_USERNAME: str
    SNOWFLAKE_PRIVATE_KEY_DECRYPTED: str
    SNOWFLAKE_ACCOUNT: str

    MODELS_S3_BUCKET: str = "compstak-machine-learning"

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
