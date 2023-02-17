import os
import pickle
import shutil
from typing import Any

import boto3
import botocore
import structlog

from lease_version_reliability.config.settings import settings

logger = structlog.get_logger()


def save_pickle(obj: Any, file_path: str) -> None:
    """
    Save pickle files
    """
    with open(file_path, "wb") as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)


def read_pickle(file_path: str) -> Any:
    """
    Read pickle files
    """
    with open(file_path, "rb") as handle:
        obj = pickle.load(handle)

    return obj


def save_model(obj: Any, filename: str) -> None:
    """
    Save training models
    """
    save_pickle(
        obj,
        "{directory}/{filename}".format(
            directory=settings.MODEL_DIR,
            filename=filename,
        ),
    )


def read_model(filename: str) -> Any:
    """
    Read pickle models
    """
    model = read_pickle(
        "{directory}/{filename}".format(
            directory=settings.MODEL_DIR,
            filename=filename,
        ),
    )

    return model


def get_web_identity_token() -> str:
    """
    Get token value from filepath
    """
    token = ""
    if settings.AWS_WEB_IDENTITY_TOKEN_FILE:
        with open(settings.AWS_WEB_IDENTITY_TOKEN_FILE) as f:
            token = f.read().strip()

    return token


def get_aws_cred() -> Any:
    """
    Get AWS credential
    """
    token = get_web_identity_token()
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role_with_web_identity(
        RoleArn=settings.AWS_ROLE_ARN,
        RoleSessionName="LeaseVersionReliabilitySession",
        WebIdentityToken=token,
    )

    return assumed_role_object["Credentials"]


def get_s3_resource() -> Any:
    """
    Get S3 resource
    """
    if not settings.AWS_WEB_IDENTITY_TOKEN_FILE:
        s3_resource = boto3.resource("s3")
    else:
        cred = get_aws_cred()
        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=cred["AccessKeyId"],
            aws_secret_access_key=cred["SecretAccessKey"],
            aws_session_token=cred["SessionToken"],
        )

    return s3_resource


def download_models() -> None:
    """
    Get models from S3 bucket
    """
    s3 = get_s3_resource()
    object_name = (
        f"lease-version-reliability/models/{settings.ENV}.model.tar.gz"
    )
    file_name = f"{settings.MODEL_DIR}/model.tar.gz"

    try:
        s3.Bucket(settings.MODELS_S3_BUCKET).download_file(
            object_name,
            file_name,
        )
        shutil.unpack_archive(file_name, settings.MODEL_DIR)
        os.remove(file_name)
        logger.debug("Successfully downloaded models")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.error("The object does not exist.")
        else:
            raise


def upload_models() -> None:
    """
    Upload models to S3 bucket
    """
    object_name = (
        f"lease-version-reliability/models/{settings.ENV}.model.tar.gz"
    )
    file_name = f"{settings.MODEL_DIR}/model.tar.gz"

    s3 = get_s3_resource()

    try:
        logger.debug("Uploading classifier.")
        shutil.make_archive(
            f"{settings.MODEL_DIR}/model",
            "gztar",
            settings.MODEL_DIR,
        )

        s3.Bucket(settings.MODELS_S3_BUCKET).upload_file(
            file_name,
            object_name,
        )

        os.remove(file_name)
        logger.info("Successfully uploaded models")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.error("Permission denied when trying to upload file.")
        else:
            raise
