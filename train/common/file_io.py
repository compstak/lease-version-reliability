import os
import shutil
import typing

import boto3
import botocore
import structlog

from train.config.settings import settings

logger = structlog.get_logger()


def get_web_identity_token() -> str:
    """
    Get token value from filepath
    """
    token = ""
    with open(settings.AWS_WEB_IDENTITY_TOKEN_FILE) as f:  # type: ignore
        token = f.read().strip()

    return token


def get_aws_cred() -> typing.Any:
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


def get_s3_resource() -> typing.Any:
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


def upload_models() -> None:
    """
    Upload the model to S3 bucket
    """

    s3 = get_s3_resource()
    object_name = f"{settings.PROJECT_NAME}/models/{settings.ENV}.model.tar.gz"
    file_name = f"{settings.MODEL_DIR}/model.tar.gz"

    try:
        shutil.make_archive(
            settings.MODEL_DIR + "/model",
            "gztar",
            settings.MODEL_DIR,
        )

        s3.Bucket(settings.MODELS_S3_BUCKET).upload_file(
            file_name,
            object_name.format(settings.ENV),
        )

        os.remove(file_name)
        logger.debug("Successfully uploaded models")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.error("Permission denied when trying to upload file.")
        else:
            raise
