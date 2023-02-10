from datetime import timedelta
import typing

import dateutil.parser as parser
import jellyfish
import numpy as np
import pandas as pd
import structlog

from train.config.settings import settings
from train.data.database import get_snowflake_connection

logger = structlog.get_logger()


def read_file(path: str) -> str:
    """
    Read query file.
    """
    with open(f"{settings.QUERY_DIR}/{path}") as fd:
        query = fd.read()

    return query


def get_reliable_data() -> pd.DataFrame:
    query = read_file("reliable_data.sql")
    df = pd.read_sql(
        query,
        get_snowflake_connection(),
    )
    df.columns = [x.lower() for x in df.columns]
    return df


def get_all_data() -> pd.DataFrame:
    query = read_file("all_data.sql")
    df = pd.read_sql(
        query,
        get_snowflake_connection(),
    )
    df.columns = [x.lower() for x in df.columns]
    return df


def get_reliable_data_by_attribute() -> pd.DataFrame:
    query = read_file("reliable_data_by_attribute.sql")
    df = pd.read_sql(
        query,
        get_snowflake_connection(),
    )
    df.columns = [x.lower() for x in df.columns]
    return df


def label_strict_equality(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    return 0


def label_tenant_name(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    else:
        if jellyfish.jaro_winkler(subject, target) > 0.9:
            return 1
    return 0


def label_transaction_size(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject >= target * 0.95 and subject <= target * 1.05:
        return 1
    return 0


def label_execution_date(subject: typing.Any, target: typing.Any) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    subject = str(subject)
    target = str(target)
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_commencement_date(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    subject = str(subject)
    target = str(target)
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_expiration_date(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    subject = str(subject)
    target = str(target)
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_lease_term(
    subject: typing.Any,
    target: typing.Any,
) -> typing.Any:
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject >= target * 0.92 and subject <= target * 1.08:
        return 1
    return 0


attribute_to_label_dict = {
    "tenant_name": label_tenant_name,
    "space_type_id": label_strict_equality,
    "transaction_size": label_transaction_size,
    "starting_rent": label_strict_equality,
    "execution_date": label_execution_date,
    "commencement_date": label_commencement_date,
    "lease_term": label_lease_term,
    "expiration_date": label_expiration_date,
    "work_value": label_strict_equality,
    "free_months": label_strict_equality,
    "transaction_type_id": label_strict_equality,
    "rent_bumps_percent_bumps": label_strict_equality,
    "rent_bumps_dollar_bumps": label_strict_equality,
    "lease_type_id": label_strict_equality,
}


def get_labels(data: pd.DataFrame, attributes: typing.Any) -> pd.DataFrame:
    for att in attributes:
        print(f"Calculating Labels: {att}")
        data[att + "_filled"] = np.where(
            (pd.notnull(data[att + "_version"])),
            1,
            0,
        )

        data[att + "_label"] = data.apply(
            lambda x: attribute_to_label_dict[att](
                x[att + "_version"],
                x[att + "_master"],
            ),
            axis=1,
        )

    return data
