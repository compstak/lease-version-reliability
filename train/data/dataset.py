from datetime import timedelta

import dateutil.parser as parser
import jellyfish
import numpy as np
import pandas as pd
import structlog

from train.data.database import CompstakServicesMySQL, get_snowflake_connection

logger = structlog.get_logger()


def get_reliable_data():
    df = pd.read_sql(
        """
    SELECT
	cv.id,
	cv.submitter_person_id,
	lds.logo,
	cv.comp_data_id AS comp_data_id_version,
	cm.comp_data_id AS comp_data_id_master,

	tv.name as tenant_name_version,
	cdcfv.space_type_id as space_type_id_version,
	cdv.transaction_size as transaction_size_version,
	cdv.starting_rent as starting_rent_version,
	cdv.execution_date as execution_date_version,
	cdv.commencement_date as commencement_date_version,
	cdv.lease_term as lease_term_version,
	cdv.expiration_date as expiration_date_version,
	cdv.work_value as work_value_version,
	cdv.free_months as free_months_version,
	cdv.transaction_type_id as transaction_type_id_version,
    cdv.rent_bumps_percent_bumps as rent_bumps_percent_bumps_version,
    cdv.rent_bumps_dollar_bumps as rent_bumps_dollar_bumps_version,
    cdv.lease_type_id as lease_type_id_version,


	tm.name as tenant_name_master,
	cdcfm.space_type_id as space_type_id_master,
	cdm.transaction_size as transaction_size_master,
	cdm.starting_rent as starting_rent_master,
	cdm.execution_date as execution_date_master,
	cdm.commencement_date as commencement_date_master,
	cdm.lease_term as lease_term_master,
	cdm.expiration_date as expiration_date_master,
	cdm.work_value as work_value_master,
	cdm.free_months as free_months_master,
	cdm.transaction_type_id as transaction_type_id_master,
    cdm.rent_bumps_percent_bumps as rent_bumps_percent_bumps_master,
    cdm.rent_bumps_dollar_bumps as rent_bumps_dollar_bumps_master,
    cdm.lease_type_id as lease_type_id_master


FROM
	mysql_compstak.comp_version cv
	JOIN mysql_compstak.comp_data cdv ON cdv.id = cv.comp_data_id
	JOIN mysql_compstak.comp_master_versions cmv ON cmv.comp_version_id = cv.id
	JOIN mysql_compstak.comp_master cm ON cmv.comp_master_id = cm.id
	JOIN mysql_compstak.comp_data cdm ON cm.comp_data_id = cdm.id
	LEFT JOIN mysql_compstak.comp_proposal cp ON cv.id = cp.comp_version_id
	LEFT JOIN ANALYTICS.LEASE_TASKS lt ON cp.COMP_BATCH_ID = lt.batch_id
	LEFT JOIN ANALYTICS.submissions s ON lt.SUBMISSION_ID = s.id
	LEFT JOIN ANALYTICS.logo_detection_submission lds ON lds.id = s.id

    LEFT JOIN mysql_compstak.tenant tv ON tv.id = cdv.tenant_id
    join mysql_compstak.comp_data_calculated_fields cdcfv on cdv.id = cdcfv.comp_data_id

    LEFT JOIN mysql_compstak.tenant tm ON tm.id = cdm.tenant_id
    join mysql_compstak.comp_data_calculated_fields cdcfm on cdm.id = cdcfm.comp_data_id
WHERE cm.id in (
		SELECT
			comp_master_id FROM mysql_compstak.comp_master_versions
		GROUP BY
			comp_master_id
		HAVING
			count(1) > 3)

    """,
        get_snowflake_connection(),
    )
    return df


async def get_version_master_data(
    db: CompstakServicesMySQL,
) -> pd.DataFrame:
    logger.info("Getting Version-Master Relationships")
    query = """
SELECT
cv.id,
cv.submitter_person_id,
lds.logo,
cv.comp_data_id AS comp_data_id_version,
cm.comp_data_id AS comp_data_id_master
FROM
comp_version cv
JOIN comp_data cdv ON cdv.id = cv.comp_data_id
JOIN comp_master_versions cmv ON cmv.comp_version_id = cv.id
JOIN comp_master cm ON cmv.comp_master_id = cm.id
JOIN comp_data cdm ON cm.comp_data_id = cdm.id
LEFT JOIN comp_proposal cp ON cv.id = cp.comp_version_id
LEFT JOIN internal_analytics.mongodb_compstak.tasks lt
ON cp.COMP_BATCH_ID = lt.batch_id
LEFT JOIN ANALYTICS.submissions s ON lt.SUBMISSION_ID = s.id
LEFT JOIN ANALYTICS.logo_detection_submission lds ON lds.id = s.id
WHERE
cm.id in (
SELECT
comp_master_id FROM comp_master_versions
GROUP BY
comp_master_id
HAVING
count(1) > 3)
    """

    data = [dict(item) for item in await db.fetch_all(query)]
    df = pd.DataFrame(data)
    return df


# async def get_comp_data(
#     db: CompstakServicesMySQL,
#     comp_data_ids: list[int],
# ) -> pd.DataFrame:
#     query = """
# SELECT
# 	cd.id as comp_data_id,
# 	t.name as tenant_name,
# 	cdcf.space_type_id,
# 	cd.transaction_size,
# 	cd.starting_rent,
# 	cd.execution_date,
# 	cd.commencement_date,
# 	cd.lease_term,
# 	cd.expiration_date,
# 	cd.work_value,
# 	cd.free_months,
# 	cd.transaction_type_id,
#     cd.rent_bumps_percent_bumps,
#     cd.rent_bumps_dollar_bumps,
#     cd.lease_type_id
# FROM
# 	comp_data cd
# 	LEFT JOIN tenant t ON t.id = cd.tenant_id
#     join comp_data_calculated_fields cdcf on cd.id = cdcf.comp_data_id
#
# WHERE cd.id in ({})
#     """.format(
#         ",".join([str(x) for x in comp_data_ids]),
#     )
#
#     data = [dict(item) for item in await db.fetch_all(query)]
#     df = pd.DataFrame(data)
#     return df


async def get_Dataset(db: CompstakServicesMySQL):

    # logger.info("Getting Version Master Data")
    # data = await get_version_master_data(db)
    #
    # comp_data_ids_version = list(set(data.comp_data_id_version.tolist()))
    # comp_data_ids_master = list(set(data.comp_data_id_master.tolist()))
    #
    # logger.info("Getting Version Comp Data")
    # version_data = await get_comp_data(db, comp_data_ids_version)
    # version_data = version_data.add_suffix("_version").drop_duplicates()
    #
    # logger.info("Getting Master Comp Data")
    # master_data =  await get_comp_data(db, comp_data_ids_master)
    # master_data =  master_data.add_suffix("_master").drop_duplicates()
    #
    # version_data.columns = version_data.columns.str.lower()
    # master_data.columns = master_data.columns.str.lower()
    #
    # logger.info("Merging Data")
    # data = data.merge(
    #     right=version_data,
    #     how="inner",
    #     left_on="comp_data_id_version",
    #     right_on="comp_data_id_version",
    # ).merge(
    #     right=master_data,
    #     how="inner",
    #     left_on="comp_data_id_master",
    #     right_on="comp_data_id_master",
    # )

    return pd.read_csv("data/raw/dataset.csv")


async def get_submitter_info(
    db: CompstakServicesMySQL,
) -> pd.DataFrame:
    # logger.info("Getting Version-Master Relationships")
    # query = """
    # select  per.id,
    # CONCAT(per.first_name, ' ',  per.last_name) as submitter_name
    # from person per
    # """
    #
    # data = [dict(item) for item in await db.fetch_all(query)]
    # df = pd.DataFrame(data)
    return pd.read_csv("data/raw/submitter_info.csv")


def label_strict_equality(subject, target):
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    return 0


def label_tenant_name(subject, target):
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject == target:
        return 1
    else:
        if jellyfish.jaro_winkler(subject, target) > 0.9:
            return 1
    return 0


def label_transaction_size(subject, target):
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if subject >= target * 0.95 and subject <= target * 1.05:
        return 1
    return 0


def label_execution_date(subject, target):
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_commencement_date(subject, target):
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_expiration_date(subject, target):
    if pd.isnull(subject) or pd.isnull(target):
        return -1
    if parser.parse(subject) <= parser.parse(target) + timedelta(
        days=90,
    ) and parser.parse(subject) >= parser.parse(target) - timedelta(days=90):
        return 1
    return 0


def label_lease_term(subject, target):
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


def get_labels(data, attributes):
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
