SELECT
cv.id,
cv.submitter_person_id,
lds.logo,
cv.comp_data_id AS comp_data_id_version,
cm.comp_data_id AS comp_data_id_master,
tv.name AS tenant_name_version,
cdcfv.space_type_id AS space_type_id_version,
cdv.transaction_size AS transaction_size_version,
cdv.starting_rent AS starting_rent_version,
cdv.execution_date AS execution_date_version,
cdv.commencement_date AS commencement_date_version,
cdv.lease_term AS lease_term_version,
cdv.expiration_date AS expiration_date_version,
cdv.work_value AS work_value_version,
cdv.free_months AS free_months_version,
cdv.transaction_type_id AS transaction_type_id_version,
cdv.rent_bumps_percent_bumps AS rent_bumps_percent_bumps_version,
cdv.rent_bumps_dollar_bumps AS rent_bumps_dollar_bumps_version,
cdv.lease_type_id AS lease_type_id_version,
tm.name AS tenant_name_master,
cdcfm.space_type_id AS space_type_id_master,
cdm.transaction_size AS transaction_size_master,
cdm.starting_rent AS starting_rent_master,
cdm.execution_date AS execution_date_master,
cdm.commencement_date AS commencement_date_master,
cdm.lease_term AS lease_term_master,
cdm.expiration_date AS expiration_date_master,
cdm.work_value AS work_value_master,
cdm.free_months AS free_months_master,
cdm.transaction_type_id AS transaction_type_id_master,
cdm.rent_bumps_percent_bumps AS rent_bumps_percent_bumps_master,
cdm.rent_bumps_dollar_bumps AS rent_bumps_dollar_bumps_master,
cdm.lease_type_id AS lease_type_id_master
FROM
internal_analytics.mysql_compstak.comp_version cv
JOIN internal_analytics.mysql_compstak.comp_data cdv
    ON cdv.id = cv.comp_data_id
JOIN internal_analytics.mysql_compstak.comp_master_versions cmv
    ON cmv.comp_version_id = cv.id
JOIN internal_analytics.mysql_compstak.comp_master cm
    ON cmv.comp_master_id = cm.id
JOIN internal_analytics.mysql_compstak.comp_data cdm
    ON cm.comp_data_id = cdm.id
LEFT JOIN internal_analytics.mysql_compstak.comp_proposal cp
    ON cv.id = cp.comp_version_id
LEFT JOIN internal_analytics.ANALYTICS.LEASE_TASKS lt
    ON cp.COMP_BATCH_ID = lt.batch_id
LEFT JOIN internal_analytics.ANALYTICS.submissions s
    ON lt.SUBMISSION_ID = s.id
LEFT JOIN internal_analytics.ANALYTICS.logo_detection_submission lds
    ON lds.id = s.id
LEFT JOIN internal_analytics.mysql_compstak.tenant tv
    ON tv.id = cdv.tenant_id
JOIN internal_analytics.mysql_compstak.comp_data_calculated_fields cdcfv
    ON cdv.id = cdcfv.comp_data_id
LEFT JOIN internal_analytics.mysql_compstak.tenant tm
    ON tm.id = cdm.tenant_id
JOIN internal_analytics.mysql_compstak.comp_data_calculated_fields cdcfm
    ON cdm.id = cdcfm.comp_data_id
