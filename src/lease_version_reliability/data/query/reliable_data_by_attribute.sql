SELECT
cmv2.comp_version_id,
sub.*
FROM
comp_master_versions cmv2
JOIN (
    SELECT cmv.comp_master_id,
    COUNT(tv.name) AS tenant_name_count,
    COUNT(cdcfv.space_type_id) AS space_type_id_count,
    COUNT(cdv.transaction_size) AS transaction_size_count,
    COUNT(cdv.starting_rent) AS starting_rent_count,
    COUNT(cdv.execution_date) AS execution_date_count,
    COUNT(cdv.commencement_date) AS commencement_date_count,
    COUNT(cdv.lease_term) AS lease_term_count,
    COUNT(cdv.expiration_date) AS expiration_date_count,
    COUNT(cdv.work_value) AS work_value_count,
    COUNT(cdv.free_months) AS free_months_count,
    COUNT(cdv.transaction_type_id) AS transaction_type_id_count,
    COUNT(cdv.rent_bumps_percent_bumps) AS rent_bumps_percent_bumps_count,
    COUNT(cdv.rent_bumps_dollar_bumps) AS rent_bumps_dollar_bumps_count,
    COUNT(cdv.lease_type_id) AS lease_type_id_count
    FROM comp_master_versions cmv
    JOIN comp_version cv
        ON cmv.comp_version_id = cv.id
    JOIN comp_data cdv
        ON cdv.id = cv.comp_data_id
    LEFT JOIN tenant tv
        ON tv.id = cdv.tenant_id
    JOIN comp_data_calculated_fields cdcfv
        ON cdv.id = cdcfv.comp_data_id
    LEFT JOIN tenant tm
        ON tm.id = cdv.tenant_id
    JOIN comp_data_calculated_fields cdcfm
        ON cdv.id = cdcfm.comp_data_id
    GROUP BY 1) AS sub
ON cmv2.comp_master_id = sub.comp_master_id
ORDER BY cmv2.comp_version_id
