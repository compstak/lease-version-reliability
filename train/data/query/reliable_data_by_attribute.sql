SELECT
cmv2.comp_version_id,
sub.*
FROM
internal_analytics.mysql_compstak.comp_master_versions cmv2
join (
select cmv.comp_master_id,
count(tv.name) as tenant_name_count,
count(cdcfv.space_type_id) as space_type_id_count,
count(cdv.transaction_size) as transaction_size_count,
count(cdv.starting_rent) as starting_rent_count,
count(cdv.execution_date) as execution_date_count,
count(cdv.commencement_date) as commencement_date_count,
count(cdv.lease_term) as lease_term_count,
count(cdv.expiration_date) as expiration_date_count,
count(cdv.work_value) as work_value_count,
count(cdv.free_months) as free_months_count,
count(cdv.transaction_type_id) as transaction_type_id_count,
count(cdv.rent_bumps_percent_bumps) as rent_bumps_percent_bumps_count,
count(cdv.rent_bumps_dollar_bumps) as rent_bumps_dollar_bumps_count,
count(cdv.lease_type_id) as lease_type_id_version

from internal_analytics.mysql_compstak.comp_master_versions cmv
JOIN internal_analytics.mysql_compstak.comp_version cv
    ON cmv.comp_version_id = cv.id
join internal_analytics.mysql_compstak.comp_data cdv
    ON cdv.id = cv.comp_data_id
LEFT JOIN internal_analytics.mysql_compstak.tenant tv
    ON tv.id = cdv.tenant_id
JOIN internal_analytics.mysql_compstak.comp_data_calculated_fields cdcfv
    ON cdv.id = cdcfv.comp_data_id
LEFT JOIN internal_analytics.mysql_compstak.tenant tm
    ON tm.id = cdv.tenant_id
JOIN internal_analytics.mysql_compstak.comp_data_calculated_fields cdcfm
    ON cdv.id = cdcfm.comp_data_id
group by 1 ) as sub
on cmv2.comp_master_id = sub.comp_master_id
