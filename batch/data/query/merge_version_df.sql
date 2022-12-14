merge into {schema}.{table} t
using {schema}.temp s
on t.comp_data_id_version = s.comp_data_id_version
when matched then
update set
s.tenant_name_prob = t.tenant_name_prob,
s.space_type_id_prob = t.space_type_id_prob,
s.transaction_size_prob = t.transaction_size_prob,
s.starting_rent_prob = t.starting_rent_prob,
s.execution_date_prob = t.execution_date_prob,
s.commencement_date_prob = t.commencement_date_prob,
s.lease_term_prob = t.lease_term_prob,
s.expiration_date_prob = t.expiration_date_prob,
s.work_value_prob = t.work_value_prob,
s.free_months_prob = t.free_months_prob,
s.transaction_type_id_prob = t.transaction_type_id_prob,
s.rent_bumps_percent_bumps_prob = t.rent_bumps_percent_bumps_prob,
s.rent_bumps_dollar_bumps_prob = t.rent_bumps_dollar_bumps_prob,
s.lease_type_id_prob = t.lease_type_id_prob,
date_created = current_timestamp

when not matched then
insert (
    comp_data_id_version,
    tenant_name_prob,
    space_type_id_prob,
    transaction_size_prob,
    starting_rent_prob,
    execution_date_prob,
    commencement_date_prob,
    lease_term_prob,
    expiration_date_prob,
    work_value_prob,
    free_months_prob,
    transaction_type_id_prob,
    rent_bumps_percent_bumps_prob,
    rent_bumps_dollar_bumps_prob,
    lease_type_id_prob,
    date_created
    )
values (
    s.comp_data_id_version,
    s.tenant_name_prob,
    s.space_type_id_prob,
    s.transaction_size_prob,
    s.starting_rent_prob,
    s.execution_date_prob,
    s.commencement_date_prob,
    s.lease_term_prob,
    s.expiration_date_prob,
    s.work_value_prob,
    s.free_months_prob,
    s.transaction_type_id_prob,
    s.rent_bumps_percent_bumps_prob,
    s.rent_bumps_dollar_bumps_prob,
    s.lease_type_id_prob,
    current_timestamp
    )
;
