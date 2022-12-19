merge into {schema}.{table} t
using {schema}.temp s
on t.submitter_person_id = s.submitter_person_id
when matched then
update set
t.tenant_name = s.tenant_name,
t.space_type_id = s.space_type_id,
t.transaction_size = s.transaction_size,
t.starting_rent = s.starting_rent,
t.execution_date = s.execution_date,
t.commencement_date = s.commencement_date,
t.lease_term = s.lease_term,
t.expiration_date = s.expiration_date,
t.work_value = s.work_value,
t.free_months = s.free_months,
t.transaction_type_id = s.transaction_type_id,
t.rent_bumps_percent_bumps = s.rent_bumps_percent_bumps,
t.rent_bumps_dollar_bumps = s.rent_bumps_dollar_bumps,
t.lease_type_id = s.lease_type_id,
t.general_reliability = s.general_reliability,
t.date_created = current_timestamp
when not matched then
insert (
    submitter_person_id,
    tenant_name,
    space_type_id,
    transaction_size,
    starting_rent,
    execution_date,
    commencement_date,
    lease_term,
    expiration_date,
    work_value,
    free_months,
    transaction_type_id,
    rent_bumps_percent_bumps,
    rent_bumps_dollar_bumps,
    lease_type_id,
    general_reliability,
    date_created
    )
values (
    s.submitter_person_id,
    s.tenant_name,
    s.space_type_id,
    s.transaction_size,
    s.starting_rent,
    s.execution_date,
    s.commencement_date,
    s.lease_term,
    s.expiration_date,
    s.work_value,
    s.free_months,
    s.transaction_type_id,
    s.rent_bumps_percent_bumps,
    s.rent_bumps_dollar_bumps,
    s.lease_type_id,
    s.general_reliability,
    current_timestamp
    )
;
