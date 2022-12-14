merge into {schema}.{table} t
using {schema}.temp s
on t.submitter_person_id = s.submitter_person_id
when matched then
update set
t.tenant_name_reliability = s.tenant_name_reliability,
t.space_type_id_reliability = s.space_type_id_reliability
t.transaction_size_reliability = s.transaction_size_reliability
t.starting_rent_reliability = s.starting_rent_reliability
t.execution_date_reliability = s.execution_date_reliability
t.commencement_date_reliability = s.commencement_date_reliability
t.lease_term_reliability = s.lease_term_reliability
t.expiration_date_reliability = s.expiration_date_reliability
t.work_value_reliability = s.work_value_reliability
t.free_months_reliability = s.free_months_reliability
t.transaction_type_id_reliability = s.transaction_type_id_reliability
t.rent_bumps_percent_bumps_reliability = s.rent_bumps_percent_bumps_reliability
t.rent_bumps_dollar_bumps_reliability = s.rent_bumps_dollar_bumps_reliability
t.lease_type_id_reliability = s.lease_type_id_reliability
t.general_reliability = s.general_reliability
t.date_created = current_timestamp
when not matched then
insert (
    submitter_person_id,
    tenant_name_reliability,
    space_type_id_reliability,
    transaction_size_reliability,
    starting_rent_reliability,
    execution_date_reliability,
    commencement_date_reliability,
    lease_term_reliability,
    expiration_date_reliability,
    work_value_reliability,
    free_months_reliability,
    transaction_type_id_reliability,
    rent_bumps_percent_bumps_reliability,
    rent_bumps_dollar_bumps_reliability,
    lease_type_id_reliability,
    general_reliability,
    date_created
    )
values (
    s.submitter_person_id,
    s.tenant_name_reliability,
    s.space_type_id_reliability,
    s.transaction_size_reliability,
    s.starting_rent_reliability,
    s.execution_date_reliability,
    s.commencement_date_reliability,
    s.lease_term_reliability,
    s.expiration_date_reliability,
    s.work_value_reliability,
    s.free_months_reliability,
    s.transaction_type_id_reliability,
    s.rent_bumps_percent_bumps_reliability,
    s.rent_bumps_dollar_bumps_reliability,
    s.lease_type_id_reliability,
    s.general_reliability,
    current_timestamp
    )
;
