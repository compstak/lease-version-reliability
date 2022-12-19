create temporary table IF NOT EXISTS {}.temp
(
    submitter_person_id integer,
    tenant_name float,
    space_type_id float,
    transaction_size float,
    starting_rent float,
    execution_date float,
    commencement_date float,
    lease_term float,
    expiration_date float,
    work_value float,
    free_months float,
    transaction_type_id float,
    rent_bumps_percent_bumps float,
    rent_bumps_dollar_bumps float,
    lease_type_id float,
    general_reliability float,
    date_created timestamp
);
