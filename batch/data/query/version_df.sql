create temporary table IF NOT EXISTS {}.temp
(
    comp_data_id_version integer
    tenant_name_prob float
    space_type_id_prob float
    transaction_size_prob float
    starting_rent_prob float
    execution_date_prob float
    commencement_date_prob float
    lease_term_prob float
    expiration_date_prob float
    work_value_prob float
    free_months_prob float
    transaction_type_id_prob float
    rent_bumps_percent_bumps_prob float
    rent_bumps_dollar_bumps_prob float
    lease_type_id_prob float
    date_created timestamp
);
