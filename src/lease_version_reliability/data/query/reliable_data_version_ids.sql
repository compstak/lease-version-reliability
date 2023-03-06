SELECT
cv.id

FROM
comp_version cv
JOIN comp_data cdv
    ON cdv.id = cv.comp_data_id
JOIN comp_master_versions cmv
    ON cmv.comp_version_id = cv.id
JOIN comp_master cm
    ON cmv.comp_master_id = cm.id
JOIN comp_data cdm
    ON cm.comp_data_id = cdm.id

WHERE cm.id IN
(
    SELECT
    comp_master_id FROM comp_master_versions
    GROUP BY
    comp_master_id
    HAVING
    COUNT(1) >= 3
);
