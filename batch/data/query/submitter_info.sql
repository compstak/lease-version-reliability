SELECT  per.id,
CONCAT(per.first_name, ' ',  per.last_name) AS submitter_name
FROM internal_analytics.mysql_compstak.person per
