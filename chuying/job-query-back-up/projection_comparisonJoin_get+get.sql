SELECT *
FROM company_type AS ct, movie_companies AS mc
WHERE ct.id = mc.company_type_id;