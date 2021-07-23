SELECT person_id, SUM(nr_order) OVER (PARTITION BY person_id ORDER BY id)
FROM cast_info;