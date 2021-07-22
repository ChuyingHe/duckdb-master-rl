EXPLAIN SELECT MIN(mc.note) AS production_note
FROM movie_companies AS mc
WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (mc.note LIKE '%(co-production)%'
       OR mc.note LIKE '%(presents)%');

