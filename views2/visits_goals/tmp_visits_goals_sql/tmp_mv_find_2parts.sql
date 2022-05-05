SELECT
      partition
FROM system.parts
WHERE table = 'tmp_visits_goals_view'
    AND active
ORDER BY partition desc
LIMIT 2;