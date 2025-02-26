CREATE VIEW `nexar-test-452015.mock_data.game_sessions` AS
SELECT
    user_id,
    level,
    MIN(CASE WHEN event_name = 'level_start' THEN event_timestamp END) AS start_time,
    MAX(CASE WHEN event_name = 'level_finish' THEN event_timestamp END) AS finish_time,
    COUNT(CASE WHEN event_name = 'use_skill' THEN 1 END) AS skill_used
FROM `nexar-test-452015.mock_data.game_events`,
UNNEST([STRUCT(
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'level') AS level
)]) AS level_struct
WHERE geo.country = 'Brazil'
GROUP BY user_id, level
HAVING start_time IS NOT NULL AND finish_time IS NOT NULL;