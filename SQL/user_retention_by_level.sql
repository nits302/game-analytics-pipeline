## 3. Tỷ lệ user còn ở lại chơi game qua từng level?

WITH user_levels AS (
    SELECT DISTINCT
        user_id,
        (SELECT ep.value.int_value 
         FROM UNNEST(event_params) ep 
         WHERE ep.key = 'level') AS level
    FROM `nexar-test-452015.mock_data.game_events`
    WHERE event_name = 'level_start'
),
level_counts AS (
    SELECT
        level,
        COUNT(DISTINCT user_id) AS user_count
    FROM user_levels
    WHERE level IS NOT NULL
    GROUP BY level
),
base_users AS (
    SELECT user_count AS level_1_users
    FROM level_counts
    WHERE level = 1
)
SELECT
    l.level,
    l.user_count,
    SAFE_DIVIDE(l.user_count, b.level_1_users) AS retention_rate
FROM level_counts l
CROSS JOIN base_users b
ORDER BY l.level;
