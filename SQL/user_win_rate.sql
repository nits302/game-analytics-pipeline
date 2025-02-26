## 1 Tỷ lệ chiến thắng (win) ở các level 1, 5, 10 của toàn bộ user


WITH level_starts AS (
    SELECT
        level,
        COUNT(*) AS total_starts
    FROM `nexar-test-452015.mock_data.level_events`
    WHERE event_name = 'level_start' AND level IN (1, 5, 10)
    GROUP BY level
),
level_wins AS (
    SELECT
        level,
        COUNT(*) AS total_wins
    FROM `nexar-test-452015.mock_data.level_events`
    WHERE event_name = 'level_finish' AND result = 'win' AND level IN (1, 5, 10)
    GROUP BY level
)
SELECT
    s.level,
    s.total_starts,
    COALESCE(w.total_wins, 0) AS total_wins,
    COALESCE(SAFE_DIVIDE(w.total_wins, s.total_starts), 0) AS win_rate
FROM level_starts s
LEFT JOIN level_wins w ON s.level = w.level
ORDER BY s.level;