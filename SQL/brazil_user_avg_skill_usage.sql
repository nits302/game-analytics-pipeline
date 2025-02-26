## 2 Tỷ lệ sử dụng skill trung bình trong 1 ván chơi của những user ở Brazil?

SELECT
    AVG(skill_used) AS avg_skill_usage_per_session
FROM `nexar-test-452015.mock_data.game_sessions`;