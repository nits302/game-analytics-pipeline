CREATE VIEW `nexar-test-452015.mock_data.level_events` AS
SELECT
    event_name,
    user_id,
    event_timestamp,
    EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'level') AS level,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'result') AS result
FROM `nexar-test-452015.mock_data.game_events`
WHERE event_name IN ('level_start', 'level_finish');
