CREATE TABLE `nexar-test-452015.mock_data.game_events` (
    event_date STRING,
    event_timestamp INT64,
    event_name STRING,
    event_params ARRAY<STRUCT<
        key STRING,
        value STRUCT<int_value INT64, string_value STRING>
    >>,
    user_id STRING,
    geo STRUCT<
        city STRING,
        country STRING,
        continent STRING,
        region STRING,
        sub_continent STRING,
        metro STRING
    >
)