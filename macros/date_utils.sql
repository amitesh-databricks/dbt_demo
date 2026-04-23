{% macro get_session(x) %}
case when month(to_timestamp({{x}})) in (10,11,12)
    then 'winter'
    when month(to_timestamp({{x}})) in (1,2,3)
    then 'spring'
    when month(to_timestamp({{x}})) in (4,5,6)
    then 'summer'
    else 'rainy'
    end
    {% endmacro %}

{% macro day_type(x) %}
case 
    when dayname(to_timestamp(started_at)) in ('sat','sun')
    then 'weekend'
    else 'businessday'
    end
{% endmacro %}

{% macro calc_trip_duration_minutes(started_at, ended_at) %}
    ROUND(
        DATEDIFF(second,
            TRY_CAST({{ started_at }} AS TIMESTAMP),
            TRY_CAST({{ ended_at }} AS TIMESTAMP)
        ) / 60.0, 2
    )
{% endmacro %}

{% macro extract_time_features(ts_col, prefix='') %}
    DATE(TRY_CAST({{ ts_col }} AS TIMESTAMP)) AS {{ prefix }}date,
    EXTRACT(YEAR  FROM TRY_CAST({{ ts_col }} AS TIMESTAMP))::INT AS {{ prefix }}year,
    EXTRACT(MONTH FROM TRY_CAST({{ ts_col }} AS TIMESTAMP))::INT AS {{ prefix }}month,
    EXTRACT(DOW   FROM TRY_CAST({{ ts_col }} AS TIMESTAMP))::INT AS {{ prefix }}day_of_week,   -- 0=Sunday
    EXTRACT(HOUR  FROM TRY_CAST({{ ts_col }} AS TIMESTAMP))::INT AS {{ prefix }}hour,
    CASE
        WHEN EXTRACT(DOW FROM TRY_CAST({{ ts_col }} AS TIMESTAMP)) IN (0,6)
        THEN TRUE ELSE FALSE
    END
{% endmacro %}

{% macro normalize_rideable_type(rideable_type_col) %}
    CASE LOWER(TRIM({{ rideable_type_col }}))
        WHEN 'electric_bike' THEN 'Electric Bike'
        WHEN 'classic_bike'  THEN 'Classic Bike'
        WHEN 'docked_bike'   THEN 'Docked Bike'
        ELSE 'Unknown'
    END
{% endmacro %}