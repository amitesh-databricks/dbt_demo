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