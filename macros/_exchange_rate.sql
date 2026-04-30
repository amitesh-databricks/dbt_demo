{% macro convert_eur_to_usd(column_name) %}
    ({{ column_name }} * 1.08) -- Fixed rate for example
{% endmacro %}