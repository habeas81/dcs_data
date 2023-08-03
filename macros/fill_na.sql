{% macro fill_na(col, value) %}

COALESCE({{ col }}, '{{ value }}')

{% endmacro %}
