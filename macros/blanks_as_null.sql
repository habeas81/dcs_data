{% macro blanks_as_null(column) %}
NULLIF(
    TRIM({{ column }}),
    ''
) 
{% endmacro %}
