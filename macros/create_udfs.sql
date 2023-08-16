{% macro create_udfs() %}
CREATE SCHEMA IF NOT EXISTS {{ target.schema }};

{{ create_f_int_or_null() }};

{% endmacro %}
