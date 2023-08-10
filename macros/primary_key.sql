{% macro primary_key(this, column) %}

alter table {{ this }}
add primary key ({{ column }})

{% endmacro %}
