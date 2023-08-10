{% macro foreign_key(this, column, parent, parent_column) %}

alter table {{ this }}
add constraint FK_{{ this.name }}_to_{{ parent.name }}
foreign key ({{ column }})
references {{ parent }} ({{ parent_column }})

{% endmacro %}
