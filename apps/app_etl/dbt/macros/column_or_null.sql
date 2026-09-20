{% macro column_or_null(source_relation, column_name, data_type) %}
{#- dlt creates a raw column only once a non-null value lands, so a real
    source column can be missing from the landed table. Emits a typed NULL
    for it instead of breaking the build. -#}
    {%- set columns = adapter.get_columns_in_relation(source_relation) | map(attribute="name") | map("lower") | list -%}
    {%- if column_name | lower in columns -%}
        {{ column_name }}
    {%- else -%}
        cast(null as {{ data_type }}) as {{ column_name }}
    {%- endif -%}
{% endmacro %}
