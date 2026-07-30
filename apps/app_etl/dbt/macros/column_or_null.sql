{% macro column_or_null(source_relation, column_name, data_type) %}
{#- dlt materializes a raw column only after it has seen a non-null value
    for it, so a legitimate source column can be missing from the landed
    table (always in the sparse local stand-in, and in prod until the
    first row that populates it). Emit NULL of the right type instead of
    breaking the build. Goes away once ingestion declares explicit column
    allowlists (dlt then creates all columns upfront). -#}
    {%- set columns = adapter.get_columns_in_relation(source_relation) | map(attribute="name") | map("lower") | list -%}
    {%- if column_name | lower in columns -%}
        {{ column_name }}
    {%- else -%}
        cast(null as {{ data_type }}) as {{ column_name }}
    {%- endif -%}
{% endmacro %}
