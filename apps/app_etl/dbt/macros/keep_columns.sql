{% macro keep_columns(source_relation, columns) %}
{#- Column allowlist of a litecore mirror model. Emits the listed columns
    that exist in the relation, in list order, and skips the rest: dlt
    creates a raw column only once a non-null value lands, so dev lacks
    columns prod has. A relation with no columns at run time is a missing
    source table and fails. -#}
    {%- set present = adapter.get_columns_in_relation(source_relation) | map(attribute="name") | map("lower") | list -%}
    {%- if execute and present | length == 0 -%}
        {{ exceptions.raise_compiler_error("keep_columns: " ~ source_relation ~ " has no columns; is the source table ingested here?") }}
    {%- endif -%}
    {%- set kept = [] -%}
    {%- for col in columns -%}
        {%- if col | lower in present -%}
            {%- do kept.append(col) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ kept | join(",\n    ") }}
{%- endmacro %}
