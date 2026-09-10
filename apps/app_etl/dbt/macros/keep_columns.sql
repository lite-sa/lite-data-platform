{% macro keep_columns(source_relation, columns) %}
{#- The explicit column allowlist of a litecore mirror model, tolerant of
    dlt's lazy column creation: a raw column exists only once a non-null
    value has landed, so dev and raw_test lack columns prod has. Emits the
    listed columns that exist in the relation, in list order, and skips a
    listed column the relation lacks (no NULL fill: nothing reads these
    tables downstream). At run time a relation with no columns at all is a
    missing source table, so fail there instead of compiling an empty
    select. At parse time the adapter returns no columns and the macro
    emits nothing, which is fine: parse never runs the SQL. -#}
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
