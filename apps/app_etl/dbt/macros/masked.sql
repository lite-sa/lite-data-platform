{% macro masked(column_name, keep_start, keep_end) %}
{#- Mask for a litecore mirror model: the first keep_start and last keep_end
    characters, a star for each one between, so the length stays real. A
    value too short to keep anything is all stars. NULL and '' give NULL. -#}
    if(
        length(nullif({{ column_name }}, '')) > {{ keep_start + keep_end }},
        concat(
            left({{ column_name }}, {{ keep_start }}),
            repeat('*', length({{ column_name }}) - {{ keep_start + keep_end }}),
            right({{ column_name }}, {{ keep_end }})
        ),
        repeat('*', length(nullif({{ column_name }}, '')))
    )
{%- endmacro %}
