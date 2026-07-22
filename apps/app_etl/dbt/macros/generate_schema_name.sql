{% macro generate_schema_name(custom_schema_name, node) -%}
{#- dbt-bigquery's built-in default CONCATENATES a custom schema onto the
    target dataset ("core_aml" for +schema: aml against target core),
    which is never what we want here — a +schema override always names a
    dataset outright (e.g. models/intermediate/aml/ -> BQ_DATASET_AML's
    value). Standard dbt override recipe: no custom schema -> target's
    dataset; a custom schema -> exactly that, nothing appended. -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
