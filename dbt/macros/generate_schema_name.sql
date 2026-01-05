{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override default schema name generation.
        If model has custom_schema_name, use it directly.
        Otherwise use target schema.
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
