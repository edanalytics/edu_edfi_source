{#
Builds a JSON-like object by aggregating key value pair columns.

Arguments:
    key_column: column to be used as the key
    value_column: column to be used as the value

Notes:
    The resulting column is an object type in Snowflake and a variant type in Databricks.

#}

{% macro json_object_agg(key_column, value_column) %}
    {{ return(adapter.dispatch('json_object_agg', 'edu_edfi_source')(key_column, value_column)) }}
{% endmacro %}

{% macro snowflake__json_object_agg(key_column, value_column) -%}
    object_agg({{ key_column }}, {{ value_column }}::variant)
{%- endmacro %}

{% macro databricks__json_object_agg(key_column, value_column) -%}
    parse_json('{' || listagg('"' || {{ key_column }} || '": "' || {{ value_column }} || '"', ', ') || '}')
{%- endmacro %}
