{#
Get a record from a JSON object case-insensitively.

Snowflake's variant type has a native function for this, but Databricks' does not.
However Databricks' native JSON type does, so we type coerce to achieve the same effect.

Arguments:
    column: The variant column from which the value should be extracted.
    value: The value you wish to extract.
#}

{% macro json_ci_get(column, value) %}
    {{ return(adapter.dispatch('json_ci_get', 'edu_edfi_source')(column, value)) }}
{% endmacro %}

{% macro snowflake__json_ci_get(column, value) -%}
    get_ignore_case({{ column }}, '{{ value }}')
{%- endmacro %}

{% macro databricks__json_ci_get(column, value) -%}
    (to_json({{ column }})):{{ value }}
{%- endmacro %}
