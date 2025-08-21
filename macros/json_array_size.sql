{#
Get the length of a variant array.

Arguments:
    column: Column or path containing the array.
#}

{% macro json_array_size(column) %}
    {{ return(adapter.dispatch('json_array_size', 'edu_edfi_source')(column)) }}
{% endmacro %}

{% macro snowflake__json_array_size(column) -%}
    array_size({{ column }})
{%- endmacro %}

{% macro databricks__json_array_size(column) -%}
    size(try_cast({{ column }} as array<string>))
{%- endmacro %}

{% macro postgres__json_array_size(column) %}
    array_length({{ column }})
{% endmacro %}
