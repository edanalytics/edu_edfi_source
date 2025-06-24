{#
Get the first record from a JSON object.

Snowflake's variant type can be accessed with [] syntax, but Databricks' cannot.
Databricks' variant must be first cast to an array, which required explicit typecasting
using the array<type> syntax, which Snowflake does not support.

TO DO: for now we only access the first record. Should this be json_positional_get instead?

Arguments:
    column: The variant column from which the value should be extracted.
    type: The value you wish to extract.
#}

{% macro json_get_first(column, type) %}
    {{ return(adapter.dispatch('json_get_first', 'edu_edfi_source')(column, type)) }}
{% endmacro %}

{% macro snowflake__json_get_first(column, type) -%}
    {{ column }}[0]::{{ type }}
{%- endmacro %}

{% macro databricks__json_get_first(column, type) -%}
    ({{ column }}::array<{{ type }}>)[0]
{%- endmacro %}
