{#
Extract elements from JSON fields.

Arguments:
    path: A JSON path, in dot or bracket notation.
    type: The datatype for the result.
    column: The column containing JSON. Default 'v'
    flattened: Set to true if the JSON column is from a flattened array (Snowflake only)

Notes:
JSON Paths should be expressed in dot notation. Reserve brackets for integers when subsetting lists.
The Path is essentially passed straight through, so consult the JSON path documentation
for your database platform.

JSON Path examples:
    'a' -- top level object called a
    'a.b' -- nested object like {"a": {"b": 1}}
    'a[0].b' -- object b from element 0 of a

Flattened:
    In Snowflake, a Flatten operation returns a table, which always contains
    and element called 'value'.
    In Databricks, Explode returns a single column.
    Thus, if we've aliased the flattened object to x, we have this syntax difference:
    Snowflake: x.value:[json path]
    Databricks: x:[json path]
    
    This approach is relatively clumsy, but it works.

#}
{% macro json_extract(path, type='', column='v') %}
    {{ return(adapter.dispatch('json_extract', 'edu_edfi_source')(path, type, column)) }}
{%  endmacro %}

{% macro snowflake__json_extract(path, type, column) -%}

{{ column }}:{{ path }}{% if type != '' %}::{{type}}{% endif %}

{%- endmacro %}


{% macro databricks__json_extract(path, type, column, flattened) -%}

{{ column }}:{{ path }}{% if type != '' %}::{{type}}{% endif %}

{%- endmacro %}
