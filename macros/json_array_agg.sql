{#
Aggregate rows into a variant array, based on any inner expression.

Arguments:
    expression: Any SQL expression to roll up into an array
    window: Optional window expression for the aggregation
    order_by: Optional sort order expression for the array
    is_terminal: Is this the outermost function call in a chain? See notes

Notes:
    This macro is more complex than it would otherwise be due to missing
    functionality in Databricks. 

    Snowflake has an array_agg function that returns a variant, but 
    Databricks' array_agg returns a bare array, which (depending on the 
    contents of the array) cannot be coerced to variant. 
    Instead we wrap it in to_json to convert the array to a JSON string,
    and then call parse_json, to turn that string into a variant. However if 
    we are constructing a sequence of nested object_construct and/or array_agg
    function calls, we can't transform back to variant until the outermost call.
    Hence: is_terminal is the option that specifies which call is outermost in 
    the chain.

    If Databricks either makes structs coercible to variants or 
    creates a variant-returning version of array_agg, this macro can get simpler.

#}

{% macro json_array_agg(expression, window='', order_by=none, is_terminal=False) %}
    {{ return(adapter.dispatch('json_array_agg', 'edu_edfi_source')(expression, window, order_by, is_terminal)) }}
{% endmacro %}

{% macro snowflake__json_array_agg(expression, window, order_by, is_terminal) -%}
    array_agg({{ expression}}) 
    {%- if order_by is not none %} 
        within group (order by {{ order_by }}) 
    {%- endif %}
    {{ window }}
{%- endmacro %}

{% macro databricks__json_array_agg(expression, window, order_by, is_terminal) -%}
    {%- if is_terminal -%}
    parse_json(to_json(
    {%- endif %}
    {%- if order_by is not none %} 
    sort_index(
    {%- endif %}
    array_agg({{ expression}})
    {{ window }}
    {%- if order_by is not none %}) {% endif %}
    {% if is_terminal %})) {% endif %}
{%- endmacro %}
