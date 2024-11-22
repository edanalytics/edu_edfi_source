{#
Build a dictionary- or struct-like object from key value pairs.

Arguments:
    paired_list: A list of length-2, key-value lists
    is_terminal: Is this the outermost function call? See notes

Notes:
    This macro is more complex than it otherwise would be due to missing
    functionality in Databricks.

    Snowflake has an object_construct function that returns a key-value variant,
    but Databricks only has named_struct, which returns a typed-struct that 
    cannot be coerced to variant.
    Instead we wrap it in to_json to convert the array to a JSON string,
    and then call parse_json, to turn that string into a variant. However if 
    we are constructing a sequence of nested object_construct and/or array_agg
    function calls, we can't transform back to variant until the outermost call.
    Hence: is_terminal is the option that specifies which call is outermost in 
    the chain.

    If Databricks either makes structs coercible to variants or 
    creates a variant-returning version of object_construct, this macro can get simpler.
#}

{% macro json_object_construct(paired_list, is_terminal=False) %}
    {{ return(adapter.dispatch('json_object_construct', 'edu_edfi_source')(paired_list, is_terminal)) }}
{% endmacro %}

{% macro snowflake__json_object_construct(paired_list, is_terminal) -%}
    object_construct(
        {%- for item in paired_list %}
            '{{ item[0] }}', {{ item[1] }} {% if not loop.last %}, {% endif %}
        {% endfor -%}
    )
{%- endmacro %}

{% macro databricks__json_object_construct(paired_list, is_terminal) -%}
    {% if is_terminal %}
    parse_json(to_json(
    {% endif %}
    named_struct(
        {%- for item in paired_list %}
            '{{ item[0] }}', {{ item[1] }} {% if not loop.last %}, {% endif %}
        {% endfor -%}
    ){% if is_terminal %})) {% endif %}
{%- endmacro %}
