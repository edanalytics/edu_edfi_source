-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{#
Build an array-like JSON object containing a single value.

Arguments:
    value: The value to include in the array

Notes:
    This macro is more complex than it otherwise would be due to differences
    in how Snowflake and Databricks construct arrays containing VARIANT values.

    Snowflake''s array_construct function returns an array that can be used
    directly with VARIANT data. Databricks'' array function instead returns a
    typed ARRAY, such as ARRAY<VARIANT>, which cannot be passed to functions
    that expect a VARIANT containing a JSON array.
    Instead we construct the array as JSON and call parse_json to return it
    as a VARIANT.

    If Databricks adds a function that constructs a VARIANT containing an
    array directly, this macro can get simpler.
#}

{% macro json_array_construct(value) %}
    {{ return(adapter.dispatch('json_array_construct', 'edu_edfi_source')(value)) }}
{% endmacro %}

{% macro snowflake__json_array_construct(value) %}
    array_construct({{ value }})
{% endmacro %}

{% macro databricks__json_array_construct(value) %}
    parse_json(concat('[', to_json({{ value }}), ']'))
{% endmacro %}
