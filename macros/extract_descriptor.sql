-- grab descriptor codes from namespaced descriptor values
{% macro extract_descriptor(col) -%}
    split_part({{ col }}, '#', -1)
{%- endmacro %}