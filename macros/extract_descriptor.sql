{# grab descriptor codes from namespaced descriptor values #}
{% macro extract_descriptor(col) -%}
    
    {%- set stripped_col = col.split(":")[-3] -%}
    {%- set config = var('descriptors', {}).get(stripped_col) or None -%}
    {%- set replace_with = config['replace_with'] or None -%}

    {# if not configured to replace (default), split part from raw value #}
    {%- if not replace_with %}

      split_part({{ col }}, '#', -1)

    {%- else %}
    {#- if configured to replace, query int_ef3__deduped_descriptors to find each value's short description -#}
      
      {% set query_descriptors -%}
        select namespace, code_value, {{ replace_with }}
        from {{ ref('int_ef3__deduped_descriptors') }}
        where lower(split_part(namespace, '/', -1)) = lower('{{stripped_col}}')
      {%- endset -%}

      {%- set descriptor_xwalk = run_query(query_descriptors) %}

      {%- if execute -%}
        {%- set namespaces = descriptor_xwalk.columns[0].values() -%}
        {%- set code_values = descriptor_xwalk.columns[1].values() -%}
        {%- set descriptions = descriptor_xwalk.columns[2].values() -%}
      {%- endif -%}
      
      {# create a case/when statement that replaces each raw code_value with short_description #}
      case
      {% for i in range(code_values|length) -%}

        when split_part({{ col }}, '#', 1) = '{{namespaces[i]}}' and split_part({{ col }}, '#', -1) = '{{code_values[i]}}'
          then '{{descriptions[i]}}'

      {% endfor %}
        {# default to raw value if not found in descriptors table #}
        else split_part({{ col }}, '#', -1)
        end       
    {%- endif -%}
{%- endmacro %}