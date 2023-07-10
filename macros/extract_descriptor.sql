-- grab descriptor codes from namespaced descriptor values
-- TODO decide how data should be returned, some options:
--   a) one value, either code value, long, or short description, based on config
--   b) if configured, create all three columns (sometimes only two?)
--   c) one value either code value, or nested json object with all three, based on config 
{% macro extract_descriptor(col) -%}
    
    {%- set stripped_col = col.split(":")[1] -%}
    {%- set config = var('descriptors')[stripped_col] or None -%}
    {%- set include_descriptions = config['include_descriptions'] or False -%}
    {%- set alias = config['alias'] -%}

    {%- if not include_descriptions -%}
      split_part({{ col }}, '#', -1)
    {%- elif include_descriptions and not alias -%}
      {{ exceptions.raise_compiler_error("Error in extract_descriptor(): if include_descriptions is set, must define an alias") }}
    {%- else %}
      -- TODO split this into a separate macro below, once we decide how data should be returned
      
      {% set query_descriptors -%}
        select namespace, code_value, description, short_description
        from {{ ref('int_ef3__deduped_descriptors') }}
        where lower(split_part(namespace, '/', -1)) = lower('{{stripped_col}}')
      {%- endset -%}
      {%- set descriptor_xwalk = run_query(query_descriptors) %}

      {%- if execute -%}
        {%- set namespaces = descriptor_xwalk.columns[0].values() -%}
        {%- set code_values = descriptor_xwalk.columns[1].values() -%}
        {%- set descriptions = descriptor_xwalk.columns[2].values() -%}
        {%- set short_descriptions = descriptor_xwalk.columns[3].values() -%}
      {%- endif -%}
      
      case
      {%- for i in range(code_values|length) %}
        when split_part({{ col }}, '#', 1) = '{{namespaces[i]}}' and split_part({{ col }}, '#', -1) = '{{code_values[i]}}'
          then '{{short_descriptions[i]}}'
      {%- endfor %}
        end       
    {%- endif -%}
{%- endmacro %}