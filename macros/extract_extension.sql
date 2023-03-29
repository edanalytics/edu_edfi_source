{#
-- this macro reads a list of configured extensions from `var`, for a given a model name, e.g. stg_ef3__students
-- when flatten = True (use case: stg models):
  -- for each extension, use the configured SQL to retrieve a nested column name, and cast to the correct data type
-- when flatten = False (use case: wh models):
  -- for each extension, just return the col name
#}
{% macro extract_extension(model_name, flatten) %}

{# If `model_name` is a singleton, convert to a list. #}
{% if model_name is string %}
    {% set model_name = [model_name] %}
{% endif %}

{% set extensions = [] -%}
{%- for relation in model_name -%}
    {%- for extension in var('extensions')[relation] -%}
        {%- do extensions.append(extension) -%}
    {%- endfor -%}
{%- endfor %}

{%- if extensions is defined and extensions|length > 0 -%},{% endif -%}

{%- for ext in extensions|unique %}
    {%- set ext_native_name =  extensions[ext].name  -%}
    {%- set full_ext_native_name =  'v_ext:' + ext_native_name  -%}
    {%- set ext_dtype =  extensions[ext].dtype  -%}
    {%- set ext_extract_descriptor =  extensions[ext].extract_descriptor -%}

    {%- if flatten %}
        {%- if ext_extract_descriptor %}
            {{extract_descriptor(full_ext_native_name)}}::{{ext_dtype}} as {{ext}}
        {%- else %}
            {{full_ext_native_name}}::{{ext_dtype}} as {{ext}}
        {%- endif %}

    {%- else %}
        {{ext}}

    {%- endif %}
    {%- if not loop.last %},{% endif -%}

{%- endfor -%}

{% endmacro %}
