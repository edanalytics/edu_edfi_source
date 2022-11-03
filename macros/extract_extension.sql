{#
-- this macro reads a list of configured extensions from `var`, for a given a model name, e.g. stg_ef3__students
-- when flatten = True (use case: stg models):
  -- for each extension, use the configured SQL to retrieve a nested column name, and cast to the correct data type
-- when flatten = False (use case: wh models):
  -- for each extension, just return the col name
#}
{% macro extract_extension(model_name, flatten) %}
{% set extensions =  var('extensions')[model_name]  %}

{%- if extensions is defined and extensions|length > 0 %},{% endif -%}

{% for ext in extensions %}
  {%- set ext_native_name =  extensions[ext].name  -%}
  {%- set ext_dtype =  extensions[ext].dtype  -%}
  {%- if flatten %}
    v_ext:{{ext_native_name}}::{{ext_dtype}} as {{ext}}
  {%- else -%}
    {{ext}}
  {%- endif %}
  {%- if not loop.last %},{% endif -%}
{% endfor %}


{% endmacro %}
