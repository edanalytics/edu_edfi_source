{% macro flatten_stg(stg_ref, keys, array_name) %}

{% set array_defs = {
        'v_electronic_mails': {
            'cols': ["{{ extract_descriptor('value:electronicMailTypeDescriptor::string') }}",
                     "lower(value:electronicMailAddress::string)",
                     "value:primaryEmailAddressIndicator::boolean",
                     "value:doNotPublishIndicator::boolean"]
             },
    }
%}

{% set array_def = array_defs[array_name] %}
{% set array_cols = array_def['cols'] %}

with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {% for key in keys %}
        {{key}},
        {% endfor %}
        {% for col in array_cols %}
        {{col}}{%- if not loop.last %},{% endif -%}
        {% endfor %}
    from stg
        , lateral flatten(input=>{{array_name}})
)
select * from flattened
{% endmacro %}