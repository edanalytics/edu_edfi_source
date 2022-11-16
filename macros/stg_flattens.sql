{% macro flatten_stg(stg_ref, keys, array_name) %}

{% set array_defs = {
        'v_electronic_mails': {
            'cols': [["value:electronicMailTypeDescriptor::string", 'email_type'],
                     ["lower(value:electronicMailAddress::string)", 'email_address'],
                     ["value:primaryEmailAddressIndicator::boolean", 'is_primary_email']
                     ["value:doNotPublishIndicator::boolean", 'do_not_publish']]
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
            {% if 'Descriptor' in col[0] %}
                {{ extract_descriptor(col[0]) }}
            {% else %}
                {{col[0]}}
            {% endif %}
        as {{col[1]}}
        {%- if not loop.last %},{% endif -%}
        {% endfor %}
    from stg
        , lateral flatten(input=>{{array_name}})
)
select * from flattened
{% endmacro %}