{% macro flatten_emails(stg_ref, keys) %}

with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {% for key in keys %}
        {{key}}{%- if not loop.last %},{% endif -%}
        {% endfor %}
        k_parent,
        {{ extract_descriptor('value:electronicMailTypeDescriptor::string') }} as email_type,
        lower(value:electronicMailAddress::string) as email_address,
        value:primaryEmailAddressIndicator::boolean as is_primary_email,
        value:doNotPublishIndicator::boolean as do_not_publish
    from stg
        , lateral flatten(input=>v_electronic_mails)
)
select * from flattened
{% endmacro %}