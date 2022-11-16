-- flatten emails
{% macro flatten_emails(stg_ref, keys) %}
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
        {{ extract_descriptor('value:electronicMailTypeDescriptor::string') }} as email_type,
        lower(value:electronicMailAddress::string)                             as email_address,
        value:primaryEmailAddressIndicator::boolean                            as is_primary_email,
        value:doNotPublishIndicator::boolean                                   as do_not_publish
    from stg
        , lateral flatten(input=>v_electronic_mails)
)
select * from flattened
{% endmacro %}

-- flatten telephones
{% macro flatten_telephones(stg_ref, keys) %}
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
        {{ extract_descriptor('value:telephoneNumberTypeDescriptor::string') }} as phone_number_type,
        value:telephoneNumber::string                                           as phone_number,
        value:orderOfPriority::int                                              as priority_order,
        value:doNotPublishIndicator::boolean                                    as do_not_publish,
        value:textMessageCapabilityIndicator::boolean                           as is_text_message_capable
    from stg
        , lateral flatten(input=>v_telephones)
)
select * from flattened
{% endmacro %}