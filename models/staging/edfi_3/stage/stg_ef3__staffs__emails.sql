with stg_staffs as (
    select * from {{ ref('stg_ef3__staffs') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_staff,
        {{ extract_descriptor('value:electronicMailTypeDescriptor::string') }} as email_type,
        lower(value:electronicMailAddress::string) as email_address,
        value:primaryEmailAddressIndicator::boolean as is_primary_email,
        value:doNotPublishIndicator::boolean as do_not_publish
    from stg_staffs
        , lateral flatten(input=>v_electronic_mails)
)
select * from flattened