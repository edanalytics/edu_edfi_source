with stg_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        ed_org_id,
        k_lea,
        k_school,
        {{ extract_descriptor('value:electronicMailTypeDescriptor::string') }} as email_type,
        lower(value:electronicMailAddress::string) as email_address,
        value:primaryEmailAddressIndicator::boolean as is_primary_email,
        value:doNotPublishIndicator::boolean as do_not_publish
    from stg_stu_ed_org
        , lateral flatten(input=>v_electronic_mails)
)
select * from flattened