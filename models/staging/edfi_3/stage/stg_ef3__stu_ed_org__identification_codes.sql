with stage_stu_ed_org as (
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
        {{extract_descriptor(json_extract('ids', 'string', 'ids', True))}} as id_system,
        {{json_extract('assigningOrganizationIdentificationCode', 'string', 'ids', True)}} as assigning_org,
        {{json_extract('identificationCode', 'string', 'ids', True)}} as id_code
    from stage_stu_ed_org
        {{ json_flatten('v_student_identification_codes', 'ids') }}
)
select * from flattened
