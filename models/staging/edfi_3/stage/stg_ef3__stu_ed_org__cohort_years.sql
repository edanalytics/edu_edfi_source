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
        {{ extract_descriptor('value:cohortYearTypeDescriptor::string') }} as cohort_year,
        value:schoolYearTypeReference:schoolYear::string as school_year,
        {{ extract_descriptor('value:termDescriptor::string') }} as term
    from stage_stu_ed_org
        , lateral flatten(input=>v_cohort_years)
)
select * from flattened