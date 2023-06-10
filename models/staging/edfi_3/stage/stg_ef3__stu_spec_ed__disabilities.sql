with stg_stu_spec_ed_org as (
    select * from {{ ref('stg_ef3__student_special_education_program_associations') }}
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
        {{ extract_descriptor('disab.value:disabilityDescriptor::string') }} as disability_type,
        {{ extract_descriptor('disab.value:disabilityDeterminationSourceTypeDescriptor::string') }} as disability_source_type,
        disab.value:disabilityDiagnosis::string as disability_diagnosis,
        disab.value:orderOfDisability::int as order_of_disability,
        -- todo: perhaps these would better serve as wide booleans
        -- in which case we would not want to double-flatten, but leave nested
        -- for a downstream step
        {{ extract_descriptor('desig.value:disabilityDesignationDescriptor::string') }} as disability_designation
    from stg_stu_spec_ed_org
        , lateral flatten(input=>v_disabilities) disab
        , lateral flatten(input=>disab.value:designations, OUTER => TRUE) as desig
)
select * from flattened
