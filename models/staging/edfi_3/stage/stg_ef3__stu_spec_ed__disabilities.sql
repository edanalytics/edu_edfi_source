with stg_stu_spec_ed_org as (
    select * from {{ ref('stg_ef3__student_special_education_program_associations') }}
),
flattened as (
    select
        tenant_code,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,
        program_enroll_begin_date,
        program_enroll_end_date,
        school_year,
        disab.value:disabilitySetBeginDate::date as begin_date,
        disab.value:disabilitySetEndDate::date as end_date,
        {{ extract_descriptor('disab.value:disabilityDescriptor::string') }} as disability_type,
        {{ extract_descriptor('disab.value:disabilityDeterminationSourceTypeDescriptor::string') }} as disability_source_type,
        disab.value:disabilityDiagnosis::string as disability_diagnosis,
        disab.value:orderOfDisability::int as order_of_disability,
        disab.value:designations as v_designations
    from stg_stu_spec_ed_org
        , lateral flatten(input=>v_disabilities) disab
)
select * from flattened
