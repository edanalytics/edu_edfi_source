with stage_stu_programs as (
    select * from {{ ref('stg_ef3__student_language_instruction_program_associations') }}
),

flattened as (
    select
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,

        {{ extract_descriptor('value:monitoredDescriptor::string') }} as monitored_status,
        {{ extract_descriptor('value:participationDescriptor::string') }} as participation_status,
        {{ extract_descriptor('value:proficiencyDescriptor::string') }} as proficiency_level,
        {{ extract_descriptor('value:progressDescriptor::string') }} as yearly_progress,

        -- edfi extensions
        value:_ext as v_ext

    from stage_stu_programs
        {{ json_flatten('v_english_language_proficiency_assessments') }}
)
select * from flattened
