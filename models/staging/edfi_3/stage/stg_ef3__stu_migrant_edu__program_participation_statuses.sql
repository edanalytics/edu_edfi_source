with stage_stu_programs as (
    select * from {{ ref('stg_ef3__student_migrant_education_program_associations') }}
),

flattened as (
    select
        k_student_program,
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,
        ed_org_id,

        program_enroll_begin_date,
        program_enroll_end_date,
        {{ extract_descriptor('value:participationStatusDescriptor::string') }} as participation_status,
        value:statusBeginDate::date     as status_begin_date,
        value:designatedBy              as designated_by,
        value:statusEndDate::date       as status_end_date,

        -- edfi extensions
        value:_ext as v_ext

    from stage_stu_programs
        {{ json_flatten('v_program_participation_statuses') }}
)

select * from flattened