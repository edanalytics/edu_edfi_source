with stage_stu_programs as (
    select * from {{ ref('stg_ef3__student_cte_program_associations') }}
),

flattened as (
    select, 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,

        program_enroll_begin_date,
        program_enroll_end_date,
        {{ extract_descriptor('value:CTEProgramServiceDescriptor::string') }} as program_service,
        value:PrimaryIndicator::boolean         as primary_indicator, 
        value:ServiceBeginDate::date            as service_begin_date
        value:ServiceEndDate::date              as service_end_date,
        value:CIPCode::string                   as cip_code

        -- edfi extensions
        value:_ext as v_ext

    from stage_stu_programs,
        lateral flatten(input => v_cte_program_services)
)

select * from flattened