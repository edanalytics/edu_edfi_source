with stage_stu_programs as (
    select * from {{ ref('stg_ef3__student_special_education_program_associations') }}
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

        program_enroll_begin_date,
        {{ extract_descriptor('value:specialEducationProgramServiceDescriptor::string') }} as program_service,
        value:primaryIndicator::boolean as primary_indicator,
        value:providers                 as v_providers,
        value:serviceBeginDate::date    as service_begin_date,
        value:serviceEndDate::date      as service_end_date,

        -- edfi extensions
        value:_ext as v_ext        

    from stage_stu_programs,
        lateral flatten(input => v_special_education_program_services)
),

-- There is a v_ext nested within v_special_education_program_services. Those extensions must be extracted here, not in prev CTE, 
-- because dbt gets confused whether to reference stage_stu_spec_ed.v_ext or v_special_education_program_services.v_ext
extended as (
    select 
        flattened.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from flattened
)

select * from extended
