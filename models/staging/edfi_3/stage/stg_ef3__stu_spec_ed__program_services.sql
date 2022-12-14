with stage_stu_spec_ed as (
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
        spec_ed_program_begin_date,
        {{ extract_descriptor('value:specialEducationProgramServiceDescriptor::string') }} as special_education_program_service,
        value:primaryIndicator::boolean as primary_indicator,
        value:providers                 as v_providers,
        value:serviceBeginDate::date    as service_begin_date,
        value:serviceEndDate::date      as service_end_date,
        -- edfi extensions
        value:_ext as v_ext
        

    from stage_stu_spec_ed
        , lateral flatten(input=>v_special_education_program_services) 
)
select * from flattened

