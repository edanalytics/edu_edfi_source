with students as (
    select * from {{ ref('stg_ef3__students') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        {{ extract_descriptor('value:otherNameTypeDescriptor::string') }} as other_name_type,
        value:personalTitlePrefix::string as personal_title_prefix,
        value:firstName::string as first_name,
        value:middleName::string as middle_name,
        value:lastSurname::string as last_surname,
        value:generationCodeSuffix::string as generation_code_suffix
    from students
        {{ json_flatten('v_other_names') }}
)

select * from flattened
