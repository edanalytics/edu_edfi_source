with stg_courses as (
    select * from {{ ref('stg_ef3__courses') }}
), 
flattened as (
    select
        tenant_code,
        api_year,
        k_course,
        {{ extract_descriptor('value:academicSubjectDescriptor::string') }} as academic_subject
    from stg_courses
        {{ json_flatten('v_academic_subjects') }}
)
select *
from flattened
