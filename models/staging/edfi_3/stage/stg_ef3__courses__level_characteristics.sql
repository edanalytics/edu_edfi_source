with stg_courses as (
    select * from {{ ref('stg_ef3__courses') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_course,
        {{ extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }} as course_level_characteristic
    from stg_courses,
        lateral flatten(input => v_level_characteristics, outer=>true)
)
select * from flattened