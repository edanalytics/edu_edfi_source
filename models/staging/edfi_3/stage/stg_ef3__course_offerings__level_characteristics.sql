with stg_course_offerings as (
    select * from {{ ref('stg_ef3__course_offerings') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_course_offering,
        k_course,
        {{ extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }} as course_level_characteristic
    from stg_course_offerings
        {{ json_flatten('v_course_level_characteristics', outer=True) }}
)
select * from flattened
