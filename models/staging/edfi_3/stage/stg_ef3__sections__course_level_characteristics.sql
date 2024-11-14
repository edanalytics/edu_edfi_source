with stg_sections as (
    select * from {{ ref('stg_ef3__sections') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_course_offering,
        k_course_section,
        {{ extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }} as course_level_characteristic
    from stg_sections
        {{ json_flatten('v_course_level_characteristics', outer=True) }}
)
select * from flattened
