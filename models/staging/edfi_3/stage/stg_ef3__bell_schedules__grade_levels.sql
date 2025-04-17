with stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_bell_schedule,
        k_school,
        {{ extract_descriptor('value:gradeLevelDescriptor::string') }} as grade_level
    from stg_bell_schedules
        {{ json_flatten('v_grade_levels') }}
)
select * from flattened
