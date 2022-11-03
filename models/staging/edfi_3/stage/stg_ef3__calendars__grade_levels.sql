with stg_calendars as (
    select * from {{ ref('stg_ef3__calendars') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_school_calendar,
        {{ extract_descriptor('value:gradeLevelDescriptor::string') }} as grade_level
    from stg_calendars,
        lateral flatten(input => v_grade_levels)
)
select * from flattened