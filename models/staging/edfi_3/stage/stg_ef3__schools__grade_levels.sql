with stage_schools as (
    select * from {{ ref('stg_ef3__schools') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_school,
        {{ extract_descriptor('value:gradeLevelDescriptor::string') }} as grade_level
    from stage_schools
        , lateral flatten(input=>v_grade_levels)
)
select * from flattened