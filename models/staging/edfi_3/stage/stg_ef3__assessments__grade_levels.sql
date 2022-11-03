with stage_assessments as (
    select * from {{ ref('stg_ef3__assessments') }}
),
flattened as (
    select
        tenant_code,
        api_year, 
        k_assessment,
        {{ extract_descriptor('value:gradeLevelDescriptor::string') }} as grade_level
    from stage_assessments,
        lateral flatten(input=>v_assessed_grade_levels)
)
select * from flattened