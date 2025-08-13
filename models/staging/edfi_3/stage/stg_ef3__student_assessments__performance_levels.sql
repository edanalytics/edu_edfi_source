with stage_student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student_assessment,
        -- used for xwalk in wh
        assessment_identifier,
        namespace,
        {{ extract_descriptor('value:assessmentReportingMethodDescriptor::string') }} as performance_level_name,
        {{ extract_descriptor('value:performanceLevelDescriptor::string') }} as performance_level_result
    from stage_student_assessments
        {{ json_flatten('v_performance_levels') }}
)
select * from flattened
