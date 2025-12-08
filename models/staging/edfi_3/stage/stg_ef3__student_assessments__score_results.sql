with stage_student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student_assessment,
        k_assessment,
        -- used for xwalk in wh
        assessment_identifier,
        namespace,
        {{ extract_descriptor('value:assessmentReportingMethodDescriptor::string') }} as score_name,
        value:result::string as score_result,
        pull_timestamp,
        last_modified_timestamp
    from stage_student_assessments
        {{ json_flatten('v_score_results') }}
)
select * from flattened
