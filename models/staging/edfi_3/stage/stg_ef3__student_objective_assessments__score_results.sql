with stage_student_objective_assessments as (
    select * from {{ ref('stg_ef3__student_objective_assessments') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student_objective_assessment,
        -- used for xwalk in wh
        assessment_identifier,
        namespace,
        objective_assessment_identification_code,
        {{ extract_descriptor('value:assessmentReportingMethodDescriptor::string') }} as score_name,
        value:result::string as score_result
    from stage_student_objective_assessments,
        lateral flatten(input=>v_score_results)
)
select * from flattened
