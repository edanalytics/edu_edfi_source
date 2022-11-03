with stage_student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        k_student_assessment,
        k_assessment,
        k_student,
        student_assessment_identifier,
        assessment_identifier,
        namespace,
        academic_subject,
        school_year,
        administration_date,
        administration_end_date,
        event_description,
        administration_environment,
        administration_language,
        event_circumstance,
        platform_type,
        reason_not_tested,
        retest_indicator,
        when_assessed_grade_level,
        value:objectiveAssessmentReference:identificationCode::string as objective_assessment_identification_code,
        value:objectiveAssessmentReference as objective_assessment_reference,
        -- unflattened lists
        value:performanceLevels as v_performance_levels,
        value:scoreResults as v_score_results
    from stage_student_assessments,
        lateral flatten(input => v_student_objective_assessments)
),
keyed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(academic_subject)',
            'student_assessment_identifier',
            'objective_assessment_identification_code']
        ) }} as k_student_objective_assessment,
        {{ gen_skey('k_objective_assessment', extras = ['academic_subject']) }},
        k_student_assessment,
        k_assessment,
        k_student,
        student_assessment_identifier,
        assessment_identifier,
        namespace,
        academic_subject,
        objective_assessment_identification_code,
        objective_assessment_reference,
        school_year,
        administration_date,
        administration_end_date,
        event_description,
        administration_environment,
        administration_language,
        event_circumstance,
        platform_type,
        reason_not_tested,
        retest_indicator,
        when_assessed_grade_level,
        v_performance_levels,
        v_score_results
    from flattened
),
-- todo: we already dedupe in student assessments so this is actually only necessary if we think there
    -- could be dupes in the objective assessments list
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_objective_assessment',
            order_by='pull_timestamp desc'
        )
    }}
)
select *
from deduped
