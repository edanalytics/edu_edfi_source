with stage_student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
stage_obj_assessments as (
    select * from {{ ref('stg_ef3__objective_assessments') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        k_student_assessment,
        k_assessment,
        k_student,
        k_student_xyear,
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
-- join to get subject from stg obj assess (if not null)
joined as (
    select
      flattened.* exclude(academic_subject),
      coalesce(stage_obj_assessments.obj_assess_academic_subject, flattened.academic_subject) as academic_subject
    from flattened
    join stage_obj_assessments
      on flattened.tenant_code = stage_obj_assessments.tenant_code
      and flattened.api_year = stage_obj_assessments.api_year
      and flattened.assessment_identifier = stage_obj_assessments.assessment_identifier
      and flattened.namespace = stage_obj_assessments.namespace
      and flattened.objective_assessment_identification_code = stage_obj_assessments.objective_assessment_identification_code
),
keyed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(academic_subject)',
            'lower(assessment_identifier)',
            'lower(namespace)',
            'lower(objective_assessment_identification_code)',
            'lower(student_assessment_identifier)']
        ) }} as k_student_objective_assessment,
        {{ gen_skey('k_objective_assessment', extras = ['academic_subject']) }},
        k_student_assessment,
        k_assessment,
        k_student,
        k_student_xyear,
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
    from joined
),
-- todo: we already dedupe in student assessments so this is actually only necessary if we think there
    -- could be dupes in the objective assessments list
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_objective_assessment',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select *
from deduped
