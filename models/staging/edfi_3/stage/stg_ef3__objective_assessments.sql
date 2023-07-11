with base_obj_assessments as (
    select * from {{ ref('base_ef3__objective_assessments') }}
    where not is_deleted
),
stage_student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
distinct_obj_subject as (
    select distinct
        tenant_code,
        api_year,
        assessment_identifier,
        namespace,
        academic_subject,
        value:objectiveAssessmentReference:identificationCode::string as objective_assessment_identification_code
    from stage_student_assessments,
        lateral flatten(input => v_student_objective_assessments)
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            ['base_obj_assessments.tenant_code',
            'base_obj_assessments.api_year',
            'lower(distinct_obj_subject.academic_subject)',
            'lower(base_obj_assessments.assessment_identifier)',
            'lower(base_obj_assessments.namespace)',
            'lower(base_obj_assessments.objective_assessment_identification_code)']
        ) }} as k_objective_assessment,
        {{ gen_skey('k_assessment', extras = ['distinct_obj_subject.academic_subject']) }},
        base_obj_assessments.*,
        distinct_obj_subject.academic_subject
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_obj_assessments
    -- point of this is to cross join objective assessments against the academic subjects to add to gen_skey
    join distinct_obj_subject 
        on base_obj_assessments.tenant_code = distinct_obj_subject.tenant_code
        and base_obj_assessments.api_year = distinct_obj_subject.api_year
        and base_obj_assessments.assessment_identifier = distinct_obj_subject.assessment_identifier
        and base_obj_assessments.namespace = distinct_obj_subject.namespace
        and base_obj_assessments.objective_assessment_identification_code = distinct_obj_subject.objective_assessment_identification_code
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_objective_assessment',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped