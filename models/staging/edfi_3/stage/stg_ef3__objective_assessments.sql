with base_obj_assessments as (
    select * from {{ ref('base_ef3__objective_assessments') }}
),
stage_student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
-- need to determine which subject(s) each objective assess id code is associated to
-- based on student results
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
join_subject as (
    select
        base_obj_assessments.*,
        distinct_obj_subject.academic_subject as academic_subject,
        -- prefer subject directly from obj assessment, else use studentAssess value
        coalesce(base_obj_assessments.academic_subject_descriptor, distinct_obj_subject.academic_subject) as obj_assess_academic_subject
    from base_obj_assessments
    -- this join will drop objective assessments with no student results
    join distinct_obj_subject 
        on base_obj_assessments.tenant_code = distinct_obj_subject.tenant_code
        and base_obj_assessments.api_year = distinct_obj_subject.api_year
        and base_obj_assessments.assessment_identifier = distinct_obj_subject.assessment_identifier
        and base_obj_assessments.namespace = distinct_obj_subject.namespace
        and base_obj_assessments.objective_assessment_identification_code = distinct_obj_subject.objective_assessment_identification_code
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(academic_subject)',
            'lower(obj_assess_academic_subject)',
            'lower(assessment_identifier)',
            'lower(namespace)',
            'lower(objective_assessment_identification_code)']
        ) }} as k_objective_assessment,
        {{ gen_skey('k_assessment', extras = ['academic_subject']) }},
        join_subject.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from join_subject
    
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_objective_assessment',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
),
{# Rename obj_assess_academic_subject --> academic_subject for human-readability and to avoid breaking change to warehouse. academic_subject above represents 'OVERALL' assessment subject, so that the gen_skey() call works. #}
renamed as (
  select 
    deduped.* RENAME (academic_subject as assess_academic_subject, obj_assess_academic_subject as academic_subject)
  from deduped
)
select * from renamed
where not is_deleted
