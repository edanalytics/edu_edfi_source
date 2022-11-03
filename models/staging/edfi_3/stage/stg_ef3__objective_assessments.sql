with base_obj_assessments as (
    select * from {{ ref('base_ef3__objective_assessments') }}
    where not is_deleted
),
stg_assessments as (
    select 
        assessment_identifier,
        namespace,
        academic_subject
    from {{ ref('stg_ef3__assessments') }}
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(stg_assessments.academic_subject)',
            'lower(base_obj_assessments.assessment_identifier)',
            'lower(base_obj_assessments.namespace)',
            'lower(objective_assessment_identification_code)']
        ) }} as k_objective_assessment,
        {{ gen_skey('k_assessment', extras = ['stg_assessments.academic_subject']) }},
        base_obj_assessments.*,
        stg_assessments.academic_subject
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_obj_assessments
    -- point of this is to cross join objective assessments against the academic subjects to add to gen_skey
    join stg_assessments 
        on base_obj_assessments.assessment_identifier = stg_assessments.assessment_identifier
        and base_obj_assessments.namespace = stg_assessments.namespace
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