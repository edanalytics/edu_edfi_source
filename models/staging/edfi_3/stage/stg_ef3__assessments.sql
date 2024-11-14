with base_assessments as (
    select *
    from {{ ref('base_ef3__assessments') }}
    where not is_deleted
),
flatten as (
    select
        {{ extract_descriptor('value:academicSubjectDescriptor::string') }} as academic_subject,
        base_assessments.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_assessments
        {{ json_flatten('v_academic_subjects') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(academic_subject)',
            'lower(assessment_identifier)',
            'lower(namespace)']
        ) }} as k_assessment,
        *
    from flatten
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_assessment',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
