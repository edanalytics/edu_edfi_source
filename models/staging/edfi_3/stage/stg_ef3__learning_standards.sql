with base_learning_standards as (
    select * from {{ ref('base_ef3__learning_standards') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'learning_standard_id']
        ) }} as k_learning_standard,
        base_learning_standards.*,
        {{ extract_descriptor('ls_academic_subject.value:academicSubjectDescriptor::string') }} as academic_subject_descriptor
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_learning_standards,
        lateral flatten(input=>v_academic_subjects, outer=>true) as ls_academic_subject
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_learning_standard',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped