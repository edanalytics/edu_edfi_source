
with certification_exams as (
    select * from {{ ref('base_tpdm__certification_exams') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'certification_exam_date',
            'lower(certification_exam_id)',
            'lower(namespace)']
        ) }} as k_certification_exam,
        certification_exams.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from certification_exams
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_certification_exam',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
