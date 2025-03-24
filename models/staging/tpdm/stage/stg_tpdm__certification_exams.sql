with certification_exams as (
    select * from {{ ref('base_tpdm__certification_exams') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
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
            order_by='last_modified_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
