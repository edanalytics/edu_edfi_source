with certification_exam_results as (
    select * from {{ ref('base_tpdm__certification_exam_results') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'certification_exam_date',
            'lower(certification_exam_id)',
            'lower(namespace)',
            'lower(person_id)',
            'lower(source_system)']
        ) }} as k_certification_exam_result,
        {{ gen_skey('k_person') }},
        {{ gen_skey('k_certification_exam') }},
        certification_exam_results.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from certification_exam_results
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_certification_exam_result',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
