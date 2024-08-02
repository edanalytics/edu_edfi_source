with credentials as (
    select * from {{ ref('base_ef3__credentials') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(namespace)',
            'lower(person_id)',
            'lower(source_system)',
            'lower(survey_id)',
            'lower(survey_response_id)']
        ) }} as k_credential,
        {{ gen_skey('k_student_academic_record')}},
        credentials.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from credentials
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_credential',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
