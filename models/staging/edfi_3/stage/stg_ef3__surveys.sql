with base_surveys as (
    select * from {{ ref('base_ef3__surveys') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(namespace)',
                'lower(survey_id)'
            ]
        ) }} as k_survey,
        base_surveys.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_surveys
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey', 
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
                  