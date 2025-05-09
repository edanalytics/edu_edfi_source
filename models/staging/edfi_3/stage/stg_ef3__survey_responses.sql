with base_survey_responses as (
    select * from {{ ref('base_ef3__survey_responses') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(survey_id)',
                'lower(survey_response_id)'
            ]
        ) }} as k_survey_response,
        {{ gen_skey('k_survey') }},
        {{ gen_skey('k_staff') }},
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_contact') }},
        base_survey_responses.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_survey_responses
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_response',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
