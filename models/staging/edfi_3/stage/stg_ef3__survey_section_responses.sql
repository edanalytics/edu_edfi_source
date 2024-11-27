with survey_section_responses as (
    select * from {{ ref('base_ef3__survey_section_responses') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(survey_id)',
            'lower(namespace)',
            'lower(survey_section_title)']
        ) }} as k_survey_section_response,
        {{ gen_skey('k_survey_response') }},
        {{ gen_skey('k_survey_section') }},
        survey_section_responses.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from survey_section_responses
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_section_response',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
