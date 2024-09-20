with base_survey_questions as (
    select * from {{ ref('base_ef3__survey_questions') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(question_code)',
                'lower(survey_id)' 
            ]
        ) }} as k_survey_question,
        {{ gen_skey('k_survey') }},
        base_survey_questions.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_survey_questions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_question, k_survey', 
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
