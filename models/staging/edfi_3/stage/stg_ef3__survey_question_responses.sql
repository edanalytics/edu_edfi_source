with base_survey_question_responses as (
    select * from {{ ref('base_ef3__survey_question_responses') }}
    where not is_deleted
),
keyed as (
    select 
        {{ gen_skey('k_survey', 'survey_question_reference') }}
        {{ gen_skey('k_survey_question') }},
        {{ gen_skey('k_survey_response') }},
        base_survey_question_responses.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_survey_question_responses
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_question, k_survey_response',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
