{{ config(
    materialized='incremental',
    unique_key=['k_survey_question', 'k_survey_response'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_survey_question_responses as (
    select * from {{ ref('base_ef3__survey_question_responses') }}

    {% if is_incremental() %}
    -- Only get newly added or deleted records since the last run
    where last_modified_timestamp > (select max(last_modified_timestamp) from {{ this }})
    {% endif %}
),
keyed as (
    select 
        {{ gen_skey('k_survey', 'survey_question_reference') }},
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
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
