with base_survey_section_aggregate_responses as (
    select * from {{ ref('base_tpdm__survey_section_aggregate_responses') }}
),
keyed as (
    select 
        {{ gen_skey('k_evaluation_element_rating') }},  
        {{ gen_skey('k_survey_section') }},  
        base_survey_section_aggregate_responses.*
        {{ extract_extension(model_name=this.name, flatten=True) }}  
    from base_survey_section_aggregate_responses
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_section_response, k_survey_section',
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
