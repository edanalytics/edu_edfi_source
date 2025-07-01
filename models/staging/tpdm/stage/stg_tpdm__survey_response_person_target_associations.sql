with survey_response_person_target_associations as (
    select * from {{ ref('base_tpdm__survey_response_person_target_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_survey_response') }},
        {{ gen_skey('k_person') }},
        survey_response_person_target_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from survey_response_person_target_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_response, k_person',
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
