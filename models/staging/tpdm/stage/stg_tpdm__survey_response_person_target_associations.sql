with base_survey_response_person_target_associations as (
    select * from {{ ref('base_tpdm__survey_response_person_target_associations') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year'
            ]
        ) }} as k_survey_response_person_target_association,
        base_survey_response_person_target_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_survey_response_person_target_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_response_person_target_association',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
