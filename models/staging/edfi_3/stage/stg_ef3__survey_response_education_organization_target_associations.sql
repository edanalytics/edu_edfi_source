with survey_response_education_organization_target_associations as (
    select * from {{ ref('base_ef3__survey_response_education_organization_target_associations') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'ed_org_id',
            'lower(namespace)',
            'lower(survey_id)', 
            'lower(survey_response_id)']
        ) }} as k_survey_response_education_organization_target_association,
        {{ edorg_ref(annualize=False) }},
        {{ gen_skey('k_survey_response') }},
        survey_response_education_organization_target_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from survey_response_education_organization_target_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_response_education_organization_target_association',
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
