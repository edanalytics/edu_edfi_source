with applications as (
    select * from {{ ref('base_tpdm__applications') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(applicant_profile_id)',
            'lower(application_id)',
            'ed_org_id']
        ) }} as k_application,
        {{ edorg_ref(annualize=False) }},
        {{ gen_skey('k_applicant_profile')}},
        applications.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from applications
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_application',
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
