with base_network_associations as (
    select * from {{ ref('base_ef3__education_organization_network_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_network') }},
        {{ edorg_ref(annualize=False) }},
        base_network_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_network_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_network, k_school, k_lea',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
