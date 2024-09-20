with base_networks as (
    select * from {{ ref('base_ef3__education_organization_networks') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'network_id'
            ]
        )}} as k_network,
        base_networks.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_networks
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_network',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped