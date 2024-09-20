with base_parents as (
    select * from {{ ref('base_ef3__parents') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(parent_unique_id)'
            ]
        ) }} as k_parent,
        base_parents.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_parents
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_parent', 
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped