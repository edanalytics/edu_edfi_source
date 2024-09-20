with base_service_centers as (
    select * from {{ ref('base_ef3__education_service_centers') }}
    where not is_deleted
),
keyed as(
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'esc_id'
            ]
        )}} as k_esc,
        base_service_centers.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_service_centers
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_esc',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped