with base_service_centers as (
    select * from {{ ref('base_ef3__education_service_centers') }}
    where not is_deleted
),
keyed as(
    select 
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'service_center_id'
            ]
        )}} as k_education_service_center,
        base_service_centers.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_service_centers
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_education_service_center',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped