with base_staffs as (
    select * from {{ ref('base_ef3__staffs') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(staff_unique_id)'
            ]
        ) }} as k_staff,
        base_staffs.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staffs
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_staff',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
