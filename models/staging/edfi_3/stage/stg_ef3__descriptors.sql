with base_descriptors as (
    select * from {{ ref('base_ef3__descriptors') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(code_value)',
            'lower(namespace)']
        ) }} as k_descriptor, 
        api_year as school_year,
        base_descriptors.*
    from base_descriptors
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_descriptor',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
order by tenant_code, api_year desc, descriptor_name, code_value

