with base_function_dimensions as (
    select * from {{ ref('base_ef3__function_dimensions') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(function_dimension_code)',
                'function_dimension_fiscal_year',
            ]
        ) }} as k_function_dimension,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_function_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_function_dimension',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
