with base_operational_unit_dimensions as (
    select * from {{ ref('base_ef3__operational_unit_dimensions') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(operational_unit_dimension_code)',
                'operational_unit_dimension_fiscal_year',
            ]
        ) }} as k_operational_unit_dimension,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_operational_unit_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_operational_unit_dimension',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
