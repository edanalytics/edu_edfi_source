with base_source_dimensions as (
    select * from {{ ref('base_ef3__source_dimensions') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(source_dimension_code)',
                'source_dimension_fiscal_year',
            ]
        ) }} as k_source_dimension,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_source_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_source_dimension',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
