with base_fund_dimensions as (
    select * from {{ ref('base_ef3__fund_dimensions') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(fund_dimension_code)',
                'fund_dimension_fiscal_year',
            ]
        ) }} as k_fund_dimension,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_fund_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_fund_dimension',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
