with base_balance_sheet_dimensions as (
    select * from {{ ref('base_ef3__balance_sheet_dimensions') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(balance_sheet_dimension_code)',
                'balance_sheet_dimension_fiscal_year',
            ]
        ) }} as k_balance_sheet_dimension,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_balance_sheet_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_balance_sheet_dimension',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
