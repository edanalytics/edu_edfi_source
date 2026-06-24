with base_program_dimensions as (
    select * from {{ ref('base_ef3__program_dimensions') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(program_dimension_code)',
                'program_dimension_fiscal_year',
            ]
        ) }} as k_program_dimension,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_program_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_program_dimension',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
