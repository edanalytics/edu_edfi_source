with base_rubric_dimensions as (
    select * from {{ ref('base_tpdm__rubric_dimensions') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year'
            ]
        ) }} as k_rubric_dimension,
        base_rubric_dimensions.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_rubric_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_rubric_dimension',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
