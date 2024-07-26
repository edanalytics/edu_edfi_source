with base_evaluation_elements as (
    select * from {{ ref('base_tpdm__evaluation_elements') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'evaluation_title',
                'evaluation_element_title',
                'school_year'
            ]
        ) }} as k_evaluation_elements,
        base_evaluation_elements.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_evaluation_elements
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation_elements',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
