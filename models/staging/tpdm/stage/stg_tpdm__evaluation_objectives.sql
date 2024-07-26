with base_evaluation_objectives as (
    select * from {{ ref('base_tpdm__evaluation_objectives') }}
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
                'school_year',
                'evaluation_objective_title'
            ]
        ) }} as k_evaluation_objectives,
        base_evaluation_objectives.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_evaluation_objectives
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation_objectives',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
