with base_local_budgets as (
    select * from {{ ref('base_ef3__local_budgets') }}
),
keyed as (
    select
        {{ gen_skey('k_local_account') }},
        {{ dbt_utils.generate_surrogate_key([
            'k_local_account',
            'as_of_date',
        ]) }} as k_local_budget_snapshot,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_local_budgets
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_local_budget_snapshot',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
