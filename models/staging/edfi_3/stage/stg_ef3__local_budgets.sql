with base_local_budgets as (
    select *
    from  {{  ref('base_ef3__local_budgets')  }} 
    where not is_deleted
),
keyed as (
    select
         {{  dbt_utils.generate_surrogate_key(
            ['tenant_code',
             'api_year', 
            'lower(as_of_date)',
            'lower(local_account_reference)',
            'lower(amount)',
            ]
        )  }}  as k_local_budget,
        *
        
    from base_local_budgets
),
deduped as (
     {{ 
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_local_budget',
            order_by='pull_timestamp desc'
        )
     }} 
)
select * from deduped