with base_local_actuals as (
    select *
    from  {{  ref('base_ef3__local_actuals')  }} 
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
        )  }}  as k_local_actual,
        *
        
    from base_local_actuals
),
deduped as (
     {{ 
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_local_actual',
            order_by='pull_timestamp desc'
        )
     }} 
)
select * from deduped