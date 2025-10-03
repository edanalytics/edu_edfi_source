with base_local_accounts as (
    select *
    from  {{  ref('base_ef3__local_accounts')  }} 
    where not is_deleted
),
keyed as (
    select
         {{  dbt_utils.generate_surrogate_key(
            ['tenant_code',
             'api_year', 
            'lower(account_identifier)',
            'lower(fiscal_year)',
            'lower(chart_of_account_reference)',
            'lower(education_organization_reference)',
            ]
        )  }}  as k_local_account,
        *
        
    from base_local_accounts
),
deduped as (
     {{ 
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_local_account',
            order_by='pull_timestamp desc'
        )
     }} 
)
select * from deduped