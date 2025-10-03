with base_chart_of_accounts as (
    select *
    from  {{  ref('base_ef3__chart_of_accounts')  }} 
    where not is_deleted
),
keyed as (
    select
         {{  dbt_utils.generate_surrogate_key(
            ['tenant_code',
             'api_year', 
            'lower(account_identifier)',
            'lower(fiscal_year)',
            'lower(education_organization_reference)',
            'lower(account_type)',
            ]
        )  }}  as k_chart_of_account,
        *
        
    from base_chart_of_accounts
),
deduped as (
     {{ 
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_chart_of_account',
            order_by='pull_timestamp desc'
        )
     }} 
)
select * from deduped