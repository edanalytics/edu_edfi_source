with base_function_dimensions as (
    select *
    from  {{  ref('base_ef3__function_dimensions')  }} 
    where not is_deleted
),
keyed as (
    select
         {{  dbt_utils.generate_surrogate_key(
            ['tenant_code',
             'api_year', 
            'lower(code)',
            'lower(fiscal_year)',
            ]
        )  }}  as k_function_dimension,
        *
        
    from base_function_dimensions
),
deduped as (
     {{ 
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_function_dimension',
            order_by='pull_timestamp desc'
        )
     }} 
)
select * from deduped