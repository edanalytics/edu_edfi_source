with base_class_periods as (
    select * from {{ ref('base_ef3__class_periods') }}
    where not is_deleted
),
keyed as (
    select
         {{ dbt_utils.generate_surrogate_key(
           ['tenant_code', 
           'api_year', 
           'lower(class_period_name)',
           'school_id']
        ) }} as k_class_period,
        {{ gen_skey('k_school') }}, 
        api_year as school_year,
        base_class_periods.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_class_periods
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_class_period',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
order by tenant_code, api_year desc, school_id, class_period_name