with base_grading_periods as (
    select * from {{ ref('base_ef3__grading_periods') }}
    where not is_deleted
),
keyed as (
    select
         {{ dbt_utils.generate_surrogate_key(
           ['tenant_code', 
            'lower(grading_period)',
            'period_sequence',
            'school_id', 
            'school_year'] 
        ) }} as k_grading_period,
        {{ gen_skey('k_school') }}, 
        base_grading_periods.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_grading_periods
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_grading_period',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
order by tenant_code, school_year desc, period_sequence