with base_cohorts as (
    select * from {{ ref('base_ef3__cohorts') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(cohort_id)',
            'ed_org_id']
        ) }} as k_cohort, 
        {{ edorg_ref() }},
        api_year as school_year,
        base_cohorts.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_cohorts
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_cohort',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
order by tenant_code, api_year desc, ed_org_id, cohort_id
