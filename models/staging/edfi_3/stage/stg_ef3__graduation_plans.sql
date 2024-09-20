with base_graduation_plans as (
    select * from {{ ref('base_ef3__graduation_plans') }}
    where not is_deleted
),
keyed as (
    select
         {{ dbt_utils.generate_surrogate_key(
           ['tenant_code', 
           'api_year', 
           'ed_org_id',
           'lower(graduation_plan_type)',
           'graduation_school_year']
        ) }} as k_graduation_plan,
        {{ edorg_ref() }},
        api_year as school_year,
        base_graduation_plans.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_graduation_plans
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_graduation_plan',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped