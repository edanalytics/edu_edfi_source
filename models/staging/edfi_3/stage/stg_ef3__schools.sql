with base_schools as (
    select * from {{ ref('base_ef3__schools') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'school_id'
            ]
        )}} as k_school,
        {{ gen_skey('k_lea') }},
        base_schools.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_schools
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_school',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped