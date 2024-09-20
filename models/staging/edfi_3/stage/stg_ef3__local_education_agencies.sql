with base_local_education_agencies as (
    select * from {{ ref('base_ef3__local_education_agencies') }}
    where not is_deleted
),
keyed as (
    select
         {{ dbt_utils.generate_surrogate_key(
           ['tenant_code', 
            'lea_id'] 
        ) }} as k_lea,
        {{ gen_skey('k_lea', 'parent_local_education_agency_reference', 'k_lea__parent') }},
        {{ gen_skey('k_sea') }},
        {{ gen_skey('k_esc') }},
        base_local_education_agencies.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_local_education_agencies
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_lea',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped