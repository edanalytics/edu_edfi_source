with base_state_education_agencies as (
    select * from {{ ref('base_ef3__state_education_agencies') }}
),
keyed as (
    select
         {{ dbt_utils.generate_surrogate_key(
           ['tenant_code', 
            'sea_id'] 
        ) }} as k_sea,
        base_state_education_agencies.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_state_education_agencies
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_sea',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
