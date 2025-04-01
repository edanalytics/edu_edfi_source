with base_locations as (
    select * from {{ ref('base_ef3__locations') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(classroom_id_code)',
            'school_id']
        ) }} as k_location, 
        {{ gen_skey('k_school') }},
        api_year as school_year,
        base_locations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_locations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_location',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
