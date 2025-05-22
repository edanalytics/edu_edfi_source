with base_people as (
    select * from {{ ref('base_ef3__people') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'person_id',
            'lower(source_system)']
        ) }} as k_person,
        base_people.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_people
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_person',
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
