with base_contacts as (
    select * from {{ ref('base_ef3__contacts') }}
    where not is_deleted
),
base_parents as (
    select * rename parent_unique_id as contact_unique_id
    from {{ ref('base_ef3__parents') }}
    where not is_deleted
),
-- parents were renamed to contacts in Data Standard v5.0
unioned as (
    select * from base_contacts
    union 
    select * from base_parents
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(contact_unique_id)'
            ]
        ) }} as k_contact,
        unioned.*
        {{ extract_extension(model_name=[this.name, 'stg_ef3__parents'], flatten=True) }}
    from unioned
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_contact', 
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
