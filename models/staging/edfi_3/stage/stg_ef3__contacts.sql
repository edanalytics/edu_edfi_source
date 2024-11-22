-- parents were renamed to contacts in Data Standard v5.0
with unioned as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('base_ef3__contacts'), 
                ref('int_ef3__parent_contact_bridge')
            ],
            exclude=['parent_unique_id']
        )
    }}
),
drop_deletes as (
    select * 
    from unioned
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(contact_unique_id)'
            ]
        ) }} as k_contact,
        drop_deletes.*
        {{ extract_extension(model_name=[this.name, 'stg_ef3__parents'], flatten=True) }}
    from drop_deletes
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
