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
-- For x-year resources (those that do not include year in unique key), there's an edge case 
-- where a record we need for historic reporting could have been deleted in a later year. To avoid removing these,
-- we need to first dedupe within year using last_modified_timestamp, then dedupe across years to get to a single record 
deduped_within_year as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_contact, api_year',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
),
-- .. then remove deletes as they shouldn't be used in x-year dedupe
deduped_within_year_no_deletes as (
    select * from deduped_within_year where not is_deleted
),
-- .. and then dedupe across years to enforce the correct grain, keeping latest year that wasn't deleted
deduped_across_years as (
    {{
        dbt_utils.deduplicate(
            relation='deduped_within_year_no_deletes',
            partition_by='k_contact',
            order_by='api_year desc'
        )
    }}
)
select * from deduped_across_years
