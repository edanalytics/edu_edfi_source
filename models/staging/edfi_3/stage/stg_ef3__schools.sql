with base_schools as (
    select * from {{ ref('base_ef3__schools') }}
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
-- For x-year resources (those that do not include year in unique key), there's an edge case 
-- where a record we need for historic reporting could have been deleted in a later year. To avoid removing these,
-- we need to first dedupe within year using last_modified_timestamp, then dedupe across years to get to a single record 
deduped_within_year as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_school, api_year',
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
            partition_by='k_school',
            order_by='api_year desc'
        )
    }}
)
select * from deduped_across_years
