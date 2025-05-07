with base_local_education_agencies as (
    select * from {{ ref('base_ef3__local_education_agencies') }}
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
-- For x-year resources (those that do not include year in unique key), there's an edge case 
-- where a record we need for historic reporting could have been deleted in a later year. To avoid removing these,
-- we need to first dedupe within year using last_modified_timestamp, then dedupe across years to get to a single record 
deduped_within_year as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_lea, api_year',
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
            partition_by='k_lea',
            order_by='api_year desc'
        )
    }}
)
select * from deduped_across_years
