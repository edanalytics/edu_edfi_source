{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key='k_school',
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_schools as (
    select * from {{ ref('base_ef3__schools') }}

    {% if is_incremental() %}
    -- Only get newly added or deleted records since the last run
    where last_modified_timestamp > (select max(last_modified_timestamp) from {{ this }})
    {% endif %}
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
    select * from deduped_within_year 
    {% if not is_incremental() %}
    where not is_deleted
    {% endif %}

)
select * from deduped_within_year_no_deletes