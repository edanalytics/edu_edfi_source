with base_staffs as (
    select * from {{ ref('base_ef3__staffs') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(staff_unique_id)'
            ]
        ) }} as k_staff,
        base_staffs.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staffs
),
-- For x-year resources (those that do not include year in unique key), we need to first dedupe with year using last_modified_timestamp to keep deleted records if last record is deleted..
deduped_within_year as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_staff, api_year',
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
            partition_by='k_staff',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped_across_years
