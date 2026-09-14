{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key=['k_student_academic_record'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_academic_records as (
    select * from {{ ref('base_ef3__student_academic_records') }}

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
                'ed_org_id',
                'school_year',
                'lower(student_unique_id)',
                'lower(academic_term)'
            ]
        ) }} as k_student_academic_record,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ edorg_ref(annualize=False) }},
        base_academic_records.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_academic_records
),
-- For x-year resources (those that do not include year in unique key), there's an edge case
-- where a record we need for historic reporting could have been deleted in a later year. To avoid removing these,
-- we need to first dedupe within year using last_modified_timestamp, then dedupe across years to get to a single record
deduped_within_year as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_academic_record, api_year',
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
),
-- .. and then dedupe across years to enforce the correct grain, keeping latest year that wasn't deleted
deduped_across_years as (
    {{
        dbt_utils.deduplicate(
            relation='deduped_within_year_no_deletes',
            partition_by='k_student_academic_record',
            order_by='api_year desc'
        )
    }}
)
select * from deduped_across_years
