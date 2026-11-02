{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key=['k_course', 'k_student_academic_record', 'course_attempt_result'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_course_transcripts as (
    select * from {{ ref('base_ef3__course_transcripts') }}

    {% if is_incremental() %}
    -- Only get newly added or deleted records since the last run
    where last_modified_timestamp > (select max(last_modified_timestamp) from {{ this }})
    {% endif %}
),
keyed as (
    select
        {{ gen_skey('k_course') }},
        {{ gen_skey('k_student_academic_record') }},
        base_course_transcripts.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_course_transcripts
),
first_deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course, k_student_academic_record, course_attempt_result',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}      
),
no_deletes as (
    select * from first_deduped
    {% if not is_incremental() %}
    where not is_deleted
    {% endif %}
),
-- for each student, keep only records from their most recent api_year.
-- academic records are cumulative: the latest ODS is the authoritative view of
-- all past transcripts, so older api_year data is superseded, not supplementary.
latest_api_year_per_student as (
    select
        tenant_code,
        student_unique_id,
        max(api_year) as latest_api_year
    from no_deletes
    group by 1, 2
),

final_deduped as (
    select no_deletes.*
    from no_deletes
    inner join latest_api_year_per_student
        on no_deletes.tenant_code = latest_api_year_per_student.tenant_code
        and no_deletes.student_unique_id = latest_api_year_per_student.student_unique_id
        and no_deletes.api_year = latest_api_year_per_student.latest_api_year
)

select * from final_deduped
