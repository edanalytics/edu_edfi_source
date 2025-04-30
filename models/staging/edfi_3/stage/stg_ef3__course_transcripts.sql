{{ config(
    materialized='incremental',
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
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course, k_student_academic_record, course_attempt_result',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{% if not is_incremental() %}
where not is_deleted
{% endif %}

