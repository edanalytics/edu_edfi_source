{{ config(
    materialized='incremental',
    unique_key=['k_student', 'k_course_section', 'attendance_event_category', 'attendance_event_date'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_student_section_attend as (
    select * from {{ ref('base_ef3__student_section_attendance_events') }}
    
    {% if is_incremental() %}
    -- Only get newly added or deleted records since the last run
    where last_modified_timestamp > (select max(last_modified_timestamp) from {{ this }})
    {% endif %}
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_course_section') }},
        base_student_section_attend.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_section_attend
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_course_section, attendance_event_category, attendance_event_date',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
