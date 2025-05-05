{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key=['k_student', 'k_school', 'k_session', 'attendance_event_category', 'attendance_event_date'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_student_school_attend as (
    select * from {{ ref('base_ef3__student_school_attendance_events') }}

    {% if is_incremental() %}
    -- Only get newly added or deleted records since the last run
    where last_modified_timestamp > (select max(last_modified_timestamp) from {{ this }})
    {% endif %}
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school') }},
        {{ gen_skey('k_session') }},
        base_student_school_attend.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_school_attend
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_school, k_session, attendance_event_category, attendance_event_date',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{% if not is_incremental() %}
where not is_deleted
{% endif %}

