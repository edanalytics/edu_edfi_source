{{ config(
    materialized='incremental',
    unique_key=['k_student', 'k_course_section', 'begin_date']
) }}
with base_student_section as (
    select * from {{ ref('base_ef3__student_section_associations') }}
    where not is_deleted

    {% if is_incremental() %}
    -- Only get new or updated records since the last run
    and last_modified_timestamp > (select max(pull_timestamp) from {{ this }})
    {% endif %}
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_course_section') }},
        base_student_section.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_section
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_course_section, begin_date',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped