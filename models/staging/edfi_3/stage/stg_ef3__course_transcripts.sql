-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key=['k_course', 'k_student_academic_record', 'course_attempt_result', 'api_year'],
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
-- A student's 2024 courses may appear in both the 2024 and 2025 ods.
-- the 2025 ods might include algebra but leave out biology, even though both are present in the 2024 ods.
-- Here, we keep the courses from each ods so biology isn't lost just because it wasn't sent again in 2025.
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course, k_student_academic_record, course_attempt_result, api_year',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{% if not is_incremental() %}
where not is_deleted
{% endif %}
