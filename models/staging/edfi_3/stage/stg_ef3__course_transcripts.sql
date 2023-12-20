with base_course_transcripts as (
    select * from {{ ref('base_ef3__course_transcripts') }}
    where not is_deleted
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
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped