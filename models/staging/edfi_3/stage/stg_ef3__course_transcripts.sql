{{ config(
    materialized='incremental',
    unique_key=['k_course', 'k_student_academic_record', 'course_attempt_result'],
    pre_hook=[
        """
        {% if is_incremental() %}
        DELETE 
        FROM {{ this }} as stg_data
        USING {{ source('raw_edfi_3', '_deletes') }} deletes_data
        WHERE deletes_data.name = '{{ this.table | replace(\"stg_ef3__\", \"\") }}'
            and stg_data.tenant_code = deletes_data.tenant_code
            and stg_data.api_year = deletes_data.api_year
            and stg_data.record_guid = replace(get_ignore_case(deletes_data.v, 'id')::string, '-')
            and deletes_data.pull_timestamp > (select max(pull_timestamp) from {{ this }})
        ;
        {% endif %}
        """
    ]
) }}
with base_course_transcripts as (
    select * from {{ ref('base_ef3__course_transcripts') }}

    {% if is_incremental() %}
    -- Only get new or updated records since the last run
    where pull_timestamp > (select max(pull_timestamp) from {{ this }})
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
            order_by='api_year desc, last_modified_timestamp desc'
        )
    }}
)
select * from deduped
{# for incremental, deletes were already filtered up top #}
{# Q before applying elsewhere: should we remove this conditional, since it's harmless to filter an already-filtered? or will that be slow? #}
{% if not is_incremental() %}
where not is_deleted
{% endif %}