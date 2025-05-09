{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key=['k_student_assessment'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with int_stu_assessments as (
    select * from {{ ref('int_ef3__student_assessments__identify_subject') }}

    {% if is_incremental() %}
    -- Only get new or updated records since the last run
    where last_modified_timestamp > (select max(pull_timestamp) from {{ this }})
    {% endif %}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(academic_subject)',
            'lower(student_assessment_identifier)',
            'lower(assessment_identifier)',
            'lower(namespace)']
        ) }} as k_student_assessment,
        {{ gen_skey('k_assessment', extras = ['academic_subject']) }},
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        int_stu_assessments.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from int_stu_assessments
    -- subsetting here because otherwise we would have referential integrity issues with k_assessment
    where academic_subject is not null
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_assessment',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{% if not is_incremental() %}
where not is_deleted
{% endif %}
