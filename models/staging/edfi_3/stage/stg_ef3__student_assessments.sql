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
            'lower(assessment_identifier)',
            'lower(namespace)',
            'lower(student_assessment_identifier)',
            'lower(student_unique_id)']
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
-- school_year is technically optional in EdFi; infer from administration_date when missing,
-- and apply the assessment dates xwalk when configured
with_derived_school_year as (
    select
        {{ edu_edfi_source.star('keyed', except=['school_year']) }},
        {% if var('edu:school_year:assessment_dates_xwalk_enabled', False) %}
        iff(dates_xwalk.override_existing,
            coalesce(dates_xwalk.school_year, keyed.school_year, {{ edu_edfi_source.derive_school_year('keyed.administration_date') }}),
            coalesce(keyed.school_year, {{ edu_edfi_source.derive_school_year('keyed.administration_date') }}, dates_xwalk.school_year))
        as school_year
        {% else %}
        coalesce(keyed.school_year, {{ edu_edfi_source.derive_school_year('keyed.administration_date') }}) as school_year
        {% endif %}
    from keyed
    {% if var('edu:school_year:assessment_dates_xwalk_enabled', False) %}
    left join {{ ref('xwalk_assessment_school_year_dates') }} dates_xwalk
        -- note: between means A >= X AND A <= Y, so date upper/lower bounds should not overlap across years
        on keyed.administration_date between dates_xwalk.start_date::date and dates_xwalk.end_date::date
        -- allow school year cutoffs to differ by assessment, but also allow those fields to be null
        and ifnull(dates_xwalk.assessment_identifier, '1') = iff(dates_xwalk.assessment_identifier is null, '1', keyed.assessment_identifier)
        and ifnull(dates_xwalk.namespace, '1') = iff(dates_xwalk.namespace is null, '1', keyed.namespace)
    {% endif %}
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='with_derived_school_year',
            partition_by='k_student_assessment',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{% if not is_incremental() %}
where not is_deleted
{% endif %}
