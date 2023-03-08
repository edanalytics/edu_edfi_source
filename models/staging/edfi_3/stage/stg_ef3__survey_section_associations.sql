with base_survey_sections_associations as (
    select * from {{ ref('base_ef3__survey_section_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(survey_id)',
                'lower(namespace)'
            ]
        ) }} as k_survey,
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'lower(local_course_code)',
                'school_id',
                'school_year',
                'lower(section_id)',
                'lower(session_name)'
            ]
        ) }} as k_course_section,
        base_survey_sections_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_surveys
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey','k_course_section' 
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped