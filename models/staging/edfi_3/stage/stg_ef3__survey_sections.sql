with survey_sections as (
    select * from {{ ref('base_ef3__survey_sections') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(namespace)',
            'lower(survey_id)',
            'lower(survey_section_title)']
        ) }} as k_survey_section,
        {{ gen_skey('k_survey') }},
        survey_sections.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from survey_sections
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_section',
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
