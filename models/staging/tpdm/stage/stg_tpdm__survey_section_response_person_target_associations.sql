with survey_section_response_person_target_associations as (
    select * from {{ ref('base_tpdm__survey_section_response_person_target_associations') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(namespace)',
            'lower(person_id)',
            'lower(source_system)',
            'lower(survey_id)',
            'lower(survey_response_identifier)',
            'lower(survey_section_title)'] 
        ) }} as k_survey_section_response_person_target_association,
        {{ gen_skey('k_survey_section_response') }},  
        {{ gen_skey('k_person') }},  
        survey_section_response_person_target_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}  
    from survey_section_response_person_target_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_survey_section_response_person_target_association',
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
