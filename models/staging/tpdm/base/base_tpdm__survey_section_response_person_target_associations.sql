with survey_section_response_person_target_associations as (
    {{ source_edfi3('survey_section_response_person_target_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string as record_guid,
        -- identity components
        v:surveySectionResponseReference:surveyIdentifier::string                    as survey_id,
        v:surveySectionResponseReference:namespace::string                           as namespace,
        v:surveySectionResponseReference:surveySectionTitle::string                  as survey_section_title,
        v:surveySectionResponseReference:surveyResponseIdentifier::string            as survey_response_identifier,
        v:personReference:personId::string                                           as person_id,
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }} as source_system,
        -- references
        v:surveySectionResponseReference as survey_section_response_reference,
        v:personReference                as person_reference,
        -- edfi extensions
        v:_ext as v_ext 
    from survey_section_response_person_target_associations
)
select * from renamed
