with base_tpdm__survey_section_responses as (
    {{ source_edfi3('base_tpdm__survey_section_responses') }}
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

        v:id::string                                               as record_guid,
        v:surveyResponseReference:surveyResponseIdentifier::string as survey_response_id,
        v:surveySectionReference:surveyIdentifier::string          as survey_id,
        v:sectionRating:int                                        as section_rating,
        -- references
        v:surveyResponseReference as survey_response_reference,
        v:surveySectionReference  as survey_section_reference
    from base_tpdm__survey_section_responses
)

select * from renamed
