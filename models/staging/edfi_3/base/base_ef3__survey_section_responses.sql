with survey_section_responses as (
    {{ source_edfi3('survey_section_responses') }}
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
        v:surveyResponseReference:namespace::string                as namespace,
        v:surveySectionReference:surveyIdentifier::string          as survey_id,
        v:surveyResponseReference:surveyResponseIdentifier::string as survey_response_id,
        v:surveySectionReference:surveySectionTitle::string        as survey_section_title,
        -- non-identity components
        v:sectionRating::float as section_rating,
        -- references
        v:surveyResponseReference as survey_response_reference,
        v:surveySectionReference  as survey_section_reference
    from survey_section_responses
)
select * from renamed
