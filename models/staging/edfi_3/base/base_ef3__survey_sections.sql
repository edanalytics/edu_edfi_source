with survey_sections as (
    {{ source_edfi3('survey_sections') }}
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

        v:id::string                                as record_guid,
        v:surveyReference::surveyIdentifier::string as survey_id,
        v:surveySectionTitle::string                as survey_section_title,
        -- references
        v:surveyReference as survey_reference
    from survey_sections
)
select * from renamed
