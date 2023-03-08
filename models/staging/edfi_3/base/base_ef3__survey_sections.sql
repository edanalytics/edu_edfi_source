with survey_sections as (
    {{ source_edfi3('survey_sections') }}
),
renamed as (
    select 
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                                  as record_guid,
        v:surveySectionTitle::string                                  as survey_section_title, 
        v:surveyReference:surveyIdentifier::string                    as survey_id,
        v:surveyReference:namespace::string                           as namespace,
        --references
        v:surveyReference          as survey_reference,
        -- edfi extensions
        v:_ext as v_ext
    from survey_sections
)
select * from renamed