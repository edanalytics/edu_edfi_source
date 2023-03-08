with survey_question as (
    {{ source_edfi3('survey_question') }}
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
        v:surveyReference:surveyIdentifier::string                    as survey_id,
        v:surveyReference:namespace::string                           as namespace,
        v:surveySectionReference:surveySectionTitle::string           as survey_section_title,
        v:questionCode::string                                        as question_code,
        v:questionText::string                                        as question_text,
        -- descriptors
        {{ extract_descriptor('v:questionFormDescriptor::string') }} as question_form,
        --references
        v:surveyReference          as survey_reference,
        v:surveySectionReference   as survey_section_reference,
        -- lists
        v:matrices                     as v_matrices,
        v:responseChoices              as v_response_choices,
        -- edfi extensions
        v:_ext as v_ext
    from survey_question
)
select * from renamed