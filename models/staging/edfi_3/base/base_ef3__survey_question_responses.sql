with survey_question_responses as (
    {{ source_edfi3('survey_question_responses') }}
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
        v:surveyQuestionReference:surveyIdentifier::string            as survey_id,
        v:surveyQuestionReference:questionCode::string                as question_code,
        v:surveyResponseReference:surveyResponseIdentifier::string    as survey_response_id,
        v:namespace::string                                           as namespace,
        v:comment::string                                             as comment,
        v:noResponse::bool                                            as no_response,
        --references
        v:surveyReference          as survey_reference,
        v:surveySectionReference   as survey_section_reference,
        -- lists
        v:surveyQuestionMatrixElementResponses  as v_survey_question_matrix_element_responses,
        v:values                                as v_values,
        -- edfi extensions
        v:_ext as v_ext
    from survey_question_responses
)
select * from renamed