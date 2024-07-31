with survey_response_person_target_associations as (
    {{ source_edfi3('survey_response_person_target_associations') }}
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
        v:surveyResponseReference:namespace::string                                 as namespace,
        v:personReference:personId::string                                          as person_id,
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string')}} as source_system,
        v:surveyResponseReference:surveyIdentifier::string                          as survey_id,
        v:surveyResponseReference:surveyResponseIdentifier::string                  as survey_response_id,
        -- references
        v:surveyResponseReference as survey_response_reference,
        v:personReference         as person_reference,
        -- edfi extensions
        v:_ext as v_ext
    from survey_response_person_target_associations
)
select * from renamed
