with survey_responses as (
    {{ source_edfi3('survey_responses') }}
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

        v:id::string                                                  as record_guid,
        v:surveyReference:surveyIdentifier::string                    as survey_id,
        v:surveyReference:namespace::string                           as namespace,
        v:surveyResponseIdentifier::string                            as survey_response_id,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        v:electronicMailAddress::string                               as electronic_mail_address,
        v:fullName::string                                            as full_name, 
        v:location::string                                            as location, 
        v:responseDate::date                                          as response_date, 
        v:responseTime::int                                           as completion_time_seconds, 
        --references
        v:surveyReference    as survey_reference,
        v:studentReference   as student_reference,
        v:staffReference     as staff_reference,
        v:parentReference    as parent_reference,
        -- lists
        v:surveyLevels  as v_survey_levels,    
        -- edfi extensions
        v:_ext as v_ext
    from survey_responses
)
select * from renamed
