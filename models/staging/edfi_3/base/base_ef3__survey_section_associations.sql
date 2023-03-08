with survey_sections_associations as (
    {{ source_edfi3('survey_section_associations') }}
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
        v:sectionReference:localCourseCode::string                    as local_course_code,
        v:sectionReference:schoolId::string                           as school_id,
        v:sectionReference:schoolYear::string                         as school_year,
        v:sectionReference:sectionIdentifier::string                  as section_id,
        v:sectionReference:sessionName::string                        as session_name,
        v:surveyReference:surveyIdentifier::string                    as survey_id,
        v:surveyReference:namespace::string                           as namespace,
        --references
        v:sectionReference as section_reference,
        v:surveyReference  as survey_reference,
        -- edfi extensions
        v:_ext as v_ext
    from survey_sections_associations
)
select * from renamed