with student_section_associations as (
    {{ source_edfi3('student_section_associations') }}
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
        v:fieldworkExperienceReference:beginDate::date             as begin_date,
        v:fieldworkExperienceReference:fieldworkIdentifier::string as fieldwork_identifier,
        v:fieldworkExperienceReference:studentUniqueId::string     as student_unique_id,
        v:sectionReference:sectionIdentifier::string               as section_identifier,
        v:sectionReference:localCourseCode::string                 as local_course_code,
        v:sectionReference:sessionName::string                     as session_name,
        v:sectionReference:schoolId::int                           as school_id,
        v:sectionReference:schoolYear::int                         as school_year,
        -- references
        v:fieldworkExperienceReference as fieldwork_experience_reference,
        v:sectionReference             as section_reference,
        -- edfi extensions
        v:_ext as v_ext
    from student_section_associations
)
select * from renamed