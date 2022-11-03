with stu_section_att as (
    {{ source_edfi3('student_section_attendance_events') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        filename,
        file_row_number,
        is_deleted,
        v:id::string                                 as record_guid,
        v:studentReference:studentUniqueId::string   as student_unique_id,
        v:sectionReference:localCourseCode::string   as local_course_code,
        v:sectionReference:schoolId::int             as school_id,
        v:sectionReference:schoolYear::int           as school_year,
        v:sectionReference:sectionIdentifier::string as section_id,
        v:sectionReference:sessionName::string       as session_name,
        v:eventDate::date                            as attendance_event_date,
        v:attendanceEventReason::string              as attendance_event_reason,
        v:event_duration::float                      as event_duration,
        v:sectionAttendanceDuration::float           as section_attendance_duration,
        v:arrivalTime::string                        as arrival_time, --todo: look at format here
        v:departureTime::string                      as departure_time, --todo: look at format here
        -- descriptors
        {{ extract_descriptor('v:attendanceEventCategoryDescriptor::string') }} as attendance_event_category,
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }}  as educational_environment,
        -- references
        v:sectionReference as section_reference,
        v:studentReference as student_reference,

        -- edfi extensions
        v:_ext as v_ext

    from stu_section_att
)
select * from renamed