with stu_sch_att as (
    {{ source_edfi3('student_school_attendance_events') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        filename,
        file_row_number,
        is_deleted,
        v:id::string                               as record_guid,
        v:schoolReference:schoolId                 as school_id,
        v:sessionReference:schoolYear              as school_year,
        v:sessionReference:sessionName::string     as session_name,
        v:studentReference:studentUniqueId::string as student_unique_id,
        v:eventDate::date                          as attendance_event_date,
        v:attendanceEventReason::string            as attendance_event_reason,
        v:eventDuration::float                    as event_duration,
        v:schoolAttendanceDuration::float          as school_attendance_duration,
        v:arrivalTime::string                      as arrival_time, --todo: look at format here
        v:departureTime::string                    as departure_time, --todo: look at format here
        -- descriptors
        {{ extract_descriptor('v:attendanceEventCategoryDescriptor::string') }} as attendance_event_category,
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }}  as educational_environment,
        -- references
        v:schoolReference  as school_reference,
        v:sessionReference as session_reference,
        v:studentReference as student_reference,

        -- edfi extensions
        v:_ext as v_ext
    from stu_sch_att
)
select * from renamed
