with class_periods as (
    {{ source_edfi3('class_periods') }}
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
        v:id::string                        as record_guid,
        ods_version,
        data_model_version,
        v:schoolReference:schoolId::integer as school_id,
        v:classPeriodName::string           as class_period_name,
        v:officialAttendancePeriod::boolean as is_official_attendance_period,
        v:schoolReference                   as school_reference,
        v:meetingTimes                      as v_meeting_times,

        -- edfi extensions
        v:_ext as v_ext
    from class_periods
)
select * from renamed