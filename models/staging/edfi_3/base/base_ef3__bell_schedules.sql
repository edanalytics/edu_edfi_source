with bell_schedules as (
    {{ source_edfi3('bell_schedules') }}
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
        v:id::string                              as record_guid,
        ods_version,
        data_model_version,
        v:bellScheduleName::string                as bell_schedule_name,
        v:schoolReference:schoolId::int           as school_id,
        v:alternateDayName::string                as alternate_day_name,
        v:startTime::string                       as start_time,
        v:endTime::string                         as end_time,
        v:totalInstructionalTime::float           as total_instructional_time,
        -- references
        v:schoolReference                         as school_reference,
        -- unflattened lists
        v:classPeriods                            as v_class_periods,
        v:dates                                   as v_dates,
        v:gradeLevels                             as v_grade_levels,
        -- edfi extensions
        v:_ext as v_ext
    from bell_schedules
)
select * from renamed
