with calendar_dates as (
    {{ source_edfi3('calendar_dates') }}
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
        v:id::string                             as record_guid,
        ods_version,
        data_model_version,
        v:date::date                             as calendar_date,
        v:calendarReference:calendarCode::string as calendar_code,
        v:calendarReference:schoolId::integer    as school_id,
        v:calendarReference:schoolYear::integer  as school_year,
        v:calendarReference                      as calendar_reference,
        v:calendarEvents                         as v_calendar_events,
        array_size(v:calendarEvents)             as n_calendar_events,

        -- edfi extensions
        v:_ext as v_ext
    from calendar_dates
)
select * from renamed