with locations as (
    {{ source_edfi3('locations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                          as record_guid,
        v:classroomIdentificationCode::string as classroom_id_code,
        v:schoolReference:schoolId::int       as school_id,
        v:maximumNumberOfSeats::int           as maximum_seating,
        v:optimumNumberOfSeats::int           as optimum_seating,
        v:schoolReference                     as school_reference,

        -- edfi extensions
        v:_ext as v_ext
    from locations
)
select * from renamed