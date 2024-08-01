with school_year_types as (
    {{ source_edfi3('school_year_types') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        filename,
        file_row_number,
        is_deleted,

        v:id::string as record_guid,
        -- identity components
        v:schoolYear::string as school_year,
        -- non-identity components
        v:currentSchoolYear::string     as current_school_year,
        v:schoolYearDescription::string as school_year_description
    from school_year_types
)
select * from renamed
