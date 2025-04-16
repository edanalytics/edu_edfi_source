with restraint_events as (
    {{ source_edfi3('restraint_events') }}
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
        v:id::string                                         as record_guid,
        ods_version,
        data_model_version,
        v:schoolReference:schoolId::integer                  as school_id,
        v:studentReference:studentUniqueId::string           as student_unique_id,
        v:restraintEventIdentifier::string                   as restraint_event_identifier,
        v:eventDate::date                                    as event_date,
        -- descriptors
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }}   as educational_environment,
        -- references
        v:schoolReference  as school_reference,
        v:studentReference as student_reference,
        -- lists
        v:programs::array  as v_programs,
        v:reasons::array   as v_reasons,

        -- edfi extensions
        v:_ext as v_ext
    from restraint_events
)
select * from renamed