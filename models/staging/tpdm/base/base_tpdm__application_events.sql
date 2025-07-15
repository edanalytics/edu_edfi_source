with app_events as (
    {{ source_edfi3('application_events') }}
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
        v:sequenceNumber::int                                                as sequence_number,
        v:eventDate::date                                                    as event_date,  -- this could also be event_start_date
        {{ extract_descriptor('v:applicationEventTypeDescriptor::string') }} as application_event_type,
        v:applicationReference:applicationIdentifier::string                 as application_identifier,
        v:applicationReference:applicantProfileIdentifier::string            as applicant_profile_identifier,
        v:applicationReference:educationOrganizationId::int                  as education_organization_id,
        -- non-identity components
        v:eventEndDate::date                     as event_end_date
        v:applicationEvaluationScore::float      as application_evaluation_score
        v:schoolYearTypeReference:schoolYear:int as school_year
        -- references
        v:applicationReference as application_reference
        -- descriptors
        {{ extract_descriptor('v:applicationEventResultDescriptor::string') }} as application_event_result_descriptor
        {{ extract_descriptor('v:termDescriptor::string') }}                   as term
        -- edfi extensions
        v:_ext as v_ext

    from app_events
)
select * from renamed
