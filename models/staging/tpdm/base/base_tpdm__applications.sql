with applications as (
    {{ source_edfi3('applications') }}
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
        v:applicantProfileReference:applicantProfileIdentifier::string as applicant_profile_id,
        v:applicationIdentifier::string                                as application_id,
        v:educationOrganizationReference:educationOrganizationId::int  as ed_org_id,
        -- non-identity components
        v:educationOrganizationReference:link:rel::string              as ed_org_type,
        v:acceptedDate::date                                           as accepted_date,
        v:applicationDate::date                                        as application_date,
        -- descriptors
        {{ extract_descriptor('v:applicationStatusDescriptor::string') }} as application_status,
        -- references
        v:applicantProfileReference      as applicant_profile_reference,
        v:educationOrganizationReference as education_organization_reference,
        -- unnested lists
        v:recruitmentEventAttendances as v_recruitment_event_attendances,
        v:scoreResults                as v_score_results,
        v:terms                       as v_terms,
        -- edfi extensions
        v:_ext as v_ext
    from applications
)
select * from renamed
