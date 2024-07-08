with candidates as (
    {{ source_edfi3('credentials') }}
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

        v:id::string                                             as record_guid,
        v:credentialIdentifier::string                           as credential_id,
        v:endorsements::string                                   as endorsements,
        v:effectiveDate::date                                    as effective_date,
        v:expirationDate::date                                   as expiration_date,
        v:issuanceDate::date                                     as issuance_date,
        v:credentialStatusDate::date                             as credential_status_date,
        v:boardCertificationIndicator::boolean                   as board_certification_indicator,
        v:namespace::string                                      as namespace,
        v:certificationTitle::string                             as certification_title,
        v:personReference:personId::string                       as person_id,
        v:certificationReference:certificationIdentifier::string as certification_id,
        -- descriptors
        {{ extract_descriptor('v:credentialFieldDescriptor::string') }}               as credential_field,
        {{ extract_descriptor('v:credentialTypeDescriptor::string') }}                as credential_type,
        {{ extract_descriptor('v:gradeLevels::string') }}                             as grade_levels,
        {{ extract_descriptor('v:stateOfIssueStateAbbreviationDescriptor::string') }} as state_of_issue_state_abbreviation,
        {{ extract_descriptor('v:teachingCredentialDescriptor::string') }}            as teaching_credential,
        {{ extract_descriptor('v:teachingCredentialBasisDescriptor::string') }}       as teaching_credential_basis,
        {{ extract_descriptor('v:academicSubjects::string') }}                        as academic_subjects,
        {{ extract_descriptor('v:certificationRouteDescriptor::string') }}            as certification_route,
        {{ extract_descriptor('v:credentialStatusDescriptor::string') }}              as credential_status,
        {{ extract_descriptor('v:educatorRoleDescriptor::string') }}                  as educator_role,
        -- references
        v:studentAcademicRecords as student_academic_records,,

    from candidates
)
select * from renamed
