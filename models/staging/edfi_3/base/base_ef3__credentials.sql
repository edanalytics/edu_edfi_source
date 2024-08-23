with credentials as (
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

        v:id::string as record_guid,
        -- identity components
        v:credentialIdentifier::string                                                as credential_id,
	v:_ext:tpdm:personReference:personId::string                                  as person_id,
        {{ extract_descriptor('v:stateOfIssueStateAbbreviationDescriptor::string') }} as state_of_issue_state_abbreviation,
        -- non-identity components
        v:effectiveDate::date  as effective_date,
        v:expirationDate::date as expiration_date,
        v:issuanceDate::date   as issuance_date,
        v:namespace::string    as namespace,
        -- descriptors
        {{ extract_descriptor('v:credentialFieldDescriptor::string') }}               as credential_field,
        {{ extract_descriptor('v:credentialTypeDescriptor::string') }}                as credential_type,
        {{ extract_descriptor('v:teachingCredentialDescriptor::string') }}            as teaching_credential,
        {{ extract_descriptor('v:teachingCredentialBasisDescriptor::string') }}       as teaching_credential_basis,
        -- unnested lists
        v:endorsements     as v_endorsements,
        v:academicSubjects as v_academic_subjects,
        v:gradeLevels      as v_grade_levels,
        -- references
	v:personReference as person_reference,
        v:studentAcademicRecords as student_academic_records,
        -- edfi extensions
        v:_ext as v_ext 
    from credentials
)
select * from renamed
