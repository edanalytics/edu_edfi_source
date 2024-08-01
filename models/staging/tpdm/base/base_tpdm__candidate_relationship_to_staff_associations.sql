with candidate_relationship_to_staff_associations as (
    {{ source_edfi3('candidate_relationship_to_staff_associations') }}
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
        v:candidateReference:candidateIdentifier::string as candidate_id,
        v:staffReference:staffUniqueId::string           as staff_unique_id,
        -- non-identity components
        v:beginDate::date as begin_date,
        -- descriptors
        {{ extract_descriptor('v:staffToCandidateRelationshipDescriptor::string') }} as staff_to_candidate_relationship,
        -- references
        v:candidateReference as candidate_reference,
        v:staffReference     as staff_reference,
        -- edfi extensions
        v:_ext as v_ext
    from candidate_relationship_to_staff_associations
)
select * from renamed
