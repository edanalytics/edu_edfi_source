with base_tpdm__candidate_relationship_to_staff_associations as (
    {{ source_edfi3('base_tpdm__candidate_relationship_to_staff_associations') }}
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

        v:id::string                                     as record_guid,
        v:candidateReference:candidateIdentifier::string as candidate_id,
        v:staffReference:staffUniqueId::string           as staff_id,
        v:beginDate::date                                as begin_date,
        -- descriptors
        {{ extract_descriptor('v:staffToCandidateRelationshipDescriptor::string') }} as staff_to_candidate_relationship,
        -- references
        v:candidateReference as candidate_reference,
        v:staffReference     as staff_reference
    from base_tpdm__candidate_relationship_to_staff_associations
)
select * from renamed
