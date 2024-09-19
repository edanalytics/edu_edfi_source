with student_parent_associations as (
    {{ source_edfi3('student_parent_associations') }}
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
        ods_version,
        data_model_version,
        v:contactPriority::int                                   as contact_priority,
        v:contactRestrictions::string                            as contact_restrictions,
        v:emergencyContactStatus::boolean                        as is_emergency_contact,
        v:livesWith::boolean                                     as is_living_with,
        v:primaryContactStatus::boolean                          as is_primary_contact,
        v:legalGuardian::boolean                                 as is_legal_guardian,
        {{ extract_descriptor('v:relationDescriptor::string') }} as relation_type,
        -- references
        v:parentReference                                        as parent_reference,
        v:studentReference                                       as student_reference,

        -- edfi extensions
        v:_ext as v_ext
    from student_parent_associations
)
select * from renamed