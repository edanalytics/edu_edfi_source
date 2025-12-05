with student_iep_disability_collections as (
    {{ source_edfi3('student_iep_disability_collections') }}
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
        ods_version,
        data_model_version,
        -- key columns
        v:studentIEPReference:studentIEPAssociationID::string                             as student_iep_association_id,
        v:studentReference:studentUniqueId::string                                        as student_unique_id,
        v:studentIEPReference:educationOrganizationReference:educationOrganizationId::int as iep_servicing_ed_org_id,
        -- references
        v:studentReference    as student_reference,
        v:studentIEPReference as student_iep_reference,
        -- lists
        v:disabilities as v_disabilities,

        -- edfi extensions
        v:_ext as v_ext

    from student_iep_disability_collections
)

select * from renamed
