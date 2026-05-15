with disabilities as (
    {{ source_edfi3('student_iep_disability_collections')}}
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
        v:studentIEPReference:educationOrganizationId::int as ed_org_id,
        v:studentIEPReference:iepFinalizedDate::date as iep_finalized_date,
        v:studentIEPReference:studentIEPAssociationID::string as student_iep_association_id,
        v:studentIEPReference:studentUniqueId::string as student_unique_id,
        -- references
        v:studentIEPReference as student_iep_reference,
        -- lists
        v:disabilities as v_disabilities,

        -- edfi extensions
        v:_ext as v_ext
    from disabilities
)
select * from renamed