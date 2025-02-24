with programs as (
    {{ source_edfi3('programs') }}
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
        v:id::string                                                  as record_guid,
        ods_version,
        data_model_version,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:programId::string                                           as program_id,
        v:programName::string                                         as program_name,
        -- descriptors
        {{ extract_descriptor('v:programTypeDescriptor::string') }} as program_type,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        -- lists
        v:characteristics    as v_characteristics,
        v:learningObjectives as v_learning_objectives,
        v:learningStandards  as v_learning_standards,
        v:services           as v_services,
        v:sponsors           as v_sponsors,

        -- edfi extensions
        v:_ext as v_ext
    from programs
)
select * from renamed