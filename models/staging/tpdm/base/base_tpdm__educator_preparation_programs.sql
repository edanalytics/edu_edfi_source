with educator_prep_programs as (
    {{ source_edfi3('educator_preparation_programs') }}
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

        v:id::string          as record_guid,
        v:programId::string   as program_id,
        v:programName::string as program_name,
        v:educationOrganizationReference:educationOrganizationId::integer   as ed_org_id,
        v:educationOrganizationReference:link:rel::string                   as ed_org_type,
        -- descriptors
        {{ extract_descriptor('v:programTypeDescriptor::string') }}         as program_type,
        {{ extract_descriptor('v:accreditationStatusDescriptor::string') }} as accreditation_status,
        -- unflattened lists
        v:gradeLevels                    as v_grade_levels,
        -- references
        v:educationOrganizationReference as ed_org_reference,
        -- edfi extensions
        v:_ext                           as v_ext
    from educator_prep_programs
)
select * from renamed
