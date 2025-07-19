with student_paths as (
    {{ source_edfi3('student_paths') }}
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

        v:id::string as record_guid,
        -- identity components
        v:pathReference:pathName::string             as path_name,
        v:studentReference:studentUniqueId::string   as student_unique_id,
        v:pathReference:educationOrganizationId::int as ed_org_id,
        -- unflattened lists
        v:periods as v_periods,
        -- references
        v:pathReference    as path_reference,
        v:studentReference as student_reference,
        -- edfi extensions
        v:_ext as v_ext

    from student_paths
)
select * from renamed