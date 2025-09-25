with student_path_phase_statuses as (
    {{ source_edfi3('student_path_phase_statuses') }}
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
        v:pathPhaseReference:pathName::string               as path_phase_path_name,
        v:pathPhaseReference:pathPhaseName::string          as path_phase_name,
        v:pathPhaseReference:educationOrganizationId::int   as path_phase_ed_org_id,
        v:studentPathReference:pathName::string             as student_path_path_name,
        v:studentPathReference:studentUniqueId::string      as student+unique_id,
        v:studentPathReference:educationOrganizationId::int as student_path_ed_org_id,
        -- non-identity components
        v:completionIndicator::boolean as has_completed,
        -- unflattened lists
        v:events  as v_events,
        v:periods as v_periods,
        -- references
        v:pathPhaseReference   as path_phase_reference,
        v:studentPathReference as student_path_reference,
        -- edfi extensions
        v:_ext as v_ext

    from student_path_phase_statuses
)
select * from renamed