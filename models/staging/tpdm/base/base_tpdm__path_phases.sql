with path_phases as (
    {{ source_edfi3('path_phases') }}
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
        v:pathPhaseName::string as path_phase_name,
        v:pathReference:educationOrganizationId::int as ed_org_id,
        v:pathReference:pathName::string as path_name,
        -- non-identity components
        v:pathPhaseSequence::int as path_phase_sequence,
        v:phasePathDescription::string as phase_path_description,
        -- unflattened lists
        v:pathMilestones as v_path_milestones,
        -- references
        v:pathReference as path_reference,
        -- edfi extensions
        v:_ext as v_ext

    from path_phases
)
select * from renamed