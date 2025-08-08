with student_path_milestone_statuses as (
    {{ source_edfi3('student_path_milestone_statuses') }}
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
        v:pathMilestoneReference:pathMilestoneName::string                                       as path_milestone_name,
        {{ extract_descriptor('v:pathMilestoneReference:pathMilestoneTypeDescriptor::string') }} as path_milestone_type,
        v:studentPathReference:educationOrganizationId::int                                      as ed_org_id,
        v:studentPathReference:pathName::string                                                  as path_name,
        v:studentPathReference:studentUniqueId::string                                           as student_unique_id,
        -- non-identity components
        v:completionIndicator::boolean          as has_completed,
        v:event:description::string             as status_description,
        v:event:pathMilestoneStatusDate::string as status_date,
        -- descriptors
        {{ extract_descriptor('v:event:pathMilestoneStatusDescriptor::string') }} as status_descriptor,
        -- references
        v:pathMilestoneReference as path_milestone_reference,
        v:studentPathReference   as student_path_reference,
        v:pathPhaseReference     as path_phase_reference,

        -- edfi extensions
        v:_ext as v_ext

    from student_path_milestone_statuses
)
select * from renamed