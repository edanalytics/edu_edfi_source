with path_milestones as (
    {{ source_edfi3('path_milestones') }}
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
        v:pathMilestoneName::string as path_milestone_name,
        {{ extract_descriptor('v:pathMilestoneTypeDescriptor::string') }} as path_milestone_type,

        -- non-identity components
        v:pathMilestoneCode::string as path_milestone_code,
        v:pathMilestoneDescription::string as path_milestone_description,
        -- edfi extensions
        v:_ext as v_ext

    from path_milestones
)
select * from renamed