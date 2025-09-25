with paths as (
    {{ source_edfi3('paths') }}
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
        v:pathName::string                                            as path_name,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        -- non-identity components
        v:graduationPlanReference:educationOrganizationId::int                                     as graduation_plan_ed_org_id,
        {{ extract_descriptor('v:graduationPlanReference:graduationPlanTypeDescriptor::string') }} as graduation_plan_type,
        v:graduationPlanReference:graduationSchoolYear::int                                        as graduation_school_year,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:graduationPlanReference        as graduation_plan_reference,
        -- edfi extensions
        v:_ext as v_ext

    from paths
)
select * from renamed