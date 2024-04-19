with source_stu_responsibility as (
    {{ source_edfi3('student_education_organization_responsibility_associations') }}
),

renamed as (
    select
        -- generic columns
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                                      as record_guid,
        v:studentReference:studentUniqueId::string                        as student_unique_id,
        v:educationOrganizationReference:educationOrganizationId::integer as ed_org_id,
        v:educationOrganizationReference:link:rel::string                 as ed_org_type,
        v:beginDate::date                                                 as begin_date,
        v:endDate::date                                                   as end_date,

        -- descriptors
        {{ extract_descriptor('v:responsibilityDescriptor::string') }} as responsibility,

        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:studentReference               as student_reference,

        -- edfi extensions
        v:_ext as v_ext

    from source_stu_responsibility
)

select * from renamed
