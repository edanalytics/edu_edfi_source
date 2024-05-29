with cohorts as (
    {{ source_edfi3('cohorts') }}
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
        v:id::string                                                  as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:cohortDescription::string                                   as cohort_description,
        v:cohortIdentifier::string                                    as cohort_id,
        -- descriptors
        {{ extract_descriptor('v:cohortScopeDescriptor::string') }} as cohort_scope,
        {{ extract_descriptor('v:cohortTypeDescriptor::string') }}  as cohort_type,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        -- lists
        v:programs as v_programs,
        
        -- edfi extensions
        v:_ext as v_ext
    from cohorts
)
select * from renamed