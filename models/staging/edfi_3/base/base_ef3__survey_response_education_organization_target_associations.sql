with survey_response_education_organization_target_associations as (
    {{ source_edfi3('survey_response_education_organization_target_associations') }}
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

        v:id::string as record_guid,
        -- identity components
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:surveyResponseReference:namespace::string                   as namespace,
        v:surveyResponseReference:surveyIdentifier::string            as survey_id,
        v:surveyResponseReference:surveyResponseIdentifier::string    as survey_response_id,
        -- non-identity components
        v:educationOrganizationReference:link:rel::string as ed_org_type,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:surveyResponseReference        as survey_response_reference
    from survey_response_education_organization_target_associations
)
select * from renamed
