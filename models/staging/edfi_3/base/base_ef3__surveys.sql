with surveys as (
    {{ source_edfi3('surveys') }}
),
renamed as (
    select 
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                                  as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:sessionReference:school_id::string                          as school_id,
        v:sessionReference:session_name::string                       as session_name,
        v:schoolYearTypeReference:schoolYear::string                  as school_year,
        v:surveyIdentifier::string                                    as survey_id,
        v:namespace::string                                           as namespace,
        v:numberAdministered::int                                     as number_administered,
        v:survey_title::string                                        as survey_title,
        -- descriptors
        {{ extract_descriptor('v:surveyCategoryDescriptor::string') }} as survey_category,
        --references
        v:educationOrganizationReference as education_organization_reference,
        v:schoolYearTypeReference        as school_year_reference,
        v:sessionReference               as session_reference,
        -- edfi extensions
        v:_ext as v_ext
    from surveys
)
select * from renamed