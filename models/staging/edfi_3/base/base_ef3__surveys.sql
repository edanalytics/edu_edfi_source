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
        v:namespace::string                                           as namespace,
        v:surveyIdentifier::string                                    as survey_id,
        v:schoolYearTypeReference:schoolYear::string                  as school_year,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:surveyTitle::string                                         as survey_title,
        v:sessionReference:school_id::string                          as school_id,
        v:sessionReference:session_name::string                       as session_name,
        v:numberAdministered::int                                     as number_administered,
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