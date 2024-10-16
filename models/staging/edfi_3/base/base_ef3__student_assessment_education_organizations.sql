with student_assessment_education_organizations as (
    select * from {{ source_edfi3('student_assessment_education_organizations') }}
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
        v:id::string                                                                            as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int                           as ed_org_identifier,
        v:educationOrganizationReference:link:rel::string                                       as ed_org_type,
        {{ extract_descriptor('v:educationOrganizationAssociationTypeDescriptor::string') }}    as ed_org_association_type,
        v:schoolYearTypeReference:schoolYear::int                                               as school_year,
        v:studentAssessmentReference:assessmentIdentifier::string                               as assement_identifier,
        v:studentAssessmentReference:namespace::string                                          as assessment_name_space,
        v:studentAssessmentReference:studentAssessmentIdentifier::string                        as student_assessment_identifier,
        v:studentAssessmentReference:studentUniqueId::string                                    as student_id,
        -- references
        v:educationOrganizationReference                                                        as education_organization_reference,
        v:studentAssessmentReference                                                            as student_assessment_reference
        -- edfi extensions
        v:_ext                                                                                  as v_ext
    from student_assessment_education_organizations
)


select  * from renamed





