with source_stu_program as (
    {{ source_edfi3('student_school_food_service_program_associations') }}
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

        v:ods_version::string,
        data_model_version,
        v:id::string                                                  as record_guid, 
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        TRY_TO_DATE(TRIM(v:beginDate::string), 'YYYY-MM-DD')          as begin_date,
        TRY_TO_DATE(TRIM(v:endDate::string), 'YYYY-MM-DD')            as end_date,
        v:programReference:educationOrganizationId::int               as program_ed_org_id, 
        v:programReference:programName::string                        as program_name,
        v:programReference:link:rel::string                           as program_reference_rel,
        v:studentReference:studentUniqueId::int                       as student_unique_id,
        v:studentReference:link:rel::string                           as student_reference_rel, 


        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }} as program_type_descriptor,

        -- references
        v:educationOrganizationReference                              as education_organization_reference,
        v:programReference                                            as program_reference,
        v:studentReference                                            as student_reference,


        -- lists
        v:programParticipationStatuses                                as program_participation_statuses,
        v:schoolFoodServiceProgramServices                            as school_food_service_program_services,

        -- edfi extensions
        v:_ext                                                        as v_ext
    from source_stu_program
)

select * from renamed