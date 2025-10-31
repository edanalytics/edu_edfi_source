with source_student_iep_associations as (
    {{ source_edfi3('student_iep_associations') }}
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

        v:id::string                                                            as record_guid,
        ods_version,
        data_model_version,
        
        -- identity components (composite key)
        v:studentIEPAssociationID::string                                       as student_iep_association_id,
        v:educationOrganizationReference:educationOrganizationId::int           as iep_servicing_ed_org_id,
        v:educationOrganizationReference:link:rel::string                       as iep_servicing_ed_org_type,
        v:iepFinalizedDate::date                                                as iep_finalized_date,
        v:studentReference:studentUniqueId::string                              as student_unique_id,
        
        -- required fields
        v:iepBeginDate::date                                                    as iep_begin_date,
        v:iepEndDate::date                                                      as iep_end_date,
        {{ extract_descriptor('v:iepStatusDescriptor::string') }}               as iep_status,
        
        -- optional fields
        v:iepAmendedDate::date                                                  as iep_amended_date,
        v:medicallyFragile::boolean                                             as is_medically_fragile,
        v:multiplyDisabled::boolean                                             as is_multiply_disabled,
        v:schoolHoursPerWeek::float                                             as school_hours_per_week,
        v:specialEducationHoursPerWeek::float                                   as spec_ed_hours_per_week,
        
        -- descriptors
        {{ extract_descriptor('v:reasonExitedDescriptor::string') }}            as reason_exited,
        {{ extract_descriptor('v:specialEducationSettingDescriptor::string') }} as special_education_setting,
        
        -- references
        v:educationOrganizationReference                                        as education_organization_reference,
        v:studentReference                                                      as student_reference,
        v:studentSpecialEducationProgramAssociationReference                    as student_spec_ed_program_association_reference,
        v:studentSpecialEducationProgramEligibilityAssociationReference         as student_spec_ed_program_eligibility_association_reference,
        
        -- collections
        v:ideaevents                                                            as v_ideaevents,
        
        -- edfi extensions
        v:_ext                                                                  as v_ext

    from source_student_iep_associations
)

select * from renamed

