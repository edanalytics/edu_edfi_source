with source_student_iep_service_deliveries as (
    {{ source_edfi3('student_iep_service_deliveries') }}
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

        v:id::string                                                                      as record_guid,
        ods_version,
        data_model_version,
        
        -- identity components (composite key)
        v:iepServiceDeliveryID::string                                                    as iep_service_delivery_id,
        v:studentReference:studentUniqueId::string                                        as student_unique_id,
        v:studentIEPReference:studentIEPAssociationID::string                             as student_iep_association_id,
        v:studentIEPReference:educationOrganizationReference:educationOrganizationId::int as iep_servicing_ed_org_id,
        v:studentIEPReference:iepFinalizedDate::date                                      as iep_finalized_date,
        {{ extract_descriptor('v:serviceDeliveryDescriptor::string') }}                   as service_delivery,
        v:serviceDeliveryDate::date                                                       as service_delivery_date,
        
        -- optional fields
        v:studentIEPServicePrescriptionReference:iepServiceID::string                     as iep_service_prescription_id,
        v:studentIEPServicePrescriptionReference:servicePrescriptionDate::date            as service_prescription_date,
        {{ extract_descriptor('v:serviceProviderDescriptor::string') }}                   as service_provider_type,
        v:serviceProviderStaffReference:staffUniqueId::string                             as service_provider_staff_unique_id,
        
        -- references
        v:studentReference                                                                as student_reference,
        v:studentIEPReference                                                             as student_iep_reference,
        v:studentIEPServicePrescriptionReference                                          as student_iep_service_prescription_reference,
        v:serviceProviderStaffReference                                                   as service_provider_staff_reference,
        v:ideaeventReferences                                                             as ideaevent_references,
        
        -- collections
        v:serviceProviders                                                                as v_service_providers,
        v:externalServiceProviders                                                        as v_external_service_providers,
        
        -- edfi extensions
        v:_ext                                                                            as v_ext

    from source_student_iep_service_deliveries
)

select * from renamed
