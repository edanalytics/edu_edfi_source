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
        v:id::string as record_guid,
        ods_version,
        data_model_version,
        -- key columns
        v:iepServiceDeliveryID::string                                    as iep_service_delivery_id,
        v:studentReference:studentUniqueId::string                        as student_unique_id,
        v:studentIEPReference:studentIEPAssociationID::string             as student_iep_association_id,
        {{ extract_descriptor('v:serviceDeliveryDksescriptor::string') }} as service_delivery,
        v:serviceDeliveryDate::date                                       as service_delivery_date,
        -- value columns
        v:studentIEPServicePrescriptionReference:iepServiceID::string          as iep_service_prescription_id,
        v:studentIEPServicePrescriptionReference:servicePrescriptionDate::date as service_prescription_date,
        v:serviceProviderStaffReference:staffUniqueId::string                  as service_provider_staff_unique_id,
        -- descriptors
        {{ extract_descriptor('v:serviceProviderDescriptor::string') }} as service_provider_type,
        -- references
        v:studentReference                       as student_reference,
        v:studentIEPReference                    as student_iep_reference,
        v:studentIEPServicePrescriptionReference as student_iep_service_prescription_reference,
        v:serviceProviderStaffReference          as service_provider_staff_reference,
        v:ideaeventReferences                    as ideaevent_references,
        -- lists
        v:serviceProviders          as v_service_providers,
        v:externalServiceProviders  as v_external_service_providers,
        
        -- edfi extensions
        v:_ext as v_ext

    from source_student_iep_service_deliveries
)

select * from renamed
