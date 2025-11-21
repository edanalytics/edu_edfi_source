with student_iep_service_prescriptions as (
    {{ source_edfi3('student_iep_service_prescriptions') }}
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
        ods_version,
        data_model_version,
        -- key columns
        v:iepServiceID::string                                                            as iep_service_id,
        v:studentReference:studentUniqueId::string                                        as student_unique_id,
        v:studentIEPReference:studentIEPAssociationID::string                             as student_iep_association_id,
        v:studentIEPReference:educationOrganizationReference:educationOrganizationId::int as iep_servicing_ed_org_id,
        v:studentIEPReference:iepFinalizedDate::date                                      as iep_finalized_date,
        {{ extract_descriptor('v:servicePrescriptionDescriptor::string') }}               as service_prescription,
        v:servicePrescriptionDate::date                                                   as service_prescription_date,
        -- value columns
        v:beginDate::date                                                   as begin_date,
        v:endDate::date                                                     as end_date,
        v:durationMinutes::float                                            as duration_minutes,
        v:frequencyValue::float                                             as frequency_value,
        v:serviceEducationOrganizationID::int                               as service_ed_org_id,
        v:serviceProviderStaffID::string                                    as service_provider_staff_id,
        -- descriptors
        {{ extract_descriptor('v:durationPeriodDescriptor::string') }}      as duration_period,
        {{ extract_descriptor('v:frequencyPeriodDescriptor::string') }}     as frequency_period,
        {{ extract_descriptor('v:serviceLocationTypeDescriptor::string') }} as service_location_type,
        -- references
        v:studentReference              as student_reference,
        v:studentIEPReference           as student_iep_reference,
        v:serviceProviderStaffReference as service_provider_staff_reference,
        v:ideaeventReferences           as ideaevent_references,
        
        -- edfi extensions
        v:_ext as v_ext

    from student_iep_service_prescriptions
)

select * from renamed
