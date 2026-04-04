with staff_ed_org_contact_assoc as (
    {{ source_edfi3('staff_education_organization_contact_associations') }}
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
        v:contactTitle::string                                        as contact_title,
        {{ extract_descriptor('v:contactTypeDescriptor::string') }}   as contact_type,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        lower(v:electronicMailAddress::string)                        as email_address,
        v:staffReference:staffUniqueId::string                        as staff_unique_id,
        -- address fields
        {{ extract_descriptor('v:address.addressTypeDescriptor::string') }} as address_type,
        {{ extract_descriptor('v:address.localeDescriptor::string') }} as locale,
        v:address.streetNumberName::string                            as street_number_name,
        v:address.apartmentRoomSuiteNumber::string                    as apartment_room_suite_number,
        v:address.city::string                                        as city,
        {{ extract_descriptor('v:address.stateAbbreviationDescriptor::string') }} as state_abbreviation,
        v:address.postalCode::string                                  as postal_code,
        v:address.nameOfCounty::string                                as county_name,
        v:address.countyFIPSCode::string                              as county_fips_code,
        v:address.buildingSiteNumber::string                          as building_site_number,
        v:address.congressionalDistrict::string                       as congressional_district,
        v:address.doNotPublishIndicator::boolean                      as do_not_publish_indicator,
        v:address.latitude::string                                    as latitude,
        v:address.longitude::string                                   as longitude,
        v:address.periods                                             as address_periods,
        -- arrays
        v:telephones                                                  as v_telephones,
        -- references
        v:educationOrganizationReference                              as education_organization_reference,
        v:staffReference                                              as staff_reference,
        -- edfi extensions
        v:_ext                                                        as v_ext

    from staff_ed_org_contact_assoc
)
select * from renamed
