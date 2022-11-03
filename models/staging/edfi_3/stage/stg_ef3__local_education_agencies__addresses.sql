with stage_leas as (
    select * from {{ ref('stg_ef3__local_education_agencies') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_lea,
        {{ extract_descriptor('addr.value:addressTypeDescriptor::string') }} as address_type,
        addr.value:streetNumberName::string as street_address,
        addr.value:apartmentRoomSuiteNumber::string as apartment_room_suite_number,
        addr.value:city::string as city,
        addr.value:nameOfCounty::string name_of_county,
        {{ extract_descriptor('addr.value:stateAbbreviationDescriptor::string') }} as state_code,
        addr.value:postalCode::string as postal_code,
        addr.value:buildingSiteNumber::string as building_site_number,
        {{ extract_descriptor('addr.value:localeDescriptor::string') }} as locale,
        addr.value:congressionalDistrict::string as congressional_district,
        addr.value:countyFIPSCode::string as county_fips_code,
        addr.value:doNotPublishIndicator::boolean as do_not_publish_indicator,
        addr.value:latitude::string as latitude,
        addr.value:longitude::string as longitude,
        timing.value:beginDate::date as address_begin_date,
        timing.value:endDate::date as address_end_date
    from stage_leas
        , lateral flatten(input=>v_addresses) as addr
        , lateral flatten(input=>addr.value:periods, outer=>true) as timing
)
select * from flattened

