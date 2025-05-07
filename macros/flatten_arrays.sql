-- flatten emails
{% macro flatten_emails(stg_ref, keys) %}
with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {%- for key in keys %}
        {{key}},
        {% endfor -%}
        {{ edu_edfi_source.extract_descriptor('value:electronicMailTypeDescriptor::string') }} as email_type,
        lower(value:electronicMailAddress::string)                             as email_address,
        value:primaryEmailAddressIndicator::boolean                            as is_primary_email,
        value:doNotPublishIndicator::boolean                                   as do_not_publish
    from stg
        , lateral flatten(input=>v_electronic_mails)
)
select * from flattened
{% endmacro %}

-- flatten telephones
{% macro flatten_telephones(stg_ref, keys) %}
with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {%- for key in keys %}
        {{key}},
        {% endfor -%}
        {{ edu_edfi_source.extract_descriptor('value:telephoneNumberTypeDescriptor::string') }} as phone_number_type,
        value:telephoneNumber::string                                           as phone_number,
        value:orderOfPriority::int                                              as priority_order,
        value:doNotPublishIndicator::boolean                                    as do_not_publish,
        value:textMessageCapabilityIndicator::boolean                           as is_text_message_capable
    from stg
        , lateral flatten(input=>v_telephones)
)
select * from flattened
{% endmacro %}

-- flatten addresses
{% macro flatten_addresses(stg_ref, keys) %}
with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {%- for key in keys %}
        {{key}},
        {% endfor -%}
        {{ edu_edfi_source.extract_descriptor('addr.value:addressTypeDescriptor::string') }} as address_type,
        addr.value:streetNumberName::string as street_address,
        addr.value:apartmentRoomSuiteNumber::string as apartment_room_suite_number,
        addr.value:city::string as city,
        addr.value:nameOfCounty::string name_of_county,
        {{ edu_edfi_source.extract_descriptor('addr.value:stateAbbreviationDescriptor::string') }} as state_code,
        addr.value:postalCode::string as postal_code,
        addr.value:buildingSiteNumber::string as building_site_number,
        {{ edu_edfi_source.extract_descriptor('addr.value:localeDescriptor::string') }} as locale,
        addr.value:congressionalDistrict::string as congressional_district,
        addr.value:countyFIPSCode::string as county_fips_code,
        addr.value:doNotPublishIndicator::boolean as do_not_publish,
        addr.value:latitude::string as latitude,
        addr.value:longitude::string as longitude,
        timing.value:beginDate::date as address_begin_date,
        timing.value:endDate::date as address_end_date
    from stg
        , lateral flatten(input=>v_addresses) as addr
        , lateral flatten(input=>addr.value:periods, outer=>true) as timing
),
full_address as (
    select *,
        concat(
            street_address, ', ',
            ifnull(apartment_room_suite_number, ''),
            case
                when apartment_room_suite_number is null
                    then ''
                else ', '
            end,
            city, ', ',
            state_code, ' ',
            postal_code
        ) as full_address
    from flattened
)
select * from full_address
{% endmacro %}
