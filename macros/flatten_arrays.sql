-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

-- flatten emails
{% macro flatten_emails(stg_ref, keys) %}
with stg as (
    {{ edu_edfi_source._flatten_stg_source(stg_ref, keys, 'v_electronic_mails') }}
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
        {{ edu_edfi_source.json_flatten('v_electronic_mails') }}
)
select * from flattened
{% endmacro %}

-- flatten telephones
{% macro flatten_telephones(stg_ref, keys) %}
with stg as (
    {{ edu_edfi_source._flatten_stg_source(stg_ref, keys, 'v_telephones') }}
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
        {{ edu_edfi_source.json_flatten('v_telephones') }}
)
select * from flattened
{% endmacro %}

-- flatten addresses
{% macro flatten_addresses(stg_ref, keys) %}
with stg as (
    {{ edu_edfi_source._flatten_stg_source(stg_ref, keys, 'v_addresses') }}
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
        {{ edu_edfi_source.json_flatten('v_addresses', 'addr') }}
        {{ edu_edfi_source.json_flatten('addr.value:periods', 'timing', outer=True) }}
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

{#
    Source CTE body for the flatten_* macros.
    stg_ref may be a single model name, or a list of model names to union. The list form is used where
    a list moved to a new resource across Ed-Fi data standards (e.g. SEOA addresses moved to
    studentDirectories in DS 6), so both sources need to feed the same sub-table.
#}
{% macro _flatten_stg_source(stg_ref, keys, list_col) %}
{%- if stg_ref is string %}
    select * from {{ ref(stg_ref) }}
{%- else %}
    {%- set relations = [] %}
    {%- for r in stg_ref %}
        {%- do relations.append(ref(r)) %}
    {%- endfor %}
    {{ dbt_utils.union_relations(
        relations=relations,
        include=['tenant_code', 'api_year'] + keys + [list_col]
    ) }}
{%- endif %}
{% endmacro %}
