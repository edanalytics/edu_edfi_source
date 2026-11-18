{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_staff as (
    select * from {{ ref('stg_ef3__staffs') }}
),
flattened as (
    select 
        tenant_code, 
        api_year, 
        k_staff, 
        value:credentialReference.credentialIdentifier::string as credential_identifier,
        {{ extract_descriptor('value:credentialReference.stateOfIssueStateAbbreviationDescriptor::string') }} as state_of_issue_state_abbreviation
    from stg_staff
        {{ json_flatten('v_credentials') }}
),
extended as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(credential_identifier)',
            'lower(state_of_issue_state_abbreviation)']
        ) }} as k_credential,
        flattened.*
    from flattened
)
select *
from extended
