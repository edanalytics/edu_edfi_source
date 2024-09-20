with network_associations as (
    {{ source_edfi3('education_organization_network_associations') }}
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
        v:id::string                            as record_guid,
        ods_version,
        data_model_version,
        v:educationOrganizationNetworkReference:educationOrganizationNetworkId::int as network_id,
        v:memberEducationOrganizationReference:educationOrganizationId::int         as ed_org_id,
        v:beginDate::date                       as begin_date,
        v:endDate::date                         as end_date,
        -- references
        v:educationOrganizationNetworkReference as network_reference,
        v:memberEducationOrganizationReference  as education_organization_reference,
        -- edfi extensions
        v:_ext as v_ext

    from network_associations
)
select * from renamed
