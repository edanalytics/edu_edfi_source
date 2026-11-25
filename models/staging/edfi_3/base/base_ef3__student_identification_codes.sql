-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with stu_id_codes as (
    {{ source_edfi3('student_identification_codes') }}
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
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        {{ extract_descriptor('v:studentIdentificationSystemDescriptor::string') }} as id_system,

        -- value columns
        v:assigningOrganizationIdentificationCode::string as assigning_org,
        v:identificationCode::string                      as id_code,

        -- references
        v:studentReference               as student_reference,
        v:educationOrganizationReference as education_organization_reference,

        -- edfi extensions
        v:_ext as v_ext

    from stu_id_codes
)
select * from renamed
