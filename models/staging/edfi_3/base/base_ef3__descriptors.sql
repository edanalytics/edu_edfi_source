-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with descriptors as (
    {{ source_edfi3('_descriptors', join_deletes=False) }}
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
        name                           as descriptor_name,
        v:id::string                   as record_guid,
        ods_version,
        data_model_version,
        v:codeValue::string            as code_value,
        v:namespace::string            as namespace,
        v:description::string          as description,
        v:shortDescription::string     as short_description
    from descriptors
)
select * 
from renamed
