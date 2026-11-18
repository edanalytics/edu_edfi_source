-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with schools_keyed as (
    select * from {{ ref('int_ef3__schools_keyed') }}
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='schools_keyed',
            partition_by='k_school, api_year',
            order_by='api_year desc'
        )
    }}
),
flattened as (
    select
        tenant_code,
        api_year,
        api_year as school_year,
        k_school,
        {{ extract_descriptor('value:gradeLevelDescriptor::string') }} as grade_level
    from deduped
        {{ json_flatten('v_grade_levels') }}
)
select * from flattened
