-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key='k_school',
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with schools_keyed as (
    select * from {{ ref('int_ef3__schools_keyed') }}
),
-- .. dedupe across years to enforce the correct grain, keeping latest year that wasn't deleted
deduped_across_years as (
    {{
        dbt_utils.deduplicate(
            relation='schools_keyed',
            partition_by='k_school',
            order_by='api_year desc'
        )
    }}
)
select * from deduped_across_years
