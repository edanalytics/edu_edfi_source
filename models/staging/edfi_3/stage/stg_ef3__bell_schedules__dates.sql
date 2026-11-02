-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_bell_schedule,
        k_school,
        value:date::date as calendar_date
    from stg_bell_schedules
        {{ json_flatten('v_dates') }}
)
select * from flattened
