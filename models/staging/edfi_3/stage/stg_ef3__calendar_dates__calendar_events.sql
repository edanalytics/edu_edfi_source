-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with stg_calendar_dates as (
    select * from {{ ref('stg_ef3__calendar_dates') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_school_calendar,
        k_calendar_date,
        n_calendar_events,
        {{ extract_descriptor('value:calendarEventDescriptor::string') }} as calendar_event
    from stg_calendar_dates
        {{ json_flatten('v_calendar_events') }}
)
select * from flattened
