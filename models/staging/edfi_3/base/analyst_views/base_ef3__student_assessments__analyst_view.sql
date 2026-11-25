-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ config(alias='base_ef3__student_assessments') }}
select * from {{ ref('base_ef3__student_assessments') }}
