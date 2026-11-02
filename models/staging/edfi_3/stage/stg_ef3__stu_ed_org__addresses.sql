-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ flatten_addresses('stg_ef3__student_education_organization_associations', 
    ['k_student', 'k_student_xyear', 'ed_org_id', 'k_lea', 'k_school']) }}
