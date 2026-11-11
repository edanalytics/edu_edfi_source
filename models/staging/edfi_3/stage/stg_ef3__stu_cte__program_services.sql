-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ edu_edfi_source.stg_student_program_services(
    'stg_ef3__student_cte_program_associations',
    program_service_descriptor='cteProgramServiceDescriptor',
    program_services_array='v_cte_program_services',
    extra_columns=['value:cipCode::string as cip_code']
) }}
