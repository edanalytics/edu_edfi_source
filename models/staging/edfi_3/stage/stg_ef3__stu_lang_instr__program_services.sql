-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ edu_edfi_source.stg_student_program_services(
    'stg_ef3__student_language_instruction_program_associations',
    program_service_descriptor='languageInstructionProgramServiceDescriptor',
    program_services_array='v_language_instruction_program_services',
    extra_columns=['value:providers as v_providers']
) }}
