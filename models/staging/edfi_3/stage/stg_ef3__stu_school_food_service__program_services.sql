-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ edu_edfi_source.stg_student_program_services(
    'stg_ef3__student_school_food_service_program_association',
    program_service_descriptor='schoolFoodServiceProgramServiceDescriptor',
    program_services_array='v_school_food_service_program_services'
) }}
