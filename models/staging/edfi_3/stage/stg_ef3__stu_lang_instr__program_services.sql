{{ edu_edfi_source.stg_student_program_services(
    'stg_ef3__student_language_instruction_program_associations',
    program_service_descriptor='languageInstructionProgramServiceDescriptor',
    program_services_array='v_language_instruction_program_services',
    child_program_service_fields=[{'name': 'providers', 'alias': 'v_providers'}]
) }}
