{{ edu_edfi_source.stg_student_program_services(
    'stg_ef3__student_homeless_program_associations',
    program_service_descriptor='homelessProgramServiceDescriptor',
    program_services_array='v_homeless_program_services',
    child_program_service_fields=[{'name': 'providers', 'alias': 'v_providers'}]
) }}
