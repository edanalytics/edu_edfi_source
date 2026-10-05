{{ edu_edfi_source.stg_student_program_services(
    'stg_ef3__student_special_education_program_associations',
    program_service_descriptor='specialEducationProgramServiceDescriptor',
    program_services_array='v_special_education_program_services',
    child_program_service_fields=[{'name': 'providers', 'alias': 'v_providers'}],
    extract_extensions=True
) }}
