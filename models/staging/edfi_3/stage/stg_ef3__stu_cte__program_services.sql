{{ edu_edfi_source.stg_student_program_services(
    'stg_ef3__student_cte_program_associations',
    program_service_descriptor='cteProgramServiceDescriptor',
    program_services_array='v_cte_program_services',
    child_program_service_fields=[{'name': 'cipCode::string', 'alias': 'cip_code'}]
) }}
