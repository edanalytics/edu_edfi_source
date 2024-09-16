{% macro gen_skey(k_name, alt_ref=None, alt_k_name=None, extras=None) %}
     
    {#- make these alphabetical for predictability -#}
    {% set skey_defs = {
        'k_school': {
            'reference_name': 'school_reference',
            'col_list': ['schoolId'],
            'annualize': False
             },
        'k_lea': {
            'reference_name': 'local_education_agency_reference',
            'col_list': ['localEducationAgencyId'],
            'annualize': False
        },
        'k_sea': {
            'reference_name': 'state_education_agency_reference',
            'col_list': ['stateEducationAgencyId'],
            'annualize': False
        },
        'k_esc': {
            'reference_name': 'education_service_center_reference',
            'col_list': ['educationServiceCenterId'],
            'annualize': False
        },
        'k_student': {
            'reference_name': 'student_reference',
            'col_list': ['studentUniqueId'],
            'annualize': True
        },
        'k_student_xyear': {
            'reference_name': 'student_reference',
            'col_list': ['studentUniqueId'],
            'annualize': False
        },
        'k_staff': {
            'reference_name': 'staff_reference',
            'col_list': ['staffUniqueId'],
            'annualize': False
        },
        'k_course': {
            'reference_name': 'course_reference',
            'col_list': ['courseCode', 
                         'educationOrganizationId'],
            'annualize': True
        },
        'k_course_offering': {
            'reference_name': 'course_offering_reference',
            'col_list': ['localCourseCode', 
                         'schoolId', 
                         'schoolYear', 
                         'sessionName'],
            'annualize': False
        },
        'k_course_section': {
            'reference_name': 'section_reference',
            'col_list': ['localCourseCode', 
                         'schoolId', 
                         'schoolYear',
                         'sectionIdentifier',
                         'sessionName'],
            'annualize': False
        },
        'k_grading_period': {
            'reference_name': 'grading_period_reference',
            'col_list': ['gradingPeriodDescriptor',
                         'periodSequence',
                         'schoolId',
                         'schoolYear'],
            'annualize': False
        },
        'k_session': {
            'reference_name': 'session_reference',
            'col_list': ['schoolId', 
                         'schoolYear', 
                         'sessionName'],
            'annualize': False
        },
        'k_class_period': {
            'reference_name': 'class_period_reference',
            'col_list': ['classPeriodName',
                         'schoolId'],
            'annualize': True
        },
        'k_location': {
            'reference_name': 'location_reference',
            'col_list': ['classroomIdentificationCode',
                         'schoolId'],
            'annualize': True
        },
        'k_program': {
            'reference_name': 'program_reference',
            'col_list': ['educationOrganizationId',
                         'programName',
                         'programTypeDescriptor'],
            'annualize': True
        },
        'k_student_academic_record': {
            'reference_name': 'student_academic_record_reference',
            'col_list': ['educationOrganizationId',
                         'schoolYear',
                         'studentUniqueId',
                         'termDescriptor'],
            'annualize': False
        },
        'k_school_calendar': {
            'reference_name': 'calendar_reference',
            'col_list': ['calendarCode',
                         'schoolId',
                         'schoolYear'],
            'annualize': False
        },
        'k_calendar_date': {
            'reference_name': 'school_calendar_reference',
            'col_list': ['calendarCode',
                         'date',
                         'schoolId',
                         'schoolYear'],
            'annualize': False
        },
        'k_ed_org': {
            'reference_name': 'education_organization_reference',
            'col_list': ['educationOrganizationId'],
            'annualize': False
        },
        'k_graduation_plan': {
            'reference_name': 'graduation_plan_reference',
            'col_list': [
                'educationOrganizationId',
                'graduationPlanTypeDescriptor',
                'graduationSchoolYear'
            ],
            'annualize': True
        },
        'k_assessment': {
            'reference_name': 'assessment_reference',
            'col_list': ['assessmentIdentifier', 
                         'namespace'],
            'annualize': True
        },
        'k_objective_assessment': {
            'reference_name': 'objective_assessment_reference',
            'col_list': ['assessmentIdentifier', 
                         'namespace',
                         'identificationCode'],
            'annualize': True
        },
        'k_discipline_incident': {
            'reference_name': 'discipline_incident_reference',
            'col_list': ['incidentIdentifier',
                         'schoolId'],
            'annualize': True
        },
        'k_network': {
            'reference_name': 'network_reference',
            'col_list': ['educationOrganizationNetworkId'],
            'annualize': False
        },
        'k_bell_schedule': {
            'reference_name': 'bell_schedule_reference',
            'col_list': ['bellScheduleName', 'schoolId'],
            'annualize': True
        },
        'k_parent': {
            'reference_name': 'parent_reference',
            'col_list': ['parentUniqueId'],
            'annualize': False
        },
        'k_cohort': {
            'reference_name': 'cohort_reference',
            'col_list': ['cohortIdentifier', 'educationOrganizationId'],
            'annualize': True
        },
        'k_survey': {
            'reference_name': 'survey_reference',
            'col_list': ['namespace', 'surveyIdentifier'],
            'annualize': True
        },
        'k_survey_question': {
            'reference_name': 'survey_question_reference',
            'col_list': ['questionCode', 'surveyIdentifier'],
            'annualize': True
        },
        'k_survey_response': {
            'reference_name': 'survey_response_reference',
            'col_list': ['surveyIdentifier', 'surveyResponseIdentifier'],
            'annualize': True
        },
        'k_learning_standard': {
            'reference_name': 'learning_standard_reference',
            'col_list': ['learningStandardId'],
            'annualize': True
        },
        'k_contact': {
            'reference_name': 'contact_reference',
            'col_list': ['contactUniqueId'],
            'annualize': False
        },

        'k_template': {
            'reference_name': '',
            'col_list': [],
            'annualize': False
        },

    }
    %}
    {#- retrieve key def for then decompose parts -#}
    {% set skey_def = skey_defs[k_name] %}
    {% set skey_ref = skey_def['reference_name'] %}
    {% set skey_vars = skey_def['col_list'] %}

    {#- deal with special case: references embedded in unusual cases
        Note: we still want the same values, we just pull them from another place
        Example: getting k_section out of studentSectionAssociationReference instead of sectionReference
     -#}
    {% if alt_ref %}
      {% set skey_ref = alt_ref %}
    {%- endif -%}

    iff(
        {{ skey_ref }} is not null, 
        {{ dbt_utils.surrogate_key(edu_edfi_source.gen_key_list(skey_def, skey_ref, skey_vars, extras=extras)) }}, 
        null
    )::varchar(32) as {{ alt_k_name or k_name }}
{%- endmacro -%}

{% macro gen_key_list(skey_def, skey_ref, skey_vars, extras=None) %}


    {% set consts = ['tenant_code'] %}
    {% if skey_def['annualize'] %}
        {% set consts = ['tenant_code', 'api_year'] %}
    {% endif %}

    
    {#- add our key constants to the output -#}
    {% set output = consts %}
    {#- add extra columns that do not exist in ref to output, always lower -#}
    {% if extras %}
        {% for var in extras %}
            {% set lower_var = 'lower(' + var + ')' %}
            {% do output.append(lower_var) %}
        {% endfor %}
    {% endif %}

    {#- loop over key vars, concattenating reference name and var name -#}
    {% for skey_var in skey_vars %}
      {% set concatted_keys = skey_ref + ':' + skey_var %}

      {#- hack: if key contains Date, coerce to drop timestamp info -#}
      {% if 'Date' in skey_var %}
        {%- set concatted_keys = concatted_keys + '::string::date' %}
      {% endif %}

      {#- hack: wrap in lower to deal with case insensitive collations -#}
      {% set concatted_keys = 'lower(' + concatted_keys + ')' %}

      {#- hack: if key contains Descriptor, parse value out -#}
      {% if 'Descriptor' in skey_var or 'descriptor' in skey_var %}
        {%- set concatted_keys = edu_edfi_source.extract_descriptor(concatted_keys) %}
      {% endif %}
      {#- grow the output object with the new key -#}
      {% do output.append(concatted_keys) %}
    {% endfor %}

    {{ return(output) }}
  
{% endmacro %}
