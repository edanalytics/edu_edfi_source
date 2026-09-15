{#
    Macro to build for any student program associations

    base_model: name of the upstream base model to ref().
    annualize_edorg: allow to configure annualized edorg. Defaults to False.
#}
{% macro stg_student_program_association(base_model, annualize_edorg=False) %}

with base_stu_programs as (
    select * from {{ ref(base_model) }}
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(student_unique_id)',
                'ed_org_id',
                'program_ed_org_id',
                'lower(program_name)',
                'lower(program_type)',
                'program_enroll_begin_date'
            ]
        ) }} as k_student_program,
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.gen_skey('k_program') }},
        {{ edu_edfi_source.edorg_ref(annualize=annualize_edorg) }},
        api_year as school_year,
        base_stu_programs.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_stu_programs
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student_program',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted

{% endmacro %}


{#
    Unpacks programs services array into one row per service.

    stage_model: name of the upstream stage model to ref().
    program_service_descriptor: the JSON key for the service descriptor on each array element, e.g. 'homelessProgramServiceDescriptor'.
    program_services_array: the column holding the services array. Defaults to v_program_services.
    child_program_service_fields: additional fields to pull off from program_services_array, given as a list of {'name': ..., 'alias': ...}, where "name" is the expression to place after `value:`
    in the flattened row, optionally with a `::type` cast  and "alias" is the output column name, e.g. [{'name': 'cipCode::string', 'alias': 'cip_code'}].
    extract_extensions: set True when the array elements nested _ext payload that needs flattening.
#}
{% macro stg_student_program_services(
    stage_model,
    program_service_descriptor,
    program_services_array='v_program_services',
    child_program_service_fields=[],
    extract_extensions=False
) %}

with stage_stu_programs as (
    select * from {{ ref(stage_model) }}
),

flattened as (
    select
        k_student_program,
        tenant_code,
        api_year,
        school_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,
        ed_org_id,

        program_enroll_begin_date,
        program_enroll_end_date,
        {{ edu_edfi_source.extract_descriptor('value:' ~ program_service_descriptor ~ '::string') }} as program_service,
        value:primaryIndicator::boolean as primary_indicator,
        value:serviceBeginDate::date    as service_begin_date,
        value:serviceEndDate::date      as service_end_date,

        {%- if child_program_service_fields %}

        -- fields local to this program service, e.g. child_program_service_fields=[{'name': 'cipCode::string', 'alias': 'cip_code'}]
        -- renders as: value:cipCode::string as cip_code,
        {% for col in child_program_service_fields -%}
        value:{{ col.name }} as {{ col.alias }},
        {% endfor %}
        {%- endif %}

        -- edfi extensions
        value:_ext as v_ext

    from stage_stu_programs
        {{ edu_edfi_source.json_flatten(program_services_array) }}
)

{%- if extract_extensions %}

, extended as (
    select
        flattened.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from flattened
)

select * from extended

{%- else %}

select * from flattened

{%- endif %}

{% endmacro %}


{#
    Unpacks programs participation statuses array into one row per status.
    
    stage_model: name of the upstream stage model to ref().
    extract_extensions: same as in stg_student_program_services().
#}
{% macro stg_student_program_participation_statuses(stage_model, extract_extensions=False) %}

with stage_stu_programs as (
    select * from {{ ref(stage_model) }}
),

flattened as (
    select
        k_student_program,
        tenant_code,
        api_year,
        school_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,
        ed_org_id,

        program_enroll_begin_date,
        program_enroll_end_date,
        {{ edu_edfi_source.extract_descriptor('value:participationStatusDescriptor::string') }} as participation_status,
        value:statusBeginDate::date as status_begin_date,
        value:designatedBy::string  as designated_by,
        value:statusEndDate::date   as status_end_date,

        -- edfi extensions
        value:_ext as v_ext

    from stage_stu_programs
        {{ edu_edfi_source.json_flatten('v_program_participation_statuses') }}
)

{%- if extract_extensions %}

, extended as (
    select
        flattened.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from flattened
)

select * from extended

{%- else %}

select * from flattened

{%- endif %}

{% endmacro %}
