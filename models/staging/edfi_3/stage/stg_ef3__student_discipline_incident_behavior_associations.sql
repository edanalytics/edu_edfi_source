with base_student_discipline_incident_behavior as (
    select * from {{ ref('base_ef3__student_discipline_incident_behavior_associations') }}
),
base_student_discipline_incident as (
    select * from {{ ref('base_ef3__student_discipline_incident_associations') }}
),
dedupe_base_student_discipline_incident  as (
    {{
        dbt_utils.deduplicate(
            relation='base_student_discipline_incident',
            partition_by='tenant_code, api_year, student_unique_id, school_id, incident_id',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
),
-- note: this model is deprecated, but still in use so stacking here
-- projects should only ever have one of the two models in use
format_student_discipline_incident as (
    -- note: the deprecated model needs to be flattened to match the grain of the new model
    select 
        {{ dbt_utils.star(ref('base_ef3__student_discipline_incident_associations'), 
            except=['student_participation_code', 'v_behaviors', 'v_ext', 'discipline_incident_reference', 'student_reference']) }},
            value:behaviorDetailedDescription::string as behavior_detailed_description,
        {{ extract_descriptor('value:behaviorDescriptor::string') }} as behavior_type,
        discipline_incident_reference,
        student_reference,
        array_agg(object_construct('disciplineIncidentParticipationCodeDescriptor',student_participation_code)) 
            over (partition by incident_id, school_id, student_unique_id) as v_discipline_incident_participation_codes,
        v_ext
    from dedupe_base_student_discipline_incident
    , lateral flatten(input=>v_behaviors)
    {% set non_offender_codes =  var('edu:discipline:non_offender_codes')  %}
    -- note: not allowing for non_offender_codes var to be empty
      {% if non_offender_codes is string -%}
        {% set non_offender_codes = [non_offender_codes] %}
      {%- endif -%}
      -- only offenders
      where student_participation_code not in (
      '{{ non_offender_codes | join("', '") }}'
      )
),
stacked as (
    select * from base_student_discipline_incident_behavior
    union all
    select * from format_student_discipline_incident
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school', 'discipline_incident_reference') }},
        {{ gen_skey('k_discipline_incident') }},
        api_year as school_year,
        stacked.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from stacked
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_discipline_incident, behavior_type',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
