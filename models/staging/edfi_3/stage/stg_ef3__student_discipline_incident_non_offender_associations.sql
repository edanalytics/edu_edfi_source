with base_student_discipline_incident_non_offender as (
    select * from {{ ref('base_ef3__student_discipline_incident_non_offender_associations') }}
    where not is_deleted
),
base_student_discipline_incident as (
    select * from {{ ref('base_ef3__student_discipline_incident_associations') }}
    where not is_deleted
),
dedupe_base_student_discipline_incident  as (
    {{
        dbt_utils.deduplicate(
            relation='base_student_discipline_incident',
            partition_by='student_unique_id, school_id, incident_id',
            order_by='pull_timestamp desc'
        )
    }}
),
-- note: this model is deprecated, but still in use so stacking here
-- projects should only ever have one of the two models in use
format_student_discipline_incident as (
    select 
        {{ dbt_utils.star(ref('base_ef3__student_discipline_incident_associations'), 
            except=['student_participation_code', 'v_behaviors', 'v_ext']) }},
        array_agg(object_construct('disciplineIncidentParticipationCodeDescriptor',student_participation_code)) 
            over (partition by incident_id, school_id, student_unique_id) as v_discipline_incident_participation_codes,
        v_ext
    from dedupe_base_student_discipline_incident
    {% set non_offender_codes =  var('edu:discipline:non_offender_codes')  %}
    -- todo: not sure we want the option for this to be empty
    {% if non_offender_codes | length -%}
      {% if non_offender_codes is string -%}
        {% set non_offender_codes = [non_offender_codes] %}
      {%- endif -%}
      -- only non offenders
      -- Victim, Witness, etc
      where student_participation_code in (
      '{{ non_offender_codes | join("', '") }}'
      )
    {%- endif -%}
),
stacked as (
    select * from base_student_discipline_incident_non_offender
    union all
    select * from format_student_discipline_incident
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_school', 'discipline_incident_reference') }},
        {{ gen_skey('k_discipline_incident') }},
        stacked.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from stacked
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_discipline_incident',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped