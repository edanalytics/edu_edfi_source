with base_student_discipline_incident_behavior as (
    select * from {{ ref('base_ef3__student_discipline_incident_behavior_associations') }}
    where not is_deleted
),
-- note: this model is deprecated, but still in use so stacking here
-- projects should only ever have one of the two models in use
base_student_discipline_incident as (
    -- note: the deprecated model needs to be flattened to match the grain of the new model
    select 
        {{ dbt_utils.star(ref('base_ef3__student_discipline_incident_associations'), 
            except=['student_participation_code', 'v_behaviors']) }},
        {{ extract_descriptor('value:behaviorDescriptor::string') }} as behavior_type,
        value:behaviorDetailedDescription::string as behavior_detailed_description
        array_agg(student_participation_code) over (partition by incident_id, school_id, student_id) as v_discipline_incident_participation_codes
    from {{ ref('base_ef3__student_discipline_incident_associations') }}
    , lateral flatten(input=>v_behaviors)
    where not is_deleted
    {% set non_offender_codes =  var('edu:discipline:non_offender_codes')  %}
    {% if non_offender_codes | length -%}
      {% if non_offender_codes is string -%}
        {% set non_offender_codes = [non_offender_codes] %}
      {%- endif -%}
      -- only offenders
      and student_participation_code not in (
      '{{ non_offender_codes | join("', '") }}'
      )
),
stacked as (
    select * from base_student_discipline_incident_behavior
    union all
    select * from base_student_discipline_incident
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
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