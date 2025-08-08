with recruitment_event_attendances as (
    {{ source_edfi3('recruitment_event_attendances') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        filename,
        file_row_number,
        is_deleted,

        v:id::string as record_guid,
        -- identity components
        v:recruitmentEventAttendeeIdentifier::string             as attendee_identifier,
        v:recruitmentEventReference:eventTitle::string           as event_title,
        v:recruitmentEventReference:eventDate::date              as event_date,
        v:recruitmentEventReference:educationOrganizationId::int as ed_org_id,
        -- non-identity components
        v:currentPosition:positionTitle::string                                        as current_position_title,
        {{ extract_descriptor('v:currentPosition:academicSubjectDescriptor::string') }} as current_position_academic_subject,
        v:currentPosition:nameOfInstitution::string                                    as current_position_institution_name,
        v:currentPosition:location::string                                             as current_position_location,
        v:firstName::string                                                            as first_name,
        v:lastSurname::string                                                          as last_surname,
        v:maidenName::string                                                           as maiden_name,
        v:middleName::string                                                           as middle_name,
        v:generationCodeSuffix::string                                                 as generation_code_suffix,
        v:personalTitlePrefix::string                                                  as personal_title_prefix,
        v:electronicMailAddress::string                                                as email_address,
        v:socialMediaNetworkName::string                                               as social_media_network_name,
        v:socialMediaUserName::string                                                  as social_media_username,
        v:met::boolean                                                                 as was_met,  -- indicates if person was met by a representative of the ed org, is there a better naming?
        v:preScreeningRating::integer                                                  as pre_screening_rating,
        v:applied::boolean                                                             as has_applied,  -- indicates whether the prospect applied for a position. There might be a better name for this.
        v:recruitmentEventAttendeeQualifications:eligible::boolean                     as is_eligible,
        v:recruitmentEventAttendeeQualifications:capacityToServe::boolean              as capacity_to_serve,
        v:recruitmentEventAttendeeQualifications:yearsOfServiceCurrentPlacement::float as years_of_service_current_placement,
        v:recruitmentEventAttendeeQualifications:yearsOfServiceTotal::float            as years_of_service_total,
        v:referral::boolean                                                            as is_referral,
        v:referredBy::string                                                           as referred_by,
        v:notes::string                                                                as additional_notes,
        v:hispanicLatinoEthnicity::boolean                                             as has_hispanic_latino_ethnicity,
        -- descriptors
        {{ extract_descriptor('v:genderDescriptor::string') }}                          as gender,
        {{ extract_descriptor('v:sexDescriptor::string') }}                             as sex,
        {{ extract_descriptor('v:recruitmentEventAttendeeTypeDescriptor::string') }}    as recruitment_event_attendee_type,
        -- unflattened lists
        v:currentPosition:gradeLevels     as current_position_grade_levels,  -- is this the correct way of naming a list that is nested within a composit part?
        v:disabilities                    as v_disabilities,
        v:personalIdentificationDocuments as v_personal_identification_documents,
        v:races                           as v_races,
        v:telephones                      as v_telephones,
        v:touchpoints                     as v_touchpoints,
        -- references
        v:recruitmentEventReference as recruitment_event_reference,    
        -- edfi extensions
        v:_ext as v_ext

    from recruitment_event_attendances
)
select * from renamed