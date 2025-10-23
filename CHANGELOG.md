# Unreleased
## New features
## Under the hood
## Fixes

# edu_edfi_source v0.6.0
## New features

- Add Ed-Fi Data Standard v5 base/stage models: `base_ef3__program_evaluations`, `base_ef3__student_program_evaluations`, `base_ef3__student_special_education_program_eligibility_associations`, `stg_ef3__program_evaluations`, `stg_ef3__student_program_evaluations`, `stg_ef3__student_special_education_program_eligibility_associations`
- Add columns to `base_ef3__student_special_education_program_associations` that are new in Ed-Fi Data Standard v5.2

## Under the hood
## Fixes

# edu_edfi_source v0.5.1
## New features
- Add base/stage models `base_ef3__student_school_food_service_program_association`, `stg_ef3__student_school_food_service_program_association`, `stg_ef3__stu_school_food_service__program_services`
- Add base/stage models `base_ef3__student_migrant_education_program_associations`, `stg_ef3__student_migrant_education_program_associations`, `stg_ef3__stu_migrant_edu__program_services`
- Add `stg_ef3__students__other_names`
## Under the hood
- Add `k_assessment` to `stg_ef3__student_assessments__score_results` and `stg_ef3__student_assessments__performance_levels` for use downstream
## Fixes
- Fix deduplication logic of `stg_ef3__course_transcripts` to remove duplicates from across ODS years, by replacing `k_course` (tied to api_year) with `course_code, course_ed_org_id` in the deduplication key

# edu_edfi_source v0.5.0
## New features
- Add Databricks platform compatibility
  - The only potentially breaking change: `base_ef3__discipline_incidents.v:incidentTime` datatype switched from time to string.
- Add new optional fields in `student_education_organization_associations` and `student_school_associations` from Ed-Fi Data Standard v5:
  - stuEdOrg: `gender_identity`, `supporter_military_connection`
  - stuSchAssoc: `is_school_choice`, `school_choice_basis`, `enrollment_type`, `next_year_school_id`, `next_year_grade_level`

# edu_edfi_source v0.4.10
## New features
- Add base/stage model for `StudentCTEProgramAssociation`
- Add stage model `stg_ef3__stu_cte__program_services` to flatten program services list for `StudentCTEProgramAssociation`
## Fixes
- Add handling of multiple academic subjects per course for DS 5.0 compatibility; new `stg_ef3__courses__academic_subjects` model and new `v_academic_subjects` columns in `base_ef3__courses` and `stg_ef3__courses` models.
- Correctly cast minimum and maximum credits for courses to float instead of int.


# edu_edfi_source v0.4.9
## Fixes
- Update unique key of staff section association for DS 5.0 compatibility

# edu_edfi_source v0.4.8
## Fixes
- Fix unique key of `k_objective_assessment` to account for recent update to include obj assess subject.

# edu_edfi_source v0.4.7
## New features
- Add base/stage models for `staffEducationOrganizationContactAssociations`
- Add (optional) support for incremental materialization of most expensive stg models
## Fixes
- Fix unique key of `k_assessment` in `stg_ef3__objective_assessments`
- Fix unique key `k_student_assessment` in `stg_ef3__student_assessments` to include `student_unique_id` (relevant only where `student_assessment_identifier` is not on its own unique)
- Fix surrogate key generation for References that include Descriptors that utilize EDU's 'replace descriptor' functionality

# edu_edfi_source v0.4.6
## Fixes
- Fix deduplication logic in all stg models to handle deleted records correctly before removal. This brings EDU more in sync with ODS true state
- Fix unique key in `stg_ef3__student_objective_assessments` to include assessment_identifier to handle edge case duplicates

# edu_edfi_source v0.4.5
## New features
- Add base/stage models for `restraintEvents`
- Add partial support for TPDM Core and TPDM Community

# edu_edfi_source v0.4.4
## Fixes
- Handle invalid timestamp formatting in student_assessments
- Fix coalesce logic for academic subjects in `stg_ef3__objective_assessments` and `stg_ef3__student_objective_assessments` to hydrate correctly when populated in respective Ed-Fi element's `academicSubject` field

# edu_edfi_source v0.4.3
## Fixes
- Add missing `program_enroll_end_date` to every stg-stu-program `__program_services` + `stu_spec_ed__disabilities`

# edu_edfi_source v0.4.2
## Fixes
- Fix surrogate key creation for `stg_ef3__grading_periods` to properly hanlde lowering of alphanumeric column (grading_period_name) that is part of natural key

# edu_edfi_source v0.4.1
## Fixes
- Fix surrogate key creation for `stg_ef3__student_contact_associations` to properly hanlde lowering of alphanumeric columns that are part of natural keys

# edu_edfi_source v0.4.0
## New features
- Add `stg_ef3__stu_ed_org__cohort_years` tracking student cohort designations (flattens Ed-Fi collection `cohort_years` for easier downstream use)
- Add base/stage models for `contacts` and `student_contact_associations`, added due to the rename from parent to contact in Ed-Fi data standard v5.0.
- Rename `k_parent` to `k_contact` in `stg_ef3__survey_responses`.
- Add `gender_identity`, `preferred_first_name`, `preferred_last_name` columns to `staffs` (Ed-Fi Data Standard v5.0 additions)
- Add `section_type` descriptor column to `sections` (Ed-Fi Data Standard v5.0 addition)
- Add `responsible_teacher_staff_reference`, `v_programs`, `v_sections` columns to `course_transcripts` (Ed-Fi Data Standard v5.0 additions)
- Add `v_programs` column to `course_transcripts` (Ed-Fi Data Standard v5.0 addition)
## Under the hood
- Add columns to `base_ef3__parents` to allow data to be unioned into new `stg_ef3__contacts` model
- Update package dependency `dbt_utils` to 1.3.0, including alignment to renamed `generate_surrogate_key()` macro. Note, this change now treats nulls and empty strings as distinct values in surrogate key generation.
- Make package macro calls to `extract_descriptor` explicit so `flatten_arrays` can be used by outer packages
## Fixes
- Fix typo in column name `courses.maxCompletionsForCredit`
- Fix surrogate key creation for `stg_ef3__student_academic_records`, `stg_ef3__student_objective_assessments`, and `stg_ef3__students` to properly handle lowering of alphanumeric columns that are part of natural keys
- Rename `stg_ef3__staff__races` to `stg_ef3__staffs__races` for consistency with EDU naming conventions

# edu_edfi_source v0.3.6
## Fixes
- Fix data type on base_ef3__student_special_education_program_associations.student_unique_id

# edu_edfi_source v0.3.5
## Fixes
- Remove records deleted from Ed-Fi in `student_education_organization_associations` and `student_parent_associations`
## Under the hood
- Add `v_ext` handling in `discipline` and `student_academic_record` flattened models
- Add missing references/surrogate key generation for ed_orgs in `graduation_plans` and `courses`

# edu_edfi_source v0.3.4
## New features
- Add base/stage models for student_education_organization_responsibility_associations
- Add column `last_modified_timestamp` to every base table (via `source_edfi3` macro). This includes timestamps of Ed-Fi deletes, which is helpful for tracking down when deletes occurred.
## Under the hood
## Fixes
- Improve performance of `stg_ef3__student_assessments` by fixing a join in `int_ef3__student_assessments__identify_subject`. Also fixes edge case bug (only applicable where assessments were loaded incorrectly).

# edu_edfi_source v0.3.3
## New features
## Under the hood
- Add explicit namespacing for macro call: `edu_edfi_source.extract_descriptor()` within `gen_skey()` so `gen_skey()` can be used outside this package
## Fixes
- Force staff_unique_id and student_unique_id to lower in construction of `k_staff` and `k_student`. This is needed for keys to match foreign keys generated using `gen_skey()` macro.

# edu_edfi_source v0.3.2
## New features
- Add `base_ef3__staff_education_organization_employment_associations`
- Add `stg_ef3__staff_education_organization_employment_associations`
- Add `stg
# edu_edfi_source v0.3.2
## New features
- Add `base_ef3__staff_education_organization_employment_associations`
- Add `stg_ef3__staff_education_organization_employment_associations`
- Add `stg_ef3__staff__races`

# edu_edfi_source v0.3.1
## Fixes
- Fix to `stg_ef3__student_assessments`: remove deleted records


# edu_edfi_source v0.3.0
## New features
- Bring SEA and ESC in line with LEA by using abbreviated names and keys
## Fixes
- Fix spelling of `stg_ef3__stu_ed_org__disabilities.disability_designation`
- Fix unique key of `stg_ef3__student_school_attendance_events` and `stg_ef3__student_section_attendance_events` to correctly align with Ed-Fi key. This creates new records that are handled in edu_wh v0.3.0

# edu_edfi_source v0.2.15
## Under the hood
- add k_student_xyear to student assessment models
## Fixes
- Fix deduplication ordering in student academic records

# edu_edfi_source v0.2.14
## New features
- Add models for SEAs
## Under the hood
- Tag cohort models for easy disabling when not in use
## Fixes
- Fix typo on attendance durations

# edu_edfi_source v0.2.13
## New features
- Add base & stage models for learning standards, and stg_ef3__grades__learning_standards for student-learning standard-grades

# edu_edfi_source v0.2.12
## Fixes
- Fix a rare edge case in the grain of the discipline model against multi-year ODSes

# edu_edfi_source v0.2.11
## New features
- Add studentAcademicRecord diplomas model

# edu_edfi_source v0.2.10
## Under the hood
- On Assessment records, use timestamps without timezones for better compatibility

# edu_edfi_source v0.2.9
## Fixes
- Fix discipline-behavior grain
- Add missing model config for disabilities

# edu_edfi_source v0.2.8
## Fixes
- Handle single quotes in Descriptors when swapping from codeValue to shortDescription

# edu_edfi_source v0.2.7
## New features
- Allow for swapping descriptor codeValues for shortDescriptions or Descriptions
## Fixes
- Fix attribution of Objective Assessments to the correct Academic Subject

# edu_edfi_source v0.2.6
## New features
- Add models for the main components of the Survey domain
- Add model for disabilities on studentSpecialEducationProgramAssociation
## Under the hood
- Change the way foreign key generation works: optional references that are Null
    will now produce a Null key rather than a valid hash that doesn't join to anything

# edu_edfi_source v0.2.5
## Fixes
- Bugfix release: address inconsistent casing in deletes IDs in Ed-Fi deployments

# edu_edfi_source v0.2.4
## Fixes
- Bugfix release: correct typo

# edu_edfi_source v0.2.3
## Fixes
- Bugfix release: correct typo
- Add to default non-offender codes

# edu_edfi_source v0.2.2
## New features
- Add models for education service centers
- Add additional discipline/behavior models

## Under the hood
- Update `extract_extension` macro to handle multiple models
- Add school_year to staff ed org assignments

# edu_edfi_source v0.2.1
## Under the hood
- Standardize naming conventions across program models
- Add minimum dbt version requirement

# edu_edfi_source v0.2.0
## New features
- Add optional domain disabling to all non-core models, using vars in dbt_project.yml.
## Under the hood
- Add a single model properties file under each subdirectory, as per DBT recommendation.
## Fixes
- Remove unnecessary deduplication keys for program association staging models


# edu_edfi_source v0.1.3
## New features
- Add simplified `full_address` field to models containing addresses
## Under the hood
- Simplify `do_not_publish_indicator` to `do_not_publish`
## Fixes
- Fix incorrect indentation on cohort test


# edu_edfi_source v0.1.2
## New features
- Add models for studentLanguageInstructionProgramAssociations, studentHomelessProgramAssociations, and studentTitleIPartAProgramAssociations
- Add model for Special Education Program Services
- Add models for cohorts
- Add model for graduation plans
## Fixes
- Remove unnecessary deduplication keys for program association staging models


# edu_edfi_source v0.1.1
## New features
- Add models for descriptors
- Add model for student languages
- Add model for student telephone numbers
- Add model for Parents
- Add model for studentParentAssociations

## Under the hood
- Factor out handling of common sub-objects like addresses and phone numbers to macros

## Fixes

# edu_edfi_source v0.1.0
Initial release
