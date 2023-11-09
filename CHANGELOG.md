# Unreleased
## New features
## Under the hood
## Fixes

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
