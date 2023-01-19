# Unreleased
## New features
- Add models for studentLanguageInstructionProgramAssociations, studentHomelessProgramAssociations, and studentTitleIPartAProgramAssociations
- Add optional domain disabling to all non-core models, using vars in dbt_project.yml.

## Under the hood
- Add a single model properties file under each subdirectory, as per DBT recommendation.

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