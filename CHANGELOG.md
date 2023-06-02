# Unreleased 
## New features
## Under the hood
## Fixes

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
