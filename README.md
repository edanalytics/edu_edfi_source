## edu_edfi_source

This package parses data from the [Ed-Fi data standard](https://www.ed-fi.org/)
into standard tabular data models.

Its goals are:
  - To be as unopinionated as possible in preparing Ed-Fi data for analysis
  - To enforce Ed-Fi's rules on data uniqueness and delete processing
  - To make Ed-Fi data easier to use and consume, such as by adding simple surrogate keys to represent Ed-Fi's multi-column natural keys

This package can be used on its own, but is part of the larger [EDU](https://enabledataunion.org)
analytics framework, and we assume the metadata structure produced by EDU's 
data loading process.

## Installation

dbt version required: `>=1.0.0, <2.0.0`

Include the following in your `packages.yml` file:

```
packages:
  - package: edanalytics/edu_edfi_source
    version: [">=0.4.0", "<0.5.0"]
```

Note: if you're using the downstream [warehouse package](https://github.com/edanalytics/edu_wh), it already includes this source package, and you don't need to install it again. 

## License
This package is free to use for noncommercial purposes. 
See [License](LICENSE.md).

## Configuration
### Sources
Importing projects must add their own source definitions. We don't include them
here because Ed-Fi can be extended and new resources are added over time, 
so each project should generate their sources from their own Ed-Fi API.

We have code to generate source definitions from an Ed-Fi API's swagger definition
[here](https://github.com/edanalytics/edu_project_template/tree/main/codegen).

This will generate a `src_edfi_3.yml` file containing the location and definitions
of all raw raw resources coming from Ed-Fi APIs which must be included in your project.
Any resources not in use can be disabled here to exclude them from dbt's graph.

### Ed-Fi Extensions

Resources that have custom columns added via extensions can be added into any model. 
Ed-Fi extensions tend to come in two flavors: new columns on existing resources, 
or new resources. This section deals with new columns, while new/custom resources 
can be handled by adding base/stage models in the importing project.

Extension columns are stored in the API payloads under an `_ext` element in the
JSON, which contains a dictionary of named extensions, each containing 
their extended columns.

To add these to your tables, you need a configuration block in `dbt_project.yml` 
specifying the name of the extended resource, the desired warehouse column name, 
a SQL snippet necessary to extract the element, and the data type you want it to have.

```
# general structure
extensions:
  {stg_table_name}:
    {desired_column_name}:
      name: '{extension_name}:{element_name}'
      dtype: '{SQL data type}'
```

```
# example:
  extensions:
    stg_ef3__student_special_education_program_associations:
      bps_iep_exit_date:
        name: 'myBPS:iepExitDate'
        dtype: 'timestamp'
```

## Project Layout

### Base models
Base models are views on raw Ed-Fi data, and show the data as it was loaded 
incrementally. This means uniqueness and deletes are not enforced, so base tables
contain the full incremental change history since the sync began.

Base models enforce the following rules:
  - Table names are prefixed `base_ef3__`, and follow the API resource names, converted to snake case
  - Base tables only source data from raw tables
  - Base tables represent only one raw object, and do not perform joins
  - Metadata from the loading process (such as timestamps, tenant codes, and data lake file names) are preserved
  - Ed-Fi deletion records are joined in to produce an `is_deleted` flag.
  - Data type coercions are enforced
  - Column names are converted to snake case
  - Descriptors are extracted to simple strings (rather than URIs)
  - Nested lists are passed forward untouched
  - Reference objects are preserved for key generation
  - Columns are re-ordered semantically rather than alphabetically

Base models are a clean, tabular look into the data as retrieved from Ed-Fi 
incrementally. They should not be used directly, but are useful for understanding
how records changed over time, or how the data loading process operates.

### Stage models
Stage models are (by default) materialized tables enforcing Ed-Fi's uniqueness constraints, removing records marked as deleted, and unpacking nested lists. 
The goal is to present Ed-Fi data in a flat tabular format for further analytics.

Stage models enforce the rules:
  - Table names are prefixed `stg_ef3__`, and tables with nested lists are unpacked 
  into sub-tables with the format `resource_name__list_name`
  - Stage tables generally have a one-to-one relationship with base tables, except in the case of unpacked lists.
  - Surrogate keys are generated to create links between objects
  - Some tables are 'annualized', to preserve year-over-year changes
    - e.g. `courses` are not annualized in Ed-Fi (`school_year` is not part of the unique key), but we preserve a separate record for each school year so that course characteristics can change over time.
  - Deleted records are removed
  - Uniqueness constraints are enforced, and only the most recent version of any
  element is preserved.
  - Ed-Fi extensions are unpacked into the table



Stage models are a true representation of Ed-Fi data as of the last sync, ready
for further use directly.

### Key Generation

Ed-Fi uses multiple natural keys for joins, and our goal is to simplify this
down to a single column key with consistent naming.

Surrogate keys follow these conventions:
  - All surrogate keys are prefixed with `k_`
  - The key name should align to a resource specified in the singular, such as `k_school` or `k_student`
  - If a table must contain multiple foreign keys to the same object, they are disambiguated
  with double underscores, such as `k_school__responsibility` and `k_school__assignment`
  - Keys (or other columns contributing to table uniqueness) should be the leftmost
  columns in a table, generally from least to most distinct values.
  - Keys are `md5` hashes of the underlying natural keys concatanated together


### Macros

#### `gen_skey`
`gen_skey` converts Ed-Fi reference objects to surrogate keys. There are several
complexities inherent in doing this, which the macro helps to make consistent.
dbt surrogate keys are `md5` hashes of the concatenated natural keys.

  - We add `tenant_code` to all natural keys to facilitate multi-tenant 
  environments, such as district collaboratives or states.
  - In some cases we add the API year to annualize the resource, allowing for 
  example each school year to have its own definition of courses.
  - Because some Ed-Fi implementations use Case Insensitive Collations (allowing 
  any casing of a value to be considered identical) we `lower` all natural keys
  to force them to be byte-identical.
  - In some cases Ed-Fi is inconsistent in whether dates are treated as `date`
  or `timestamp` in different places, so we coerce date fields to be strictly 
  `date` to drop the time component.
  - When natural keys contain descriptors, we drop the namespace/URI components
  and use only the descriptor code itself.

Also note that reference objects are not included in the base object: while 
`studentSchoolAssociations` contains a `schoolReference`, `schools` itself does not.
This means the key must be generated separately in defining objects from referencing
objects. To make things simpler, we always include `tenant_code` and (when annualized)
`api_year` first, and then remaining natural keys in alphabetical order.

#### `extract_extension`
`extract_extension` pulls Ed-Fi extension objects out into stand-alone columns. 
See the Configuration section for usage.

#### `extract_descriptor`
`extract_descriptor` pulls the `descriptor_code` from a descriptor URI.


#### `source_edfi3`
`source_edfi3` replaces the standard `source` macro, joining in the `_deletes`
raw table to create the `is_deleted` flag.

#### `edorg_ref`
`edorg_ref` breaks apart `educationOrganizationReference` objects into a pair
of key columns `k_lea` and `k_school`, disambiguating such references.

## Dependencies

This package depends on `dbt_utils`. If your project also makes use of `dbt_utils`,
we recommend you remove it from your root `packages.yml` to avoid package
version conflicts.

## Package Maintenance

The Education Analytics team only maintains the latest version of the package.
We recommend that you stay consistent with the [latest version](https://github.com/edanalytics/edu_edfi_source/releases/latest) 
of the package and refer to the [CHANGELOG](https://github.com/edanalytics/edu_edfi_source/blob/main/CHANGELOG.md)
and release notes for more information on changes across versions.

## Platform Compatibility
Currently only Snowflake is supported.

We are working on adding the scaffolding for multi-platform support, and once 
this is in place would welcome contributions.

[Contact us](mailto:edu@edanalytics.org) if you're interested in support in another
platform or contributing to this effort.
