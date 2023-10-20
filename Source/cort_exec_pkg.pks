CREATE OR REPLACE PACKAGE cort_exec_pkg
AUTHID CURRENT_USER
AS

/*
CORT - Oracle database DevOps tool

Copyright (C) 2013-2023  Rustam Kafarov

www.cort.tech
master@cort.tech

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
  Description: Main package executing table recreation and rollback with current user privileges
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.01   | Rustam Kafarov    | Added support for sequences, indexes, builds, create table as select. Orevall improvements
  15.00   | Rustam Kafarov    | Added data copy/move by partitions
  17.00   | Rustam Kafarov    | Removed some procedures from public declarations
  19.00   | Rustam Kafarov    | Revised parameters
  20.00   | Rustam Kafarov    | Added support of long names introduced in Oracle 12.2 
  21.00   | Rustam Kafarov    | Split params into change and run params 
  ----------------------------------------------------------------------------------------------------------------------
*/


  -- table level params

  TYPE gt_rename_rec IS RECORD(
    object_type                  arrays.gt_name,  -- object type
    object_name                  arrays.gt_name,  -- original object name
    object_owner                 arrays.gt_name,  -- object owner
    generated                    CHAR(1),         -- Specify is name generated (Y) or not (N)
    current_name                 arrays.gt_name,  -- current name
    cort_name                    arrays.gt_name,  -- cort name
    temp_name                    arrays.gt_name,  -- temp name
    rename_name                  arrays.gt_name,  -- renamed name (used by RENAME FOR REPLACE)
    parent_object_name           arrays.gt_name,  -- used by constraints, LOBs, VARRAYs
    lob_column_name              arrays.gt_name   -- used by LOBs, VARRAYs
  );

  TYPE gt_storage_rec IS RECORD(
    initial_extent               VARCHAR2(40), -- Size of the initial extent (in bytes); NULL for partitioned tables
--    next_extent                  VARCHAR2(40), -- Size of secondary extents (in bytes); NULL for partitioned tables  /* NOT USED IN LOCAL TBS */
    min_extents                  VARCHAR2(40), -- Minimum number of extents allowed in the segment; NULL for partitioned tables
--    max_extents                  VARCHAR2(40), -- Maximum number of extents allowed in the segment; NULL for partitioned tables. /* NOT USED IN LOCAL TBS */
    max_size                     VARCHAR2(40), -- Maximum number of blocks allowed in the segment of the LOB data partition, or DEFAULT
    pct_increase                 VARCHAR2(40), -- Percentage increase in extent size; NULL for partitioned tables
    freelists                    VARCHAR2(40), -- Number of process freelists allocated to the segment; NULL for partitioned tables
    freelist_groups              VARCHAR2(40), -- Number of freelist groups allocated to the segment; NULL for partitioned tables
--    optimal_size  for rollback segments only (NOT USED)
    buffer_pool                  VARCHAR2(7)   -- Default buffer pool for the table; NULL for partitioned tables:
                                               --   DEFAULT, KEEP, RECYCLE, NULL
--    encrypt   for tablespaces only (NOT USED)
  );

  TYPE gt_physical_attr_rec IS RECORD(
    pct_free                     PLS_INTEGER, -- Minimum percentage of free space in a block; NULL for partitioned tables
    pct_used                     PLS_INTEGER, -- Minimum percentage of used space in a block; NULL for partitioned tables
    ini_trans                    PLS_INTEGER, -- Initial number of transactions; NULL for partitioned tables
    max_trans                    PLS_INTEGER, -- Maximum number of transactions; NULL for partitioned tables
    storage                      gt_storage_rec
  );


  TYPE gt_compression_rec IS RECORD(
    compression                  VARCHAR2(8),  -- Indicates whether table compression is enabled (ENABLED) or not (DISABLED); NULL for partitioned tables
    compress_for                 VARCHAR2(18)  -- Default compression for what kind of operations:
                                               --   11.1: DIRECT LOAD ONLY, FOR ALL OPERATIONS, NULL
                                               --   11.2: BASIC, OLTP, QUERY LOW, QUERY HIGH, ARCHIVE LOW, ARCHIVE HIGH, NULL
                                               --   10.1 and 10.2: not used

  );

  TYPE gt_parallel_rec IS RECORD(
    degree                       VARCHAR2(10), -- Number of threads per instance for scanning the table, or DEFAULT
    instances                    VARCHAR2(10)  -- Number of instances across which the table is to be scanned, or DEFAULT
  );



  TYPE gt_constraint_rec IS RECORD(
    owner                        arrays.gt_name,     -- Owner of the constraint definition
    constraint_name              arrays.gt_name,     -- Name of the constraint definition
    constraint_type              VARCHAR2(1),        -- Type of the constraint definition:
                                                     --    C - Check constraint on a table
                                                     --    P - Primary key
                                                     --    U - Unique key
                                                     --    R - Referential integrity
                                                     --    V - With check option, on a view
                                                     --    O - With read only, on a view
                                                     --    H - Hash expression
                                                     --    F - Constraint that involves a REF column
                                                     --    S - Supplemental logging
    table_name                   arrays.gt_name,     -- Name associated with the table with the constraint definition
    search_condition             VARCHAR2(32767),    -- Text of search condition for a check constraint
    column_arr                   arrays.gt_name_arr, -- Column names ordered by position
    r_owner                      arrays.gt_name,     -- Owner of the table referred to in a referential constraint
    r_constraint_name            arrays.gt_name,     -- Name of the unique constraint definition for the referenced table
    r_table_name                 arrays.gt_name,     -- Referencing table name
    r_column_arr                 arrays.gt_name_arr, -- Referencing table key column names ordered by position
    delete_rule                  VARCHAR2(9),        -- Delete rule for a referential constraint:
                                                     --    CASCADE
                                                     --    SET NULL
                                                     --    NO ACTION
    status                       VARCHAR2(8),        -- Enforcement status of the constraint:
                                                     --    ENABLED
                                                     --    DISABLED
    deferrable                   VARCHAR2(14),       -- Indicates whether the constraint is deferrable (DEFERRABLE) or not (NOT DEFERRABLE)
    deferred                     VARCHAR2(9),        -- Indicates whether the constraint was initially deferred (DEFERRED) or not (IMMEDIATE)
    validated                    VARCHAR2(13),       -- Indicates whether all data obeys the constraint (VALIDATED) or not (NOT VALIDATED)
    generated                    VARCHAR2(14),       -- Indicates whether the name of the constraint is user-generated (USER NAME) or system-generated (GENERATED NAME)
    bad                          VARCHAR2(3),        -- Indicates whether this constraint specifies a century in an ambiguous manner (BAD) or not (NULL).
                                                     -- To avoid errors resulting from this ambiguity, rewrite the constraint using the TO_DATE function with a four-digit year.
    rely                         VARCHAR2(4),        -- Indicates whether an enabled constraint is enforced (RELY) or unenforced (NULL)
    index_owner                  arrays.gt_name,     -- Name of the user owning the index
    index_name                   arrays.gt_name,     -- Name of the index (only shown for unique and primary-key constraints)
    rename_rec                   gt_rename_rec,      -- record holding names for renaming
    has_references               BOOLEAN,            -- Has references or not. Valid only for PK/UK
    drop_flag                    BOOLEAN             -- Indicates that DROP claue has been already generated for this constraint or constraint has been dropped implicitly
  );

  TYPE gt_constraint_arr IS TABLE OF gt_constraint_rec INDEX BY PLS_INTEGER;


  TYPE gt_log_group_rec IS RECORD(
    owner                        arrays.gt_name,     --   Owner of the log group definition
    log_group_name               arrays.gt_name,     --   Name of the log group definition
    table_name                   arrays.gt_name,     --   Name of the table on which the log group is defined
    log_group_type               VARCHAR2(19),       --   Type of the log group:
                                                     --      PRIMARY KEY LOGGING
                                                     --      UNIQUE KEY LOGGING
                                                     --      FOREIGN KEY LOGGING
                                                     --      ALL COLUMN LOGGING
                                                     --      USER LOG GROUP
    always                       VARCHAR2(11),       -- Y indicates the log group is logged any time a row is updated; N indicates the log group is logged any time a member column is updated
    generated                    VARCHAR2(14),       -- Indicates whether the name of the supplemental log group was system generated (GENERATED NAME) or not (USER NAME)
    column_arr                   arrays.gt_lstr_arr, -- Column names ordered by position
    column_log_arr               arrays.gt_name_arr, -- Column logging property ordered by position
    rename_rec                   gt_rename_rec,      -- record holding names for renaming
    drop_flag                    BOOLEAN             -- Indicates that DROP claue has been already generated for this constraint
  );

  TYPE gt_log_group_arr   IS TABLE OF gt_log_group_rec  INDEX BY PLS_INTEGER;

  TYPE gt_cluster_columns_rec IS RECORD(
    cluster_name                 arrays.gt_name,
    clu_column_name              arrays.gt_name,
    table_name                   arrays.gt_name,
    tab_column_name              arrays.gt_name
  );
  TYPE gt_cluster_columns_arr IS TABLE OF gt_cluster_columns_rec INDEX BY PLS_INTEGER;

  TYPE gt_lob_rec IS RECORD (
    owner                        arrays.gt_name, -- Owner of the object containing the LOB
    table_name                   arrays.gt_name, -- Name of the object containing the LOB
    column_name                  VARCHAR2(4000), -- Name of the LOB column or attribute
    column_indx                  PLS_INTEGER,    -- Column index in column_arr
    lob_name                     arrays.gt_name, -- Name of the LOB segment
    lob_index_name               arrays.gt_name, -- Name of the LOB index
    partition_name               arrays.gt_name, -- Name of the table partition/subpartition
    partition_level              VARCHAR2(12),   -- PARTITION/SUBPARTITION
    partition_position           NUMBER,         -- Position of the LOB data partition/subpartition within the LOB item
    parent_lob_part_name         arrays.gt_name, -- LOB_PARTITION_NAME for lob subpartition
    lob_partition_name           arrays.gt_name, -- Name of the LOB data partition/subpartition
    lob_indpart_name             arrays.gt_name, -- Name of the corresponding LOB index partition/subpartition
    tablespace_name              arrays.gt_name, -- Name of the tablespace containing the LOB segment
    chunk                        NUMBER,         -- Size (in bytes) of the LOB chunk as a unit of allocation or manipulation
    pctversion                   VARCHAR2(20),   -- Maximum percentage of the LOB space used for versioning
    retention                    VARCHAR2(20),   -- Type of retention used for this LOB. Possible values for SecureFiles: NONE, AUTO, MIN, MAX, DEFAULT, INVALID
                                                 -- Possible values for BasicFiles: YES, NO
    min_retention                NUMBER,         -- Minimum retention time (in seconds). This column is only meaningful for SecureFiles with RETENTION_TYPE set to MIN.
    freepools                    NUMBER,         -- Number of freepools for this LOB segment
    cache                        VARCHAR2(10),   -- Indicates whether and how the LOB data is to be cached in the buffer cache:
                                                 --   YES - LOB data is placed in the buffer cache
                                                 --   NO - LOB data either is not brought into the buffer cache or is brought into the buffer cache and placed at the least recently used end of the LRU list
                                                 --   CACHEREADS - LOB data is brought into the buffer cache only during read operations but not during write operations
    logging                      VARCHAR2(7),    -- Indicates whether or not changes to the LOB are logged:
                                                 --   NONE, YES, NO
    encrypt                      VARCHAR2(4),    -- Indicates whether or not the LOB is encrypted:
                                                 --   YES,  NO,  NONE - Not applicable to BasicFile LOBs
    compression                  VARCHAR2(6),    -- Level of compression used for this LOB:
                                                 --   MEDIUM,  HIGH,  NO,  NONE - Not applicable to BasicFile LOBs
    deduplication                VARCHAR2(15),   -- Kind of deduplication used for this LOB:
                                                 --   LOB - Deduplicate, NO - Keep duplicates, NONE - Not applicable to BasicFile LOBs
    in_row                       VARCHAR2(3),    -- Indicates whether some of the LOBs are stored inline with the base row (YES) or not (NO). For partitioned objects, refer to the *_LOB_PARTITIONS and *_PART_LOBS views.
    partitioned                  VARCHAR2(3),    -- Indicates whether the LOB column is in a partitioned table (YES) or not (NO)
    --11.1
    securefile                   VARCHAR2(3),    -- Indicates whether the LOB is a SecureFile LOB (YES) or not (NO)
    segmnent_created             VARCHAR2(3),    -- Indicates whether the LOB segment has been created (YES) or not (NO)
    -- 11.2 onnly
    flash_cache                  VARCHAR2(7),    -- Database Smart Flash Cache hint to be used for partition blocks:
                                                 --    DEFAULT, KEEP, NONE
    cell_flash_cache             VARCHAR2(7),    -- Cell flash cache hint to be used for partition blocks:
                                                 --    DEFAULT, KEEP, NONE
    --12.1
    retention_type               VARCHAR2(7),    -- Type of retention used for this LOB. Possible values for SecureFiles:  NONE, AUTO, MIN, MAX, DEFAULT, INVALID
                                                 -- Possible values for BasicFiles: YES, NO
    retention_value              NUMBER,         -- Minimum retention time (in seconds). This column is only meaningful for SecureFiles with RETENTION_TYPE set to MIN.
    storage                      gt_storage_rec,
    xml_column_indx              PLS_INTEGER,    -- index of related XMLTYPE column. Normally equals to column_indx-1
    varray_column_indx           PLS_INTEGER,    -- index of related VARRAY column. Normally equals to column_indx
    rename_rec                   gt_rename_rec,  -- record holding names for renaming
    tablespace_specified         BOOLEAN
  );

  TYPE gt_lob_arr IS TABLE OF gt_lob_rec INDEX BY PLS_INTEGER;
  
  -- ADT attributes
  TYPE gt_type_attr_rec   IS RECORD(
    owner                        arrays.gt_name,         -- Owner of the type
    type_name                    arrays.gt_name,         -- Name of the type
    attr_name                    arrays.gt_name,         -- Name of the attribute
    attr_type_mod                VARCHAR2(7),            -- Type modifier of the attribute: ref, pointer
    attr_type_owner              arrays.gt_name,         -- Owner of the type of the attribute
    attr_type_name               arrays.gt_name,         -- Name of the type of the attribute
    length                       NUMBER,                 -- Length of the char attribute, or maximum length of the varchar or varchar2 attribute.
    precision                    NUMBER,                 -- Decimal precision of the number or decimal attribute, or binary precision of the float attribute.
    scale                        NUMBER,                 -- Scale of the number or decimal attribute
    character_set_name           VARCHAR2(44),           -- Character set name of the attribute (char_cs or nchar_cs)
    attr_no                      NUMBER,                 -- Syntactical order number or position of the attribute as specified in the type specification or create type statement (not to be used as an id number)
    inherited                    VARCHAR2(3),            -- Indicates whether the attribute is inherited from a supertype (YES) or not (NO)
    char_used                    VARCHAR2(4),            -- Indicates whether the attribute uses byte length semantics (BYTE) or char length semantics (CHAR). for nchar and nvarchar2 attribute types, this value is always CHAR.
    defined_type_name            arrays.gt_name,         -- Type name where attribute is defined  
    iot_primary_key              BOOLEAN,                -- indicates whether attribute is inclided into IOT primary key
    partition_key                BOOLEAN,                -- indicates whether attribute is inclided into partition/subpartition key columns
    cluster_key                  BOOLEAN,                -- indicates whether attribute is part of any cluster
    func_index_key               BOOLEAN,                -- indicates whether attribute is part of any function_based index
    domain_index_key             BOOLEAN                 -- indicates whether attribute is part of any domain index
  );
  TYPE gt_type_attr_arr IS TABLE OF gt_type_attr_rec INDEX BY PLS_INTEGER;

  -- Abstract Data Type
  TYPE gt_adt_rec IS RECORD(
    owner                        arrays.gt_name,         -- Type owner 
    type_name                    arrays.gt_name,         -- Type name (actual name)
    synonym_name                 arrays.gt_name,         -- Synonym for type (user name)
    typeid                       arrays.gt_name,         -- TypeID of the type (unique within same root)
    supertype_owner              arrays.gt_name,         -- Owner of the supertype (NULL if type is not a subtype)
    supertype_name               arrays.gt_name,         -- Name of the supertype (NULL if type is not a subtype)
    type_path                    VARCHAR2(32767),        -- / delimetered tree path 
    attribute_arr                gt_type_attr_arr,       -- Array of attributes in the type including attributes for all subtypes
    attribute_ind_arr            arrays.gt_int_indx,     -- Index of attributes indexed by "ATTR_NAME"
    attributes                   PLS_INTEGER,            -- total number of attributes
    local_attributes             PLS_INTEGER,            -- number of local (not inherited) attributes
    local_attr_start_pos         PLS_INTEGER,            -- start position of local attributes in attribute_arr
    typeid_name                  VARCHAR2(32767),        -- part of decode expression canverting typeid into type_name
    subtype_adt_arr              arrays.gt_name_arr      -- list of all subtypes
  );

  TYPE gt_adt_indx IS TABLE OF gt_adt_rec INDEX BY VARCHAR2(300); -- index by "type_owner"."type_name"
  
  
  TYPE gt_column_rec IS RECORD(
    owner                        arrays.gt_name,        -- Owner of the table, view, or cluster
    table_name                   arrays.gt_name,        -- Name of the table, view, or cluster
    column_name                  arrays.gt_name,        -- Column name
    column_indx                  PLS_INTEGER,           -- Index in column_arr array
    data_type                    VARCHAR2(128),         -- Datatype of the column
    data_type_mod                VARCHAR2(3),           -- Datatype modifier of the column
    data_type_owner              arrays.gt_name,        -- Owner of the datatype of the column
    data_length                  PLS_INTEGER,           -- Length of the column (in bytes)
    data_precision               PLS_INTEGER,           -- Decimal precision for NUMBER datatype; binary precision for FLOAT datatype; NULL for all other datatypes
    data_scale                   PLS_INTEGER,           -- Digits to the right of the decimal point in a number
    nullable                     VARCHAR2(1),           -- Indicates whether a column allows NULLs. The value is N if there is a NOT NULL constraint on the column or if the column is part of a PRIMARY KEY
    notnull_constraint_name      arrays.gt_name,          -- Name of NOT NULL constraint (populated only if name is USER GENERATED)
    column_id                    PLS_INTEGER,           -- Sequence number of the column as created
    data_default                 VARCHAR2(32767),       -- Default value for the column
    character_set_name           VARCHAR2(44),          -- Name of the character set: CHAR_CS, NCHAR_CS
    char_col_decl_length         PLS_INTEGER,           -- Declaration length of the character type column
    char_length                  PLS_INTEGER,           -- Displays the length of the column in characters. This value only applies to the following datatypes:
                                                        --  CHAR  VARCHAR2  NCHAR  NVARCHAR
    char_used                    VARCHAR2(1),           -- Indicates that the column uses BYTE length semantics (B) or CHAR length semantics (C), or whether the datatype is not any of the following (NULL):
                                                        --  CHAR  VARCHAR2  NCHAR  NVARCHAR
    hidden_column                VARCHAR2(3),           -- Indicates whether the column is a hidden column (YES) or not (NO)
    virtual_column               VARCHAR2(3),           -- Indicates whether the column is a virtual column (YES) or not (NO)
    segment_column_id            PLS_INTEGER,           -- Sequence number of the column in the segment
    internal_column_id           PLS_INTEGER,           -- Internal sequence number of the column
    qualified_col_name           VARCHAR2(4000),        -- Qualified column name
    substitutable                VARCHAR2(1),           -- substitutable flag (Y/N). Only for ADT columns.
    col_comment                  VARCHAR2(4000),        -- column comment
    -- 12.1
    user_generated               VARCHAR2(3),           -- Indicates whether the column is a user-generated column (YES) or a system-generated column (NO). Invisible columns are hidden columns that are also user- generated columns.
    default_on_null              VARCHAR2(3),           -- Indicates whether the column has DEFAULT ON NULL semantics (YES) or not (NO)
    identity_column              VARCHAR2(3),           -- Indicates whether this is an identity column (YES) or not (NO)
    evaluation_edition           VARCHAR2(128),         -- Name of the edition in which editioned objects referenced in an expression column are resolved
    unusable_before              VARCHAR2(128),         -- Name of the oldest edition in which the column is usable
    unusable_beginning           VARCHAR2(128),         -- Name of the oldest edition in which the column becomes perpetually unusable
    --12.2
    collation                    VARCHAR2(100),         -- Collation for the column. Only applies to columns with character data types.
    collated_column_id           NUMBER,                -- Internal sequence number of a column, for which this virtual column generates a collation key.
    --
    encryption_alg               VARCHAR2(29),          -- Encryption algorithm used to protect secrecy of data in this column:
                                                        --   3 Key Triple DES 168 bits key, AES 128 bits key, AES 192 bits key, AES 256 bits key
    salt                         VARCHAR2(3),           -- Indicates whether the column is encrypted with SALT (YES) or not (NO)
    new_column_name              arrays.gt_name,        -- new column name in case of renaming
    temp_column_name             arrays.gt_name,        -- temp column name for 2-way renaming process
    sql_start_position           PLS_INTEGER,           -- Start position of definition
    sql_end_position             PLS_INTEGER,           -- End position of definition
    sql_next_start_position      PLS_INTEGER,           -- Start position of definition of the next column
    matched_column_id            PLS_INTEGER,           -- column_id of matched column from another table structure
    iot_primary_key              BOOLEAN,               -- indicates whether attribute is inclided into IOT primary key
    partition_key                BOOLEAN,               -- indicates whether attribute is inclided into partition/subpartition key columns
    cluster_key                  BOOLEAN,               -- indicates whether attribute is part of any cluster
    unused_flag                  BOOLEAN,               -- indicates whether column is unused
    unused_segment_shift         NUMBER,                -- inidictaes of number preceding segment unused columns
    invisible_segment_shift      NUMBER,                -- inidictaes of number preceding segment invisible columns
    original_data_type           arrays.gt_name,        -- name of object type (in case synonym is used)
    adt_flag                     BOOLEAN                -- Indicates if the column of user-deined ADT type
  );


  TYPE gt_column_arr IS TABLE OF gt_column_rec INDEX BY PLS_INTEGER;


  TYPE gt_lob_template_rec IS RECORD (
    subpartition_name            VARCHAR2(34),   -- Name of the subpartition
    lob_column_name              arrays.gt_name, -- Name of the LOB column
    lob_segment_name             arrays.gt_name, -- Name of the LOB segment
    tablespace_name              arrays.gt_name  -- Tablespace name
  );

  TYPE gt_lob_template_arr IS TABLE OF gt_lob_template_rec INDEX BY PLS_INTEGER;


  TYPE gt_xml_col_rec IS RECORD(
    owner                        arrays.gt_name,  -- Owner of the XML table
    table_name                   arrays.gt_name,  -- Name of the XML table
    column_name                  VARCHAR2(4000),  -- Name of the XML table column
    column_indx                  PLS_INTEGER,     -- Column index in column_arr
    lob_column_indx              PLS_INTEGER,     -- Lob column index in column_arr
    xmlschema                    VARCHAR2(700),   -- Name of the XML Schema that is used for the table definition
    schema_owner                 arrays.gt_name,  -- Owner of the XML Schema that is used for the table definition
    element_name                 VARCHAR2(2000),  -- Name of the XML SChema element that is used for the table
    storage_type                 VARCHAR2(17),    -- Storage option for the XMLtype data:
                                                  --   OBJECT-RELATIONAL,  BINARY,  CLOB
    anyschema                    VARCHAR2(3),     -- If storage is BINARY, indicates whether the column allows ANYSCHEMA (YES) or not (NO), else NULL
    nonschema                    VARCHAR2(3)      -- If storage is BINARY, indicates whether the column allows NONSCHEMA (YES) or not (NO), else NULL
  );

  TYPE gt_xml_col_arr IS TABLE OF gt_xml_col_rec INDEX BY PLS_INTEGER;


  TYPE gt_varray_rec IS RECORD(
    owner                        arrays.gt_name,       -- Owner of the table containing the varray
    table_name                   arrays.gt_name,       -- Name of the containing table
    column_name                  VARCHAR2(4000),       -- Name of the varray column or attribute
    column_indx                  PLS_INTEGER,          -- Column index in column_arr
    type_owner                   arrays.gt_name,       -- Owner of the varray type
    type_name                    arrays.gt_name,       -- Name of the varray type
    lob_name                     arrays.gt_name,       -- Name of the LOB if the varray is stored in a LOB
    lob_column_indx              PLS_INTEGER,          -- Lob column index in column_arr
    storage_spec                 arrays.gt_name,       -- Indicates whether the storage was defaulted (DEFAULT) or user-specified (USER_SPECIFIED)
    return_type                  VARCHAR2(20),         -- Return type of the column: LOCATOR,  VALUE
    element_substitutable        VARCHAR2(25)          -- Indicates whether the varray element is substitutable (Y) or not (N)
  );

  TYPE gt_varray_arr IS TABLE OF gt_varray_rec INDEX BY PLS_INTEGER;


  TYPE gt_partition_rec IS RECORD (
    object_type                  VARCHAR2(5),          -- TABLE/INDEX
    object_owner                 arrays.gt_name,       -- Owner of the table/index
    object_name                  arrays.gt_name,       -- Name of the table/index
    composite                    VARCHAR2(3),          -- Indicates whether the table is composite-partitioned (YES) or not (NO)
    partition_level              VARCHAR2(12),         -- PARTITION/SUBPARTITION
    partition_type               VARCHAR2(20),         -- LIST/RANGE/HASH/SYSTEM/REFERENCE
    partition_name               arrays.gt_name,       -- Name of the partition
    parent_partition_name        arrays.gt_name,       -- Name of the parent partition (for subpartitions)
    high_value                   VARCHAR2(32767),      -- Partition/subartition bound value expression
    position                     NUMBER,               -- Position of the partition/subpartition within the table/partition
    partition_rec                partition_utils.gt_partition_rec,
    physical_attr_rec            gt_physical_attr_rec, -- physical attributes
    compression_rec              gt_compression_rec,   -- compression
    parallel_rec                 gt_parallel_rec,      -- parallel degree
    logging                      VARCHAR2(7),          -- Indicates whether or not changes to the table are logged:
                                                       --   NONE - Not specified, YES, NO
    tablespace_name              arrays.gt_name,       -- Tablespace name
    iot_key_compression          VARCHAR2(8),          -- Indicates whether index compression is enabled (ENABLED) or not (DISABLED)
    overflow_tablespace          arrays.gt_name,       -- Tablespace of overflow tablespace name
    overflow_physical_attr_rec   gt_physical_attr_rec, -- physical attributes
    overflow_logging             VARCHAR2(7),          -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    parent_table_partition       arrays.gt_name,       -- Parent table's corresponding partition
    interval                     VARCHAR2(3),          -- Indicates whether the partition is in the interval section of an interval partitioned table (YES) or whether the partition is in the range section (NO)
    subpartition_from_indx       PLS_INTEGER,          -- left range in subpartition array
    subpartition_to_indx         PLS_INTEGER,          -- right range in subpartition array
    lob_arr                      gt_lob_arr,           -- LOB attributes indexed by column INDEX
    indx                         PLS_INTEGER,          -- Index in array
    matching_indx                PLS_INTEGER,          -- index of matching partition from source table (populated for both tables as cross-reference)
    subpart_comp_result          PLS_INTEGER,          -- result of subpartitions compare
    is_partition_empty           BOOLEAN,              -- indicates is partition empty or not
    filter_clause                VARCHAR2(32767)       -- filtering clause to select all data from this partition (used for list and range only)
  );

  TYPE gt_partition_arr IS TABLE OF gt_partition_rec INDEX BY PLS_INTEGER;


  TYPE gt_subpartition_template_rec IS RECORD (
    subpartition_type            VARCHAR2(20),         -- Type of the subpartitioning
    subpartition_name            VARCHAR2(34),         -- Name of the subpartition
    subpartition_position        NUMBER,               -- Position of the subpartition
    tablespace_name              arrays.gt_name,       -- Tablespace name of the subpartition
    tablespace_set_flag          BOOLEAN,              -- identifies if DEFAULT tablespace set explicitly in the script
    high_bound                   VARCHAR2(32767),      -- Literal list values of the subpartition
    lob_template_arr             gt_lob_template_arr   -- LOB templates
  );

  TYPE gt_subpartition_template_arr IS TABLE OF gt_subpartition_template_rec INDEX BY PLS_INTEGER;


  TYPE gt_privilege_rec IS RECORD (
    grantor                      arrays.gt_name,       -- Name of the user who performed the grant
    grantee                      arrays.gt_name,       -- Name of the user to whom access was granted
    table_schema                 arrays.gt_name,       -- Schema of the object
    table_name                   arrays.gt_name,       -- Name of the object
    column_name                  arrays.gt_name,       -- Name of the column
    privilege                    VARCHAR2(40),         -- Privilege on the object
    grantable                    VARCHAR2(3),          -- Indicates whether the privilege was granted with the GRANT OPTION (YES) or not (NO)
    hierarchy                    VARCHAR2(3)           -- Indicates whether the privilege was granted with the HIERARCHY OPTION (YES) or not (NO)
  );

  TYPE gt_privilege_arr IS TABLE OF gt_privilege_rec INDEX BY PLS_INTEGER;


  TYPE gt_index_rec IS RECORD(
    indx                         PLS_INTEGER,         -- index in array
    owner                        arrays.gt_name,      -- Owner of the index
    index_name                   arrays.gt_name,      -- Name of the index
    index_type                   VARCHAR2(27),        -- Type of the index:
                                                      --   NORMAL, NORMAL/REV, BITMAP, FUNCTION-BASED NORMAL,
                                                      --   FUNCTION-BASED NORMAL/REV, FUNCTION-BASED BITMAP,
                                                      --   CLUSTER, IOT - TOP, DOMAIN
    table_owner                  arrays.gt_name,      -- Owner of the indexed object
    table_name                   arrays.gt_name,      -- Name of the indexed object
    table_type                   VARCHAR2(30),        -- Type of the indexed object:
                                                      --   NEXT OBJECT, INDEX, TABLE, CLUSTER, VIEW, SYNONYM, SEQUENCE
    table_object_owner           arrays.gt_name,      -- object owner for object table
    table_object_type            arrays.gt_name,      -- object name for object table
    column_arr                   arrays.gt_name_arr,  -- Column names ordered by position
    sort_order_arr               arrays.gt_str_arr,   -- Sort orders (ASC,DESC) ordered by position
    column_expr_arr              arrays.gt_xlstr_arr, -- Column expressions ordered by position
    uniqueness                   VARCHAR2(9),         -- Indicates whether the index is unique (UNIQUE) or nonunique (NONUNIQUE)
    compression                  VARCHAR2(13),         -- Indicates whether index compression is enabled
                                                      --   From version 12 available values are:
                                                      --     ADVANCED HIGH - Advanced high compression (from 12.2)
                                                      --     ADVANCED LOW - Advanced low compression (from 12.1)
                                                      --     DISABLED - No compression is present
                                                      --     ENABLED - Prefix compression (before 12.2)
                                                      --     PREFIX - Prefix compression (since 12.2)
    prefix_length                PLS_INTEGER,         -- Number of columns in the prefix of the compression key
    tablespace_name              arrays.gt_name,      -- Name of the tablespace containing the index
    status                       VARCHAR2(8),         -- Indicates whether a nonpartitioned index is VALID or UNUSABLE, for partitioned index
    physical_attr_rec            gt_physical_attr_rec,--  physical attributes
    pct_threshold                NUMBER,              -- Threshold percentage of block space allowed per index entry
    include_column               PLS_INTEGER,         -- Column ID of the last column to be included in index-organized table primary key (non-overflow) index. This column maps to the COLUMN_ID column of the *_TAB_COLUMNS view.
    parallel_rec                 gt_parallel_rec,     -- parallel degree
    logging                      VARCHAR2(7),         -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    partitioned                  VARCHAR2(3),         -- Indicates whether the table is partitioned (YES) or not (NO)
    partitioning_type            VARCHAR2(9),         -- Type of the partitioning method:
                                                      --     RANGE, HASH, SYSTEM, LIST, REFERENCE
    subpartitioning_type         VARCHAR2(7),         -- Type of the composite partitioning method:
                                                      --     RANGE, HASH, SYSTEM, LIST
    part_key_column_arr          arrays.gt_name_arr,  -- List of partition key columns
    subpart_key_column_arr       arrays.gt_name_arr,  -- List of subpartition key columns
    partition_arr                gt_partition_arr,    -- array of partitions
    partition_indx_arr           arrays.gt_int_indx,  -- index by partition name
    subpartition_arr             gt_partition_arr,    -- array of subpartitions
    subpartition_indx_arr        arrays.gt_int_indx,  -- index by subpartition name
    locality                     VARCHAR2(6),         -- Indicates whether the partitioned index is local (LOCAL) or global (GLOBAL)
    alignment                    VARCHAR2(12),        -- Indicates whether the partitioned index is prefixed (PREFIXED) or non-prefixed (NON_PREFIXED)
    interval                     VARCHAR2(1000),      -- String of the interval value
    temporary                    VARCHAR2(1),         -- Indicates whether the index is on a temporary table (Y) or not (N)
    duration                     VARCHAR2(15),        -- Indicates the duration of a temporary table:
                                                      --    SYS$SESSION - Rows are preserved for the duration of the session
                                                      --    SYS$TRANSACTION - Rows are deleted after COMMIT
    generated                    VARCHAR2(1),         -- Indicates whether the name of the index is system-generated (Y) or not (N)
    secondary                    VARCHAR2(1),         -- Indicates whether the index is a secondary object created by theODCIIndexCreate method of the Oracle Data Cartridge (Y) or not (N)
    pct_direct_access            NUMBER,              -- For a secondary index on an index-organized table, the percentage of rows with VALID guess
    ityp_owner                   arrays.gt_name,        -- For a domain index, the owner of the indextype
    ityp_name                    arrays.gt_name,        -- For a domain index, the name of the indextype
    parameters                   VARCHAR2(1000),      -- For a domain index, the parameter string
    domidx_status                VARCHAR2(12),        -- Status of a domain index:
                                                      --    NULL - Index is not a domain index
                                                      --    VALID - Index is a valid domain index
                                                      --    IDXTYP_INVLD - Indextype of the domain index is invalid
    domidx_opstatus              VARCHAR2(6),         -- Status of the operation on a domain index:
                                                      --    NULL - Index is not a domain index
                                                      --    VALID - Operation performed without errors
                                                      --    FAILED - Operation failed with an error
    funcidx_status               VARCHAR2(8),         -- Status of a function-based index:
                                                      --    NULL - Index is not a function-based index
                                                      --    ENABLED - Function-based index is enabled
                                                      --    DISABLED - Function-based index is disabled
    visibility                   VARCHAR2(10),        -- Indicates whether the index is VISIBLE or INVISIBLE to the optimizer
    join_index                   VARCHAR2(3),         -- Indicates whether the index is a join index (YES) or not (NO)
    join_inner_owner_arr         arrays.gt_name_arr,  -- Array of owner of join inner table (for join index)
    join_inner_table_arr         arrays.gt_name_arr,  -- Array of name of join inner table (for join index)
    join_inner_column_arr        arrays.gt_name_arr,  -- Array of column of join inner table (for join index)
    join_outer_owner_arr         arrays.gt_name_arr,  -- Array of owner of join inner table (for join index)
    join_outer_table_arr         arrays.gt_name_arr,  -- Array of name of join inner table (for join index)
    join_outer_column_arr        arrays.gt_name_arr,  -- Array of column of join inner table (for join index)
    column_table_owner_arr       arrays.gt_name_arr,  -- Array of owners of tables of columns (for join index)
    column_table_arr             arrays.gt_name_arr,  -- Array of tables of columns (for join index)
    orphaned_entries             VARCHAR2(3),         -- Indicates whether a global index contains stale entries because of deferred index maintenance during DROP/TRUNCATE PARTITION, or MODIFY PARTITION INDEXING OFF operations.
                                                      --   Possible values:
                                                      --     YES - The index contains orphaned entries
                                                      --     NO - The index does not contain orphaned entries
    indexing                     VARCHAR2(7),         -- Indicates whether a global index is decoupled from the underlying table.
                                                      --   Possible values:
                                                      --     PARTIAL - The index is partial, that is, it will follow the table's indexing property.
                                                      --     FULL - The index will include all partitions of the table.
    recreate_flag                BOOLEAN,             -- Indicates index need to be recreated
    drop_flag                    BOOLEAN,             -- Indicates that DROP claue has been already generated for this index or index has been dropped implicitly
    constraint_name              arrays.gt_name,        -- Constraint (PK/UK only) name assigned to this index.
    rename_rec                   gt_rename_rec        -- record holding names for renaming
  );

  TYPE gt_index_arr IS TABLE OF gt_index_rec INDEX BY PLS_INTEGER;

  TYPE gt_trigger_rec IS RECORD(
    owner                        arrays.gt_name, -- Owner of the trigger
    trigger_name                 arrays.gt_name, -- Name of the trigger
    trigger_type                 VARCHAR2(16),   -- When the trigger fires: BEFORE STATEMENT, BEFORE EACH ROW, AFTER STATEMENT, AFTER EACH ROW, INSTEAD OF, COMPOUND
    triggering_event             VARCHAR2(227),  -- DML, DDL, or database event that fires the trigger
    table_owner                  arrays.gt_name, -- Owner of the table on which the trigger is defined
    base_object_type             VARCHAR2(16),   -- Base object on which the trigger is defined: TABLE, VIEW, SCHEMA, DATABASE
    table_name                   arrays.gt_name, -- If the base object type of the trigger is SCHEMA or DATABASE, then this column is NULL; if the base object type of the trigger is TABLE or VIEW, then this column indicates the table or view name on which the trigger is defined
    column_name                  VARCHAR2(4000), -- Name of the nested table column (if a nested table trigger), else NULL
    referencing_names            VARCHAR2(128),  -- Names used for referencing OLD and NEW column values from within the trigger
    when_clause                  VARCHAR2(4000), -- Must evaluate to TRUE for TRIGGER_BODY to execute
    status                       VARCHAR2(8),    -- Indicates whether the trigger is enabled (ENABLED) or disabled (DISABLED)
    description                  VARCHAR2(4000), -- Trigger description; useful for re-creating a trigger creation statement
    action_type                  VARCHAR2(11),   -- Action type of the trigger body: CALL, PL/SQL
    trigger_body                 VARCHAR2(32767),-- Statements executed by the trigger when it fires
    referenced_trigger_indx      arrays.gt_name, -- Owner of the referenced trigger
    rename_rec                   gt_rename_rec   -- record holding names for renaming
  );

  TYPE gt_trigger_arr IS TABLE OF gt_trigger_rec INDEX BY PLS_INTEGER;


  TYPE gt_policy_rec IS RECORD(
    object_owner                 arrays.gt_name,     -- Owner of the synonym, table, or view
    object_name                  arrays.gt_name,     -- Name of the synonym, table, or view
    policy_group                 arrays.gt_name,     -- Name of the policy group
    policy_name                  arrays.gt_name,     -- Name of the policy
    pf_owner                     arrays.gt_name,     -- Owner of the policy function
    package                      arrays.gt_name,     -- Name of the package containing the policy function
    function                     arrays.gt_name,     -- Name of the policy function
    sel                          VARCHAR2(3),        -- Indicates whether the policy is applied to queries on the object (YES) or not (NO)
    ins                          VARCHAR2(3),        -- Indicates whether the policy is applied to INSERT statements on the object (YES) or not (NO)
    upd                          VARCHAR2(3),        -- Indicates whether the policy is applied to UPDATE statements on the object (YES) or not (NO)
    del                          VARCHAR2(3),        -- Indicates whether the policy is applied to DELETE statements on the object (YES) or not (NO)
    idx                          VARCHAR2(3),        -- Indicates whether the policy is enforced for index maintenance on the object (YES) or not (NO)
    chk_option                   VARCHAR2(3),        -- Indicates whether the check option is enforced for the policy (YES) or not (NO)
    enable                       VARCHAR2(3),        -- Indicates whether the policy is enabled (YES) or disabled (NO)
    static_policy                VARCHAR2(3),        -- Indicates whether the policy is static (YES) or not (NO)
    policy_type                  VARCHAR2(24),       -- Policy type: STATIC, SHARED_STATIC, CONTEXT_SENSITIVE, SHARED_CONTEXT_SENSITIVE, DYNAMIC
    long_predicate               VARCHAR2(3),        -- Indicates whether the policy function can return a maximum of 32 KB of predicate (YES) or not (NO). If NO, the default maximum predicate size is 4000 bytes.
    sec_rel_col_arr              arrays.gt_name_arr, -- Name of the security relevant column
    column_option                VARCHAR2(20)        -- Option of the security relevant column: NULL, NONE, ALL_ROWS
  );

  TYPE gt_policy_arr IS TABLE OF gt_policy_rec INDEX BY PLS_INTEGER;

  TYPE gt_nested_table_rec IS RECORD(
    owner                        arrays.gt_name,       -- owner of the nested table
    table_name                   arrays.gt_name,       -- name of the nested table
    table_type_owner             arrays.gt_name,       -- owner of the type of which the nested table was created
    table_type_name              arrays.gt_name,       -- name of the type of the nested table
    parent_table_name            arrays.gt_name,       -- name of the parent table containing the nested table
    parent_table_column          VARCHAR2(4000),       -- column name of the parent table that corresponds to the nested table
    column_indx                  PLS_INTEGER,          -- Column index in column_arr
    storage_spec                 VARCHAR2(120),        -- indicates whether storage for the nested table is user_specified or default
    return_type                  VARCHAR2(20),         -- return type of the varray column (locator) or (value)
    element_substitutable        VARCHAR2(1),          -- indicates whether the nested table element is substitutable (Y) or not (N)
    rename_rec                   gt_rename_rec         -- record holding names for renaming
  );
  TYPE gt_nested_table_arr IS TABLE OF gt_nested_table_rec INDEX BY PLS_INTEGER;


  TYPE gt_table_rec      IS RECORD(
    owner                        arrays.gt_name,               -- Owner of the table
    table_name                   arrays.gt_name,               -- Name of the table
    tablespace_name              arrays.gt_name,               -- Name of the tablespace containing the table; NULL for partitioned, temporary, and index-organized tables
    cluster_name                 arrays.gt_name,               -- Name of the cluster, if any, to which the table belongs
    cluster_owner                arrays.gt_name,               -- Owner of the cluster, if any, to which the table belongs
    iot_name                     arrays.gt_name,               -- Name of the index-organized table, if any, to which the overflow or mapping table entry belongs. If the IOT_TYPE column is not NULL, then this column contains the base table name.
    iot_type                     VARCHAR2(12),                 -- If the table is an index-organized table, then IOT_TYPE is IOT, IOT_OVERFLOW, or IOT_MAPPING. If the table is not an index-organized table, then IOT_TYPE is NULL.
    iot_index_name               arrays.gt_name,               -- Name of the index (primary key) for IOT table
    iot_index_owner              arrays.gt_name,               -- Owner of the index (primary key) for IOT table
    iot_pk_column_arr            arrays.gt_name_arr,           -- List of PK key columns
    iot_pk_column_sort_type_arr  arrays.gt_name_arr,           -- List of PK key columns sort order (descend/ascend)
    iot_pct_threshold            NUMBER,                       -- Threshold percentage of block space allowed per index entry
    iot_include_column           PLS_INTEGER,                  -- Column ID of the last column to be included in index-organized table primary key (non-overflow) index. This column maps to the COLUMN_ID column of the *_TAB_COLUMNS view.
    iot_prefix_length            PLS_INTEGER,                  -- Number of columns in the prefix of the compression key
    iot_key_compression          VARCHAR2(8),                  -- Indicates whether index compression is enabled (ENABLED) or not (DISABLED)
    overflow_table_name          arrays.gt_name,               -- Name of overflow table
    overflow_tablespace          arrays.gt_name,               -- Tablespace of overflow tablespace name
    overflow_physical_attr_rec   gt_physical_attr_rec,         -- physical attributes
    overflow_logging             VARCHAR2(7),                  -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    mapping_table                VARCHAR2(1),                  -- Y - yes, N - no
    partitioned                  VARCHAR2(3),                  -- Indicates whether the table is partitioned (YES) or not (NO)
    partitioning_type            VARCHAR2(9),                  -- Type of the partitioning method:
                                                               --     RANGE, HASH, SYSTEM, LIST, REFERENCE
    subpartitioning_type         VARCHAR2(7),                  -- Type of the composite partitioning method:
                                                               --     NONE, RANGE, HASH, SYSTEM, LIST
    part_key_column_arr          arrays.gt_name_arr,           -- List of partition key columns
    subpart_key_column_arr       arrays.gt_name_arr,           -- List of subpartition key columns
    subpartition_template_arr    gt_subpartition_template_arr, -- List of subpartition templates
    ref_ptn_constraint_name      arrays.gt_name,               -- Name of the partitioning referential constraint for reference-partitioned tables
    interval                     VARCHAR2(1000),               -- String of the interval value
    temporary                    VARCHAR2(1),                  -- Indicates whether the table is temporary (Y) or not (N)
    duration                     VARCHAR2(15),                 -- Indicates the duration of a temporary table:
                                                               --   SYS$SESSION - Rows are preserved for the duration of the session
                                                               --   SYS$TRANSACTION - Rows are deleted after COMMIT
                                                               --   Null - Permanent table
    secondary                    VARCHAR2(1),                  -- Indicates whether the table is a secondary object created by the ODCIIndexCreate method of the Oracle Data Cartridge (Y) or not (N)
    nested                       VARCHAR2(3),                  -- Indicates whether the table is a nested table (YES) or not (NO)
    object_id_type               VARCHAR2(16),                 -- Indicates whether the object ID (OID) is USER-DEFINED or SYSTEM GENERATED
    table_type_owner             arrays.gt_name,               -- If an object table, owner of the type from which the table is created
    table_type                   arrays.gt_name,               -- If an object table, type of the table
    row_movement                 VARCHAR2(8),                  -- Indicates whether partitioned row movement is enabled (ENABLED) or disabled (DISABLED)
    dependencies                 VARCHAR2(8),                  -- Indicates whether row-level dependency tracking is enabled (ENABLED) or disabled (DISABLED)
    physical_attr_rec            gt_physical_attr_rec,         -- physical attributes
    compression_rec              gt_compression_rec,           -- compression
    compressed_partitions        BOOLEAN,                      -- TRUE of there is at least one compressed partition, otherwise FALSE
    parallel_rec                 gt_parallel_rec,              -- parallel degree
    logging                      VARCHAR2(7),                  -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    cache                        VARCHAR2(5),                  -- Indicates whether the table is to be cached in the buffer cache (Y) or not (N)
    monitoring                   VARCHAR2(3),                  -- Indicates whether the table has the MONITORING attribute set (YES) or not (NO)
    read_only                    VARCHAR2(3),                  -- Indicates whether the table IS READ-ONLY (YES) or not (NO)
    result_cache                 VARCHAR2(7),                  -- Result cache mode annotation for the table:
                                                               --   DEFAULT - Table has not been annotated, FORCE, MANUAL
    tab_comment                  VARCHAR2(4000),               -- table comment
    rename_rec                   gt_rename_rec,                -- record holding names for renaming
    column_arr                   gt_column_arr,                -- array of columns
    column_indx_arr              arrays.gt_int_indx,           -- index by column name
    column_qualified_indx_arr    arrays.gt_int_indx,           -- index by qualified column name=column_na
    constraint_arr               gt_constraint_arr,            -- array of constraints
    constraint_indx_arr          arrays.gt_int_indx,           -- index by constraint name
    log_group_arr                gt_log_group_arr,             -- array of log groups
    log_group_indx_arr           arrays.gt_int_indx,           -- index by log group name
    index_arr                    gt_index_arr,                 -- array if indexes
    index_indx_arr               arrays.gt_int_indx,           -- index by "index_owner"."index_name"
    lob_arr                      gt_lob_arr,                   -- arrays of lob columns indexed by LOB column index
    xml_col_arr                  gt_xml_col_arr,               -- arrays of xml columns indexed by XML column index
    varray_arr                   gt_varray_arr,                -- arrays of varray columns indexed by VARRAY column indx
    partition_arr                gt_partition_arr,             -- array of partitions
    partition_indx_arr           arrays.gt_int_indx,           -- index by partition name
    subpartition_arr             gt_partition_arr,             -- array of subpartitions
    subpartition_indx_arr        arrays.gt_int_indx,           -- index by subpartition name
    ref_constraint_arr           gt_constraint_arr,            -- array of foreign keys referencing on the table from other tables
    ref_constraint_indx_arr      arrays.gt_int_indx,           -- index by foreign key name
    join_index_arr               gt_index_arr,                 -- array of join indexes on other tables joined with given table
    join_index_indx_arr          arrays.gt_int_indx,           -- index by join indexes
    is_table_empty               BOOLEAN,                      -- indicates is table empty or not
    privilege_arr                gt_privilege_arr,             -- array of table privileges
    trigger_arr                  gt_trigger_arr,               -- array of tables triggers
    trigger_indx_arr             arrays.gt_int_indx,           -- index by triggers
    policy_arr                   gt_policy_arr,                -- array of tables policies
    tablespace_block_size_indx   arrays.gt_num_indx,           -- index of block_size of tablespaces
    cluster_column_arr           arrays.gt_name_arr,           -- array of cluster key columns
    nested_tables_arr            gt_nested_table_arr           -- array of nested tables
  );
  
  TYPE gt_table_arr IS TABLE OF gt_table_rec INDEX BY PLS_INTEGER;

  TYPE gt_sequence_rec    IS RECORD(
    owner                        arrays.gt_name,               -- Owner of the sequence
    sequence_name                arrays.gt_name,               -- Name of the sequence
    min_value                    NUMBER,                       -- Minimum value of the sequence
    max_value                    NUMBER,                       -- Maximum value of the sequence
    increment_by                 NUMBER,                       -- Value by which sequence is incremented
    cycle_flag                   VARCHAR2(1),                  -- Indicates whether the sequence wraps around on reaching the limit (Y) or not (N)
    order_flag                   VARCHAR2(1),                  -- Indicates whether sequence numbers are generated in order (Y) or not (N)
    cache_size                   NUMBER,                       -- Number of sequence numbers to cache
    last_number                  NUMBER,                       -- Last sequence number written to disk. If a sequence uses caching, the number written to disk is the last number placed in the sequence cache. This number is likely to be greater than the last sequence number that was used.
    rename_rec                   gt_rename_rec,                -- record holding names for renaming
    privilege_arr                gt_privilege_arr              -- array of table privileges
  );


  TYPE gt_method_param_rec   IS RECORD(
    owner                        arrays.gt_name,         -- Owner of the type
    type_name                    arrays.gt_name,         -- Name of the type
    method_name                  arrays.gt_name,         -- Name of the method
    method_no                    NUMBER,                 -- For an overloaded method, a number distinguishing this method from others of the same. Do not confuse this number with the object ID.
    param_name                   arrays.gt_name,         -- Name of the parameter
    param_no                     NUMBER,                 -- Parameter number (position)
    param_mode                   VARCHAR2(6),            -- Mode of the parameter (IN, OUT, IN OUT)
    param_type_mod               VARCHAR2(7),            -- Whether this parameter is a REF to another object
    param_type_owner             arrays.gt_name,         -- Owner of the type of the parameter
    param_type_name              arrays.gt_name,         -- Name of the type of the parameter
    character_set_name           VARCHAR2(44)            -- Whether the character set or the method is fixed-length character set (CHAR_CS) or fixed-length national character set (NCHAR_CS), or a particular character set specified by the user
  );
  TYPE gt_method_param_arr IS TABLE OF gt_method_param_rec INDEX BY PLS_INTEGER;


  TYPE gt_type_method_rec   IS RECORD(
    owner                        arrays.gt_name,         -- Owner of the type
    type_name                    arrays.gt_name,         -- Name of the type
    method_name                  arrays.gt_name,         -- Name of the method
    method_no                    NUMBER,                 -- Method number for distinguishing overloaded methods (not to be used as ID number)
    method_type                  VARCHAR2(6),            -- Type of the method: MAP, ORDER, PUBLIC
    parameter_arr                gt_method_param_arr,    -- Array of parameters to the method
    result_rec                   gt_method_param_rec,    -- Arrays of results returned by the method
    final                        VARCHAR2(3),            -- Indicates whether the method is final (YES) or not (NO)
    instantiable                 VARCHAR2(3),            -- Indicates whether the method is instantiable (YES) or not (NO)
    overriding                   VARCHAR2(3),            -- Indicates whether the method is overriding a supertype method (YES) or not (NO)
    inherited                    VARCHAR2(3),            -- Indicates whether the method is inherited from a supertype (YES) or not (NO)
    static                       VARCHAR2(3),            -- Indicates whether the method is static or not (YES) or not (NO)
    constructor                  VARCHAR2(3)             -- YES/NO flag
  );
  TYPE gt_type_method_arr IS TABLE OF gt_type_method_rec INDEX BY PLS_INTEGER;

  TYPE gt_type_method_ind_arr IS TABLE OF arrays.gt_int_arr INDEX BY arrays.gt_name; -- index by method nam

  TYPE gt_dependency_rec  IS RECORD(
    owner                        arrays.gt_name,         -- Owner of the object
    name                         arrays.gt_name,         -- Name of the object
    type                         arrays.gt_name          -- Type of the object
  );

  TYPE gt_dependency_arr IS TABLE OF gt_dependency_rec INDEX BY PLS_INTEGER;
  
  TYPE gt_type_rec        IS RECORD(
    owner                        arrays.gt_name,         -- Owner of the type
    type_name                    arrays.gt_name,         -- Real name of the type (could be changed by adding #N where N is a digit when cort type rename is used)
    typecode                     arrays.gt_name,         -- Typecode of the type
    method_arr                   gt_type_method_arr,     -- Array of methods in the type
    method_ind_arr               gt_type_method_ind_arr, -- Index of arrays of methods(1 or many if overloaded) indexed by method name.
    predefined                   VARCHAR2(3),            -- Indicates whether the type is a predefined type (YES) or not (NO)
    incomplete                   VARCHAR2(3),            -- Indicates whether the type is an incomplete type (YES) or not (NO)
    final                        VARCHAR2(3),            -- Indicates whether the type is a final type (YES) or not (NO)
    instantiable                 VARCHAR2(3),            -- Indicates whether the type is an instantiable type (YES) or not (NO)
    persistable                  VARCHAR2(3),            -- Indicates whether the type is a persistable type (YES) or not (NO)
    supertype_owner              arrays.gt_name,         -- Owner of the supertype (NULL if type is not a subtype)
    supertype_name               arrays.gt_name,         -- Name of the supertype (NULL if type is not a subtype)
    local_attributes             NUMBER,                 -- Number of local (not inherited) attributes (if any) in the subtype
    local_methods                NUMBER,                 -- Number of local (not inherited) methods (if any) in the subtype
    table_dependency_arr         gt_table_arr,           -- Array of dependent tables
    type_dependency_arr          gt_dependency_arr,      -- Array of dependent types
--    sql_text                     CLOB,
    rename_rec                   gt_rename_rec,          -- record holding names for renaming
    privilege_arr                gt_privilege_arr        -- array of table privileges
  );
  
  TYPE gt_type_arr IS TABLE OF gt_type_rec INDEX BY PLS_INTEGER;

  TYPE gt_view_rec        IS RECORD(
    owner                        arrays.gt_name,         -- Owner of the view
    view_name                    arrays.gt_name,         -- Name of the view
    view_text                    CLOB,                   -- View text
    type_text                    VARCHAR2(4000),         -- Type clause of the typed view
    oid_text                     varchar2(4000),         -- with oid clause of the typed view
    view_type_owner              arrays.gt_name,         -- owner of the type of the view if the view is a typed view
    view_type                    arrays.gt_name,         -- type of the view if the view is a typed view
    superview_name               arrays.gt_name,         -- name of the superview
    editioning_view              varchar2(1),            -- reserved for future use
    read_only                    varchar2(1),            -- indicates whether the view is read-only (y) or not (n)
    columns_arr                  arrays.gt_name_arr      -- array of column names
  );
 

  TYPE gt_change_rec IS RECORD(
    group_type                   VARCHAR2(50),           -- logical group
    change_sql                   CLOB,                   -- forward change DDL/SQL
    revert_sql                   CLOB,                   -- revert change DDL/SQL 
    display_sql                  VARCHAR2(4000),         -- short version of SQL to display in explain plan (if NULL show full SQL) 
    status                       VARCHAR2(20),
    start_time                   TIMESTAMP,
    end_time                     TIMESTAMP,
    revert_start_time            TIMESTAMP,
    revert_end_time              TIMESTAMP,
    threadable                   VARCHAR(1),
    error_msg                    VARCHAR2(1000),
    row_id                       ROWID
  );
  
  TYPE gt_change_arr IS TABLE OF gt_change_rec INDEX BY PLS_INTEGER;
  
   
  TYPE gt_data_source_rec IS RECORD(
    data_source_sql             CLOB,                    -- custom SQL
    column_arr                  gt_column_arr,           -- array of columns
    column_indx_arr             arrays.gt_int_indx,      -- index by column name
    data_filter                 VARCHAR2(32767),         -- custom filter for default data source
    data_values                 arrays.gt_xlstr_indx,    -- array of mapped values indexed by column name 
    part_source_tab_rec         gt_table_rec,
    change_result               PLS_INTEGER
  );
  

  -- record for holding current run params
  g_run_params                   cort_params_pkg.gt_run_params_rec;
  -- record for holding current change params
  g_params                       cort_params_pkg.gt_params_rec;

  -- output debug text
  PROCEDURE debug(
    in_text      IN CLOB,
    in_details   IN CLOB DEFAULT NULL
  );

  -- format begin/end message
  FUNCTION format_message(
    in_template     IN VARCHAR2,
    in_object_type  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2;

  -- raise application error wrapper
  PROCEDURE raise_error(
    in_msg  IN VARCHAR,
    in_code IN NUMBER DEFAULT -20000
  );

  PROCEDURE check_if_process_active(in_job_rec IN cort_jobs%ROWTYPE);

  -- executes DDL command
  PROCEDURE execute_immediate(
    in_sql        IN CLOB,
    in_echo       IN BOOLEAN DEFAULT TRUE,
    in_test       IN BOOLEAN DEFAULT FALSE
  );

  -- constructor for gt_change_rec
  FUNCTION change_rec(
    in_group_type  IN VARCHAR2,
    in_change_sql  IN CLOB,
    in_revert_sql  IN CLOB DEFAULT NULL,
    in_display_sql IN VARCHAR2 DEFAULT NULL,
    in_threadable  IN VARCHAR2 DEFAULT 'N'
  )
  RETURN gt_change_rec;

  -- change status for the given change_rec
  FUNCTION change_status(
    in_rec    IN gt_change_rec, 
    in_status IN VARCHAR2
  )
  RETURN gt_change_rec;

  -- add new entry to array
  PROCEDURE add_change(
    io_change_arr IN OUT NOCOPY gt_change_arr,
    in_change_rec IN gt_change_rec
  );

  -- add new entry to array
  PROCEDURE add_changes(
    io_change_arr IN OUT NOCOPY gt_change_arr,
    in_change_arr IN gt_change_arr
  );
  
/*
    -- execute DDL changes
  PROCEDURE apply_changes(
    io_change_arr   IN OUT NOCOPY gt_change_arr,
    in_test         IN BOOLEAN DEFAULT FALSE,
    in_echo         IN BOOLEAN DEFAULT TRUE,
    in_act_on_error IN VARCHAR2 DEFAULT 'REVERT' -- 'REVERT', 'RAISE', 'SKIP'
  );
*/
  -- Returns TRUE if table exists otherwise returns FALSE
  FUNCTION object_exists(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN BOOLEAN;

  FUNCTION get_prev_synonym_name(in_table_name IN VARCHAR2)
  RETURN VARCHAR2;

  FUNCTION get_object_temp_name(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_prefix       IN VARCHAR2 DEFAULT cort_params_pkg.gc_temp_prefix
  )
  RETURN VARCHAR2;

  -- return column indx in table column_arr by name
  FUNCTION get_column_indx(
    in_table_rec   IN gt_table_rec,
    in_column_name IN VARCHAR2
  )
  RETURN PLS_INTEGER;
  
  -- returns adt_rec by type_owner and type_name
  FUNCTION get_adt_rec(    
    in_owner      IN VARCHAR2,
    in_type_name  IN VARCHAR2
  ) 
  RETURN gt_adt_rec;

  -- read table metadata
  PROCEDURE read_table(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    in_read_data       IN BOOLEAN,
    out_table_rec      OUT NOCOPY gt_table_rec
  );

  -- read individual index metadata
  PROCEDURE read_index(
    in_index_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_index_rec      OUT NOCOPY gt_index_rec
  );

  -- read all indexes metadata on to given table
  PROCEDURE read_table_indexes(
    io_table_rec IN OUT NOCOPY gt_table_rec
  );

  -- read table properties and all attributes/dependant objects
  PROCEDURE read_table_cascade(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    in_read_data       IN BOOLEAN,
    in_debug_text      IN VARCHAR2,
    out_table_rec      OUT NOCOPY gt_table_rec
  );

-- temp for test 
PROCEDURE backup_table(
    in_table_rec       IN gt_table_rec,
--    in_type_rename_rec IN gt_rename_rec,
    io_change_arr      IN OUT NOCOPY gt_change_arr
  );
  
  -- read sequence metadata
  PROCEDURE read_sequence(
    in_sequence_name   IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_sequence_rec   IN OUT NOCOPY gt_sequence_rec
  );

  -- read object type metadata
  PROCEDURE read_type(
    in_type_name         IN VARCHAR2,
    in_owner             IN VARCHAR2,
--    in_read_dependencies IN BOOLEAN DEFAULT TRUE,
    out_type_rec         IN OUT NOCOPY gt_type_rec
  );

  -- read view metadata
  PROCEDURE read_view(
    in_view_name       IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_view_rec       OUT NOCOPY gt_view_rec
  );

  PROCEDURE copy_stats(
    in_owner        IN VARCHAR2,
    in_source_table IN VARCHAR2,
    in_target_table IN VARCHAR2,
    in_source_part  IN VARCHAR2 DEFAULT NULL,
    in_target_part  IN VARCHAR2 DEFAULT NULL,
    in_part_level   IN VARCHAR2 DEFAULT NULL
  );

  FUNCTION get_job_rec RETURN cort_jobs%ROWTYPE;

  -- reset global variables
  PROCEDURE init(
    in_job_rec IN cort_jobs%ROWTYPE
  );

  -- Public: create or replace object
  PROCEDURE create_or_replace(
    in_job_rec       IN cort_jobs%ROWTYPE
  );

  -- Add metadata of creating recreatable object
  PROCEDURE before_create_or_replace(
    in_job_rec       IN cort_jobs%ROWTYPE
  );

  -- Rename table and it's constraints and indexes
  PROCEDURE rename_table(
    in_job_rec       IN cort_jobs%ROWTYPE
  );

  PROCEDURE alter_object(
     in_job_rec       IN cort_jobs%ROWTYPE
  );

  PROCEDURE drop_object(
    in_job_rec       IN cort_jobs%ROWTYPE
  );

  -- Rollback the latest change for given object
  PROCEDURE revert_object(
    in_job_rec       IN cort_jobs%ROWTYPE
  );

  -- Resume pending changes for given object
  PROCEDURE resume_change(
    in_job_rec  in cort_jobs%ROWTYPE
  );

  -- Drop revert object when new release is created. Repoint prev synonym to actual table
  PROCEDURE reset_object(
    in_job_rec       IN cort_jobs%ROWTYPE
  );

  -- internal procedure. do not call directly!
  -- Procedure is called from job
  PROCEDURE execute_action(
    in_job_id IN TIMESTAMP
  );

END cort_exec_pkg;
/