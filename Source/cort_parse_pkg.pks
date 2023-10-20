CREATE OR REPLACE PACKAGE cort_parse_pkg
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
  Description: Parser utility for SQL commands and CORT hints
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support of indexes and sequences, create table as select
  15.00   | Rustam Kafarov    | Added support of objects
  17.00   | Rustam Kafarov    | Added cleanup, get_cort_indexes, parse_cort_index
  18.00   | Rustam Kafarov    | Introduced complext type gt_sql_rec instead of individual global variables
  20.00   | Rustam Kafarov    | Added support of long names introduced in Oracle 12.2 
  21.00   | Rustam Kafarov    | Split params into change and run params 
  ----------------------------------------------------------------------------------------------------------------------
*/

  TYPE gt_lexical_unit_rec IS RECORD(
    unit_type      VARCHAR2(20),    -- Could be: SQL, COMMENT, LINE COMMENT, LITERAL, CORT PARAM, QUOTED NAME
    text           CLOB,            -- original text
    start_position PLS_INTEGER,     -- start position in original SQL text (array index)
    end_position   PLS_INTEGER      -- end position in original SQL text
  );

  TYPE gt_lexical_unit_arr IS TABLE OF gt_lexical_unit_rec INDEX BY PLS_INTEGER;

  TYPE gt_replace_rec IS RECORD(
    object_type    VARCHAR2(50),
    object_name    VARCHAR2(128),
    start_pos      PLS_INTEGER,
    end_pos        PLS_INTEGER,
    new_name       VARCHAR2(128)
  );

  TYPE gt_replace_arr IS TABLE OF gt_replace_rec INDEX BY PLS_INTEGER;
  
  TYPE gt_sql_column_rec IS RECORD(
    column_name       arrays.gt_name,
    start_position    PLS_INTEGER,     -- definition start position in SQL text (array index)
    data_type         VARCHAR2(1024),
    virtual_flag      BOOLEAN
  );

  TYPE gt_sql_column_arr IS TABLE OF gt_sql_column_rec INDEX BY PLS_INTEGER; 

  TYPE gt_sql_rec IS RECORD(
    original_sql                 CLOB, -- original SQL
    normalized_sql               CLOB, -- normalized sql excluding literals and comments
    lexical_units_arr            gt_lexical_unit_arr, -- Parsed SQL stored as array of lexical units(SQL/comments/quoted names/string literals)
    replaced_names_arr           gt_replace_arr,      -- list of replaced names indexes by replace position
    object_type                  arrays.gt_name,
    object_name                  arrays.gt_name,
    object_owner                 arrays.gt_name,
    current_schema               arrays.gt_name,
    run_param_start_pos          PLS_INTEGER,
    run_param_end_pos            PLS_INTEGER,
    name_start_pos               PLS_INTEGER,
    definition_start_pos         PLS_INTEGER,
    columns_start_pos            PLS_INTEGER,
    columns_end_pos              PLS_INTEGER,
    columns_arr                  gt_sql_column_arr,
    as_select_flag               BOOLEAN,
    as_select_fromitself         BOOLEAN,
    as_select_sql                CLOB,
    subquery_start_pos           PLS_INTEGER,
    subquery_end_pos             PLS_INTEGER,
    partitioning_type            VARCHAR2(20),
    subpartitioning_type         VARCHAR2(20),
    part_key_column_arr          arrays.gt_name_arr,
    subpart_key_column_arr       arrays.gt_name_arr,
    partitions_start_pos         PLS_INTEGER,
    partitions_end_pos           PLS_INTEGER,
    partitions_count             PLS_INTEGER,    
    table_definition_start_pos   PLS_INTEGER,   -- index's table start definition (after keyword ON {CLUSTER})
    is_cluster                   BOOLEAN,       -- is index created in cluster
    table_name                   arrays.gt_name,  -- index's table name
    table_owner                  arrays.gt_name   -- index's owner name
  );

  TYPE gt_sql_arr IS TABLE OF gt_sql_rec INDEX BY PLS_INTEGER;

  FUNCTION get_regexp_const(
    in_value          IN VARCHAR2
  )
  RETURN VARCHAR2;

  FUNCTION is_simple_name(in_name IN VARCHAR2)
  RETURN BOOLEAN;


  PROCEDURE parse_sql(
    in_sql      IN   CLOB,
    out_sql_rec OUT NOCOPY gt_sql_rec
  );

  FUNCTION get_normalized_sql(
    in_sql_rec      IN gt_sql_rec,
    in_quoted_names IN BOOLEAN DEFAULT TRUE,
    in_str_literals IN BOOLEAN DEFAULT TRUE,
    in_comments     IN BOOLEAN DEFAULT TRUE
  )
  RETURN CLOB;

  PROCEDURE parse_run_params(
    in_sql          IN CLOB,
    io_params_rec   IN OUT NOCOPY cort_params_pkg.gt_run_params_rec
  );

  -- Parse column definition start/end positions
  PROCEDURE parse_table_sql(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_table_name IN VARCHAR2,
    in_owner_name IN VARCHAR2
  );

  -- wraper for parsers for all supported object types
  PROCEDURE parse_create_sql(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  );

  -- Finds index declarations in cort comments 
  PROCEDURE parse_table_indexes(
    in_sql_rec    IN gt_sql_rec,
    out_sql_arr   OUT NOCOPY gt_sql_arr 
  );

  -- modify partitions in original SQL
  PROCEDURE modify_partition_sql(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_sql_rec             IN OUT NOCOPY gt_sql_rec
  );
  
  -- replaces table name and all names of existing depending objects (constraints, log groups, indexes, lob segments)
  PROCEDURE replace_table_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    io_sql_rec   IN OUT NOCOPY gt_sql_rec
  );

  -- replace all cort synonyms for types with real type names  
  PROCEDURE replace_type_synonyms(
    io_sql_rec   IN OUT NOCOPY gt_sql_rec
  );

  -- replaces index and table names for CREATE INDEX statement
  PROCEDURE replace_index_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    in_index_rec IN cort_exec_pkg.gt_index_rec,
    io_sql_rec   IN OUT NOCOPY gt_sql_rec
  );

  -- replaces sequence name
  PROCEDURE replace_seq_names(
    in_rename_rec   IN cort_exec_pkg.gt_rename_rec,
    io_sql_rec      IN OUT NOCOPY gt_sql_rec
  );

  -- replaces type name
  PROCEDURE replace_type_names(
    in_rename_rec   IN cort_exec_pkg.gt_rename_rec,
    io_sql_rec      IN OUT NOCOPY gt_sql_rec
  );

  -- replaces names in expression
  PROCEDURE update_expression(
    io_expression      IN OUT NOCOPY VARCHAR2,
    in_replace_names   IN arrays.gt_name_indx
  );

  -- return original name for renamed object. If it wasn't rename return current name
  FUNCTION get_original_name(
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2;

  -- delete global array 
  PROCEDURE cleanup;


  -- parses create statement and return object type, owner and name
  PROCEDURE parse_explain_sql(
    in_sql           IN CLOB,
    out_object_type  OUT VARCHAR2,
    out_object_owner OUT VARCHAR2,
    out_object_name  OUT VARCHAR2
  );
/*
  FUNCTION is_create_or_replace_statement(
    in_sql           IN CLOB
  )
  RETURN VARCHAR2;

  FUNCTION get_alter_type_replace(in_sql IN CLOB)
  RETURN CLOB;
*/


  FUNCTION parse_qualified_col_expr(
    in_qualified_col_name IN VARCHAR2
 )
  RETURN VARCHAR2;


END cort_parse_pkg;
/