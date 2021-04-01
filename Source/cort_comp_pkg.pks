CREATE OR REPLACE PACKAGE cort_comp_pkg
AS

/*
CORT - Oracle database DevOps tool

Copyright (C) 2013  Softcraft Ltd - Rustam Kafarov

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
  Description: Main comparison functionality
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added comparioson for indexes and sequences. General improvements, bug fixing
  15.00   | Rustam Kafarov    | Added advanced partitions comparison
  16.00   | Rustam Kafarov    | Added get_create_view_sql
  18.00   | Rustam Kafarov    | Introduced constant gc_result_cas_from_itself  
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  gc_result_nochange          CONSTANT PLS_INTEGER := 0;
  gc_result_alter             CONSTANT PLS_INTEGER := 1;
  gc_result_alter_move        CONSTANT PLS_INTEGER := 2;
  gc_result_exchange          CONSTANT PLS_INTEGER := 3;
  gc_result_part_exchange     CONSTANT PLS_INTEGER := 4;
  gc_result_recreate          CONSTANT PLS_INTEGER := 5;
  gc_result_create_as_select  CONSTANT PLS_INTEGER := 6;
  gc_result_create            CONSTANT PLS_INTEGER := 7;
  gc_result_replace           CONSTANT PLS_INTEGER := 8;
  gc_result_rename            CONSTANT PLS_INTEGER := 9;
  gc_result_drop              CONSTANT PLS_INTEGER := 10;
  gc_result_cas_from_itself   CONSTANT PLS_INTEGER := 11;
  
  
  
  TYPE gt_index_statement_rec IS RECORD(
    index_owner      arrays.gt_name,
    index_name       arrays.gt_name,
    change_type      pls_integer,
    frwd_ddl_arr     arrays.gt_clob_arr,
    rlbk_ddl_arr     arrays.gt_clob_arr
  );
  
  TYPE gt_index_statement_arr IS TABLE OF gt_index_statement_rec INDEX BY PLS_INTEGER;

  -- return string name assosiated with integer constant
  FUNCTION get_result_name(in_result_code IN PLS_INTEGER) RETURN VARCHAR2;

  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_int_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ','
  )
  RETURN VARCHAR2;

  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_name_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ',',
    in_enclose_char IN VARCHAR2 DEFAULT '"'
  )
  RETURN VARCHAR2;

  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_lstr_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ',',
    in_enclose_char IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2;
  
  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_xlstr_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ',',
    in_enclose_char IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2;
  
  -- returns 32 byte length HASH sting
  FUNCTION get_hash_value(in_value IN VARCHAR2)
  RETURN VARCHAR2;

  -- Compares string arrays and return 0 if they identical, otherwise return 1
  FUNCTION comp_array(
    in_source_arr IN arrays.gt_name_arr,
    in_target_arr IN arrays.gt_name_arr
  )
  RETURN PLS_INTEGER;

  -- Compares string arrays and return 0 if they identical, otherwise return 1
  FUNCTION comp_array(
    in_source_arr IN arrays.gt_str_arr,
    in_target_arr IN arrays.gt_str_arr
  )
  RETURN PLS_INTEGER;

  -- add forward and rollback statements to arrays
  PROCEDURE add_stmt(
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    in_frwd_stmt     IN CLOB,
    in_rlbk_stmt     IN CLOB
  );

  -- return column data type clause
  FUNCTION get_column_type_clause(
    in_column_rec IN cort_exec_pkg.gt_column_rec
  )
  RETURN VARCHAR2;

  -- compares two structures of tables.  
  FUNCTION comp_tables(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec, 
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec, 
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- compares column name dependent table attributes 
  FUNCTION comp_table_col_attrs(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec, 
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec 
  )
  RETURN PLS_INTEGER;
  
  -- compares two arrays of table columns. Puts into out_com_result: 
  FUNCTION comp_table_columns(
    io_source_table_rec    IN OUT NOCOPY cort_exec_pkg.gt_table_rec, 
    io_target_table_rec    IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- compares index  
  FUNCTION comp_index(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_source_index_rec    IN OUT NOCOPY cort_exec_pkg.gt_index_rec,
    io_target_index_rec    IN OUT NOCOPY cort_exec_pkg.gt_index_rec,
    in_comp_phys_attr      IN BOOLEAN,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- compare all indexes (exception PK/UK) on table
  FUNCTION comp_indexes(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- builds CREATE INDEX statements to add index
  PROCEDURE create_index(
    in_index_rec             IN cort_exec_pkg.gt_index_rec,
    in_table_name            IN VARCHAR2,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );
  
  -- builds DROP INDEX ALTER statements to drop index
  PROCEDURE drop_index(
    in_index_rec             IN cort_exec_pkg.gt_index_rec,
    in_table_name            IN VARCHAR2,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );
  
  -- copies table indexes
  PROCEDURE copy_indexes(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- rollback alter statements
    in_copy_pk_uk            IN BOOLEAN DEFAULT FALSE -- copy indexes for PK/UK
  );
  
  -- compare all constraints
  FUNCTION comp_constraints(
    io_source_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- copy all references from source table to the target one
  PROCEDURE copy_references(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- returns DDL to drop renamed ref and 2 step rollback: create without validation + validation
  PROCEDURE drop_references(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- compare all supplemental logs
  FUNCTION comp_log_groups(
    io_source_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- compare lob attributes
  FUNCTION comp_lobs(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;
  
  -- compare xml columns
  FUNCTION comp_xml_columns(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- compare varray columns
  FUNCTION comp_varray_columns(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- compare partitions/subpartitions
  FUNCTION comp_partitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr, 
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr, 
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;
  
  -- return privileges statements
  PROCEDURE get_privileges_stmt(
    in_privilege_arr         IN cort_exec_pkg.gt_privilege_arr,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- copies sequence privileges
  PROCEDURE copy_privileges(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );
  
  -- updates trigger
  PROCEDURE update_trigger(
    in_table_rec             IN cort_exec_pkg.gt_table_rec,
    in_trigger_rec           IN cort_exec_pkg.gt_trigger_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- drops table triggers
  PROCEDURE drop_triggers(
    in_table_rec             IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- copies table triggers
  PROCEDURE copy_triggers(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );
  
  -- copies table policies
  PROCEDURE copy_policies(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- Returns 1 if same partitiong used, for same columns/ref_constraint and partitions could be preserved. Otherwise returns 0
  FUNCTION comp_partitioning(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_check_position   IN BOOLEAN DEFAULT TRUE
  )
  RETURN PLS_INTEGER;
  
  -- Returns 1 if same partitiong used, for same columns/ref_constraint and partitions could be preserved. Otherwise returns 0
  FUNCTION comp_subpartitioning(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_check_position   IN BOOLEAN DEFAULT TRUE
  )
  RETURN PLS_INTEGER;

  -- returns list of individual partitions clauses
  FUNCTION get_partitions_sql(
    in_partition_arr    IN cort_exec_pkg.gt_partition_arr,
    in_subpartition_arr IN cort_exec_pkg.gt_partition_arr
  )
  RETURN CLOB;
  
  -- Returns TRUE if it's possible to create subpartitioning for SWAP table, otherwise - FALSE
  FUNCTION is_subpartitioning_available(
    in_table_rec IN cort_exec_pkg.gt_table_rec
  )
  RETURN BOOLEAN;

  -- Returns first column suitable for partitioning 
  FUNCTION get_column_for_partitioning(
    in_table_rec    IN cort_exec_pkg.gt_table_rec
  )
  RETURN VARCHAR2;

  -- create swap table for given table (and partition - optionaly)
  PROCEDURE create_swap_table_sql(
    in_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_swap_table_rec IN cort_exec_pkg.gt_table_rec,
    io_frwd_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );
  
  -- create clone table for given table
  PROCEDURE create_clone_table_sql(
    in_table_rec       IN cort_exec_pkg.gt_table_rec,
    in_simple_mode     IN BOOLEAN DEFAULT FALSE,
    in_rename_mode     IN VARCHAR2 DEFAULT 'TO_TEMP',
    in_all_partitions  IN BOOLEAN DEFAULT TRUE,
    io_frwd_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- exchanges source table partition (in_partition_name) with target table
  PROCEDURE exchange_partition(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_partition_rec    IN cort_exec_pkg.gt_partition_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );

  -- Returns list of column name for INSERT statement and list of column values for SELECT statement from source table
  PROCEDURE get_column_values_list(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    out_columns_list    OUT NOCOPY CLOB,
    out_values_list     OUT NOCOPY CLOB
  );
  
  -- rename object
  PROCEDURE rename_object(
    in_rename_mode   IN VARCHAR2,
    io_rename_rec    IN OUT NOCOPY cort_exec_pkg.gt_rename_rec,
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );
  
  -- compares two structures of sequences.  
  FUNCTION comp_sequences(
    in_source_sequence_rec IN cort_exec_pkg.gt_sequence_rec, 
    in_target_sequence_rec IN cort_exec_pkg.gt_sequence_rec, 
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- return sequence create statement
  FUNCTION get_sequence_sql(
    in_sequence_rec in cort_exec_pkg.gt_sequence_rec
  )
  RETURN VARCHAR2;

  -- compares two structures of types.  
  FUNCTION comp_types(
    in_source_type_rec     IN cort_exec_pkg.gt_type_rec, 
    in_target_type_rec     IN cort_exec_pkg.gt_type_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  FUNCTION get_create_view_sql(in_view_rec IN cort_exec_pkg.gt_view_rec)
  RETURN CLOB;

END cort_comp_pkg;
/