CREATE OR REPLACE PACKAGE BODY cort_comp_pkg
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
  16.00   | Rustam Kafarov    | Added get_create_view_sql. Fixed bugs with TYPES
  19.00   | Rustam Kafarov    | Revised parameters
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2
  ----------------------------------------------------------------------------------------------------------------------
*/

  gc_tablespace              CONSTANT VARCHAR2(30) := 'TABLESPACE';
  gc_overflow_tablespace     CONSTANT VARCHAR2(30) := 'OVERFLOW TABLESPACE';
  gc_add_overflow            CONSTANT VARCHAR2(30) := 'ADD OVERFLOW';
  gc_overflow                CONSTANT VARCHAR2(30) := 'OVERFLOW';
  gc_physical_attr           CONSTANT VARCHAR2(30) := '<physical_attr>';
  gc_overflow_physical_attr  CONSTANT VARCHAR2(30) := '<overflow_physical_attr>';
  gc_pctfree                 CONSTANT VARCHAR2(30) := 'PCTFREE';
  gc_pctused                 CONSTANT VARCHAR2(30) := 'PCTUSED';
  gc_initrans                CONSTANT VARCHAR2(30) := 'INITRANS';
  gc_storage                 CONSTANT VARCHAR2(30) := 'STORAGE';
  gc_initial                 CONSTANT VARCHAR2(30) := 'INITIAL';
  gc_minextents              CONSTANT VARCHAR2(30) := 'MINEXTENTS';
--  gc_maxextents              CONSTANT VARCHAR2(30) := 'MAXEXTENTS';
  gc_maxsize                 CONSTANT VARCHAR2(30) := 'MAXSIZE';
  gc_pctincrease             CONSTANT VARCHAR2(30) := 'PCTINCREASE';
  gc_freelists               CONSTANT VARCHAR2(30) := 'FREELISTS';
  gc_freelist_groups         CONSTANT VARCHAR2(30) := 'FREELIST GROUPS';
  gc_buffer_pool             CONSTANT VARCHAR2(30) := 'BUFFER_POOL';
  gc_logging                 CONSTANT VARCHAR2(30) := '<logging>';
  gc_overflow_logging        CONSTANT VARCHAR2(30) := '<overflow_logging>';
  gc_compression             CONSTANT VARCHAR2(30) := '<compression>';
  gc_parallel                CONSTANT VARCHAR2(30) := 'PARALLEL';
  gc_degree                  CONSTANT VARCHAR2(30) := 'DEGREE';
  gc_instances               CONSTANT VARCHAR2(30) := 'INSTANCES';
  gc_cache                   CONSTANT VARCHAR2(30) := '<cache>';
  gc_row_movement            CONSTANT VARCHAR2(30) := '<row_movement>';
  gc_monitoring              CONSTANT VARCHAR2(30) := '<monitoring>';
  gc_pct_threshold           CONSTANT VARCHAR2(30) := 'PCTTHRESHOLD';
  gc_mapping                 CONSTANT VARCHAR2(30) := '<mapping>';
  gc_including_column        CONSTANT VARCHAR2(30) := '<including_column>';

  gc_rename_column           CONSTANT VARCHAR2(30) := 'RENAME COLUMN';
  gc_add_column              CONSTANT VARCHAR2(30) := 'ADD';
  gc_drop_column             CONSTANT VARCHAR2(30) := 'DROP COLUMN';
  gc_modify_column           CONSTANT VARCHAR2(30) := 'MODIFY';
  gc_column_name             CONSTANT VARCHAR2(30) := '<column_name>';
  gc_data_type               CONSTANT VARCHAR2(30) := '<data_type>';
  gc_default                 CONSTANT VARCHAR2(30) := 'DEFAULT';
  gc_char_used               CONSTANT VARCHAR2(30) := '<char_used>';
  gc_encryption              CONSTANT VARCHAR2(30) := '<encryption>';
  gc_salt                    CONSTANT VARCHAR2(30) := '<salt>';
  gc_generated_always_as     CONSTANT VARCHAR2(30) := 'GENERATED ALWAYS AS';
  gc_nullable                CONSTANT VARCHAR2(30) := '<nullable>';
  gc_column_properties       CONSTANT VARCHAR2(30) := '<gc_column_properties>';
  gc_set_unused_column       CONSTANT VARCHAR2(30) := 'SET UNUSED COLUMN';
  gc_cascade_constraints     CONSTANT VARCHAR2(30) := 'CASCADE CONSTRAINTS';

  gc_add_supplemental_log    CONSTANT VARCHAR2(30) := 'ADD SUPPLEMENTAL LOG';
  gc_drop_supplemental_log   CONSTANT VARCHAR2(30) := 'DROP SUPPLEMENTAL LOG';
  gc_log_group               CONSTANT VARCHAR2(30) := 'GROUP';
  gc_log_columns             CONSTANT VARCHAR2(30) := '<log_columns>';
  gc_log_always              CONSTANT VARCHAR2(30) := '<always>';
  gc_log_data                CONSTANT VARCHAR2(30) := 'DATA';

--  gc_create_index            CONSTANT VARCHAR2(30) := '<create_index>';
  gc_create                  CONSTANT VARCHAR2(30) := 'CREATE';
  gc_drop                    CONSTANT VARCHAR2(30) := 'DROP';
  gc_index                   CONSTANT VARCHAR2(30) := 'INDEX';
  gc_alter_index             CONSTANT VARCHAR2(30) := 'ALTER INDEX';
  gc_rename_to               CONSTANT VARCHAR2(30) := 'RENAME TO';
  gc_on_table                CONSTANT VARCHAR2(30) := 'ON';
  gc_index_columns           CONSTANT VARCHAR2(30) := '<gc_index_columns>';
  gc_indextype               CONSTANT VARCHAR2(30) := 'INDEXTYPE';
  gc_parameters              CONSTANT VARCHAR2(30) := 'PARAMETERS';
  gc_from_clause             CONSTANT VARCHAR2(30) := 'FROM';
  gc_where_clause            CONSTANT VARCHAR2(30) := 'WHERE';
  gc_locality                CONSTANT VARCHAR2(30) := '<locality>';
  gc_reverse                 CONSTANT VARCHAR2(30) := 'REVERSE';
  gc_key_compression         CONSTANT VARCHAR2(30) := '<key_compression>';
  gc_indexing                CONSTANT VARCHAR2(30) := 'INDEXING';

  gc_constraint              CONSTANT VARCHAR2(30) := 'CONSTRAINT';
  gc_rename_constraint       CONSTANT VARCHAR2(30) := 'RENAME CONSTRAINT';
  gc_add_constraint          CONSTANT VARCHAR2(30) := 'ADD CONSTRAINT';
  gc_drop_constraint         CONSTANT VARCHAR2(30) := 'DROP CONSTRAINT';
  gc_modify_constraint       CONSTANT VARCHAR2(30) := 'MODIFY CONSTRAINT';
  gc_constraint_name         CONSTANT VARCHAR2(30) := '<constraint_name>';
  gc_constraint_type         CONSTANT VARCHAR2(30) := '<constraint_type>';
  gc_constraint_columns      CONSTANT VARCHAR2(30) := '<constraint_columns>';
  gc_references              CONSTANT VARCHAR2(30) := 'REFERENCES';
  gc_ref_columns             CONSTANT VARCHAR2(30) := '<ref_columns>';
  gc_delete_rule             CONSTANT VARCHAR2(30) := '<delete_rule>';
  gc_deferrable              CONSTANT VARCHAR2(30) := '<deferrable>';
  gc_deferred                CONSTANT VARCHAR2(30) := 'INITIALLY';
  gc_constraint_index        CONSTANT VARCHAR2(30) := 'USING INDEX';
  gc_status                  CONSTANT VARCHAR2(30) := '<status>';
  gc_validated               CONSTANT VARCHAR2(30) := '<validated>';

  gc_interval                CONSTANT VARCHAR2(30) := 'SET INTERVAL';
  gc_modify_default_attrs    CONSTANT VARCHAR2(30) := 'MODIFY DEFAULT ATTRIBUTES';
  gc_move                    CONSTANT VARCHAR2(30) := 'MOVE ';
  gc_move_partition          CONSTANT VARCHAR2(30) := 'MOVE PARTITION';
  gc_move_subpartition       CONSTANT VARCHAR2(30) := 'MOVE SUBPARTITION';
  gc_rename                  CONSTANT VARCHAR2(30) := 'RENAME ';
  gc_rename_partition        CONSTANT VARCHAR2(30) := 'RENAME PARTITION';
  gc_rename_subpartition     CONSTANT VARCHAR2(30) := 'RENAME SUBPARTITION';
  gc_modify                  CONSTANT VARCHAR2(30) := 'MODIFY ';
  gc_modify_partition        CONSTANT VARCHAR2(30) := 'MODIFY PARTITION';
  gc_modify_subpartition     CONSTANT VARCHAR2(30) := 'MODIFY SUBPARTITION';
  gc_coalesce_partition      CONSTANT VARCHAR2(30) := 'COALESCE PARTITION';
  gc_coalesce_subpartition   CONSTANT VARCHAR2(30) := 'COALESCE SUBPARTITION';
  gc_add_partition           CONSTANT VARCHAR2(30) := 'ADD PARTITION';
  gc_add_subpartition        CONSTANT VARCHAR2(30) := 'ADD SUBPARTITION';
  gc_drop_partition          CONSTANT VARCHAR2(30) := 'DROP PARTITION';
  gc_drop_subpartition       CONSTANT VARCHAR2(30) := 'DROP SUBPARTITION';
  gc_subpartitions           CONSTANT VARCHAR2(30) := '<subpartitions>';
  gc_list_values             CONSTANT VARCHAR2(30) := 'VALUES';
  gc_range_values            CONSTANT VARCHAR2(30) := 'VALUES LESS THAN';
  gc_visible                 CONSTANT VARCHAR2(30) := 'VISIBLE';
  gc_invisible               CONSTANT VARCHAR2(30) := 'INVISIBLE';

  gc_lob_item                CONSTANT VARCHAR2(30) := 'LOB';
  gc_lob_params              CONSTANT VARCHAR2(30) := '<lob_params>';
  gc_lob_name                CONSTANT VARCHAR2(30) := '<lob_name>';
  gc_modify_lob_item         CONSTANT VARCHAR2(30) := 'MODIFY LOB';
  gc_securefile              CONSTANT VARCHAR2(30) := '<securefile>';
  gc_chunk                   CONSTANT VARCHAR2(30) := 'CHUNK';
  gc_pctversion              CONSTANT VARCHAR2(30) := 'PCTVERSION';
  gc_freepools               CONSTANT VARCHAR2(30) := 'FREEPOOLS';
  gc_deduplication           CONSTANT VARCHAR2(30) := '<deduplication>';
  gc_lob_compression         CONSTANT VARCHAR2(30) := '<lob_compression>';
  gc_encrypt                 CONSTANT VARCHAR2(30) := '<encrypt>';
  gc_in_row                  CONSTANT VARCHAR2(30) := '<in_row>';
  gc_retention               CONSTANT VARCHAR2(30) := 'RETENTION';
  gc_store_as                CONSTANT VARCHAR2(30) := 'STORE AS';

  gc_xml_type_column         CONSTANT VARCHAR2(30) := 'XMLTYPE COLUMN';
  gc_xml_storage_type        CONSTANT VARCHAR2(30) := '<xml_storage_type>';
  gc_xml_schema              CONSTANT VARCHAR2(30) := 'XMLSCHEMA';
  gc_xml_element             CONSTANT VARCHAR2(30) := 'ELEMENT';
  gc_xml_anyschema           CONSTANT VARCHAR2(30) := '<xml_anyschema>';
  gc_xml_nonschema           CONSTANT VARCHAR2(30) := '<xml_nonschema>';

  gc_varray                  CONSTANT VARCHAR2(30) := 'VARRAY';
  gc_substitution_column     CONSTANT VARCHAR2(30) := '<substitution_column>';

  gc_grant_privilege         CONSTANT VARCHAR2(30) := 'GRANT';
  gc_revoke_privilege        CONSTANT VARCHAR2(30) := 'REVOKE';
  gc_privilege_column        CONSTANT VARCHAR2(30) := '<privilege_column>';
  gc_to                      CONSTANT VARCHAR2(30) := 'TO';
  gc_from                    CONSTANT VARCHAR2(30) := 'FROM';
  gc_hierarchy_option        CONSTANT VARCHAR2(30) := 'WITH HIERARCHY OPTION';
  gc_grant_option            CONSTANT VARCHAR2(30) := 'WITH GRANT OPTION';

  gc_create_trigger          CONSTANT VARCHAR2(30) := 'CREATE OR REPLACE TRIGGER';
  gc_drop_trigger            CONSTANT VARCHAR2(30) := 'DROP TRIGGER';
  gc_trigger_type            CONSTANT VARCHAR2(30) := '<trigger_type>';
  gc_triggerring_event       CONSTANT VARCHAR2(30) := '<triggerring_event>';
  gc_referencing_names       CONSTANT VARCHAR2(30) := '<referencing_names>';
  gc_for_each_row            CONSTANT VARCHAR2(30) := '<for_each_row>';
  gc_follows                 CONSTANT VARCHAR2(30) := 'FOLLOWS';
  gc_when                    CONSTANT VARCHAR2(30) := 'WHEN';

  gc_min_value               CONSTANT VARCHAR2(30) := 'MINVALUE';
  gc_max_value               CONSTANT VARCHAR2(30) := 'MAXVALUE';
  gc_increment_by            CONSTANT VARCHAR2(30) := 'INCREMENT BY';
  gc_cycle_flag              CONSTANT VARCHAR2(30) := '<cycle_flag>';
  gc_order_flag              CONSTANT VARCHAR2(30) := '<order_flag>';
  gc_cache_size              CONSTANT VARCHAR2(30) := '<cache_size>';

  gc_final                   CONSTANT VARCHAR2(30) := 'FINAL';
  gc_instantiable            CONSTANT VARCHAR2(30) := 'INSTANTIABLE';

  TYPE gt_type_matrix IS TABLE OF arrays.gt_mstr_indx INDEX BY VARCHAR2(255);

  g_type_matrix              gt_type_matrix;

  -- array of indexes indexed by hash value
  TYPE gt_hash_indx_arr     IS TABLE OF arrays.gt_int_arr INDEX BY VARCHAR2(255); -- by hash value
  TYPE gt_hash_val_indx_arr IS TABLE OF arrays.gt_num_arr INDEX BY VARCHAR2(255); -- by hash value


  PROCEDURE debug(
    in_text    IN CLOB,
    in_details IN CLOB DEFAULT NULL
  )
  AS
  BEGIN
    cort_exec_pkg.debug(in_text, in_details);
  END debug;

  FUNCTION empty_partition_arr
  RETURN cort_exec_pkg.gt_partition_arr
  AS
    l_empty_partition_arr cort_exec_pkg.gt_partition_arr;
  BEGIN
    RETURN l_empty_partition_arr;
  END empty_partition_arr;


  -- return string name assosiated with integer constant
  FUNCTION get_result_name(in_result_code IN PLS_INTEGER) RETURN VARCHAR2
  AS
  BEGIN
    RETURN CASE in_result_code
             WHEN gc_result_nochange           THEN 'no_change'
             WHEN gc_result_alter              THEN 'alter'
             WHEN gc_result_alter_move         THEN 'alter_move'
             WHEN gc_result_exchange           THEN 'exchange'
             WHEN gc_result_part_exchange      THEN 'part_exchange'
             WHEN gc_result_recreate           THEN 'recreate'
             WHEN gc_result_create_as_select   THEN 'create_as_select'
             WHEN gc_result_create             THEN 'create'
             WHEN gc_result_replace            THEN 'replace'
             WHEN gc_result_rename             THEN 'rename'
             WHEN gc_result_drop               THEN 'drop'
             WHEN gc_result_cas_from_itself    THEN 'create_as_select_from_itself'
             ELSE to_char(in_result_code)
           END;
  END get_result_name;

  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_int_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ','
  )
  RETURN VARCHAR2
  AS
    l_indx   PLS_INTEGER;
    l_result VARCHAR2(32767);
  BEGIN
    l_result := NULL;
    l_indx := in_value_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      l_result := l_result || in_value_arr(l_indx);
      l_indx := in_value_arr.NEXT(l_indx);
      EXIT WHEN l_indx IS NULL;
      l_result := l_result || in_sep_char;
    END LOOP;
    RETURN l_result;
  END convert_arr_to_str;

  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_name_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ',',
    in_enclose_char IN VARCHAR2 DEFAULT '"'
  )
  RETURN VARCHAR2
  AS
    l_indx   PLS_INTEGER;
    l_result VARCHAR2(32767);
  BEGIN
    l_result := NULL;
    l_indx := in_value_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      l_result := l_result || in_enclose_char||in_value_arr(l_indx)||in_enclose_char;
      l_indx := in_value_arr.NEXT(l_indx);
      EXIT WHEN l_indx IS NULL;
      l_result := l_result || in_sep_char;
    END LOOP;
    RETURN l_result;
  END convert_arr_to_str;

  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_lstr_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ',',
    in_enclose_char IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_indx   PLS_INTEGER;
    l_result VARCHAR2(32767);
  BEGIN
    l_result := NULL;
    l_indx := in_value_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      l_result := l_result || in_enclose_char||in_value_arr(l_indx)||in_enclose_char;
      l_indx := in_value_arr.NEXT(l_indx);
      EXIT WHEN l_indx IS NULL;
      l_result := l_result || in_sep_char;
    END LOOP;
    RETURN l_result;
  END convert_arr_to_str;

  FUNCTION convert_arr_to_str(
    in_value_arr    IN arrays.gt_xlstr_arr,
    in_sep_char     IN VARCHAR2 DEFAULT ',',
    in_enclose_char IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_indx   PLS_INTEGER;
    l_result VARCHAR2(32767);
  BEGIN
    l_result := NULL;
    l_indx := in_value_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      l_result := l_result || in_enclose_char||in_value_arr(l_indx)||in_enclose_char;
      l_indx := in_value_arr.NEXT(l_indx);
      EXIT WHEN l_indx IS NULL;
      l_result := l_result || in_sep_char;
    END LOOP;
    RETURN l_result;
  END convert_arr_to_str;

  FUNCTION reverse(in_string IN VARCHAR2)
  RETURN VARCHAR2
  AS
    l_string VARCHAR2(32767);
  BEGIN
    FOR i IN 1..LENGTH(in_string) LOOP
      l_string := substr(in_string,i,1)||l_string;
    END LOOP;
    RETURN l_string;
  END reverse;

  -- returns 32 byte length HASH sting
  FUNCTION get_hash_value(in_value IN VARCHAR2)
  RETURN VARCHAR2
  AS
  BEGIN
    IF LENGTH(in_value) > 255 THEN
      RETURN dbms_obfuscation_toolkit.MD5(input => utl_raw.cast_to_raw(in_value))||
             dbms_obfuscation_toolkit.MD5(input => utl_raw.cast_to_raw(reverse(in_value)))||
             dbms_obfuscation_toolkit.MD5(input => utl_raw.cast_to_raw(LOWER(in_value)))||
             dbms_obfuscation_toolkit.MD5(input => utl_raw.cast_to_raw(reverse(UPPER(in_value))));
    ELSE
      RETURN in_value;
    END IF;
  END get_hash_value;

  -- compares 2 simple values. Returns 0 if they are identical, otherwise 1
  FUNCTION comp_value(in_val1 IN VARCHAR2, in_val2 in VARCHAR2)
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF (in_val1 = in_val2) OR (in_val1 IS NULL AND in_val2 IS NULL) THEN
      RETURN 0;
    ELSE
      RETURN 1;
    END IF;
  END comp_value;

  FUNCTION comp_value(in_val1 IN NUMBER, in_val2 in NUMBER)
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF (in_val1 = in_val2) OR (in_val1 IS NULL AND in_val2 IS NULL) THEN
      RETURN 0;
    ELSE
      RETURN 1;
    END IF;
  END comp_value;

  FUNCTION comp_value(in_val1 IN DATE, in_val2 in DATE)
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF (in_val1 = in_val2) OR (in_val1 IS NULL AND in_val2 IS NULL) THEN
      RETURN 0;
    ELSE
      RETURN 1;
    END IF;
  END comp_value;

  -- Compares string arrays and return 0 if they identical, otherwise return 1
  FUNCTION comp_array(
    in_source_arr IN arrays.gt_name_arr,
    in_target_arr IN arrays.gt_name_arr
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    -- number of elements must be the same
    IF in_source_arr.COUNT <> in_target_arr.COUNT THEN
      RETURN 1;
    END IF;
    -- loop for every element from 1 position. Check also that array is dense.
    FOR i IN 1..in_source_arr.COUNT LOOP
      IF in_source_arr.EXISTS(i) AND
         in_target_arr.EXISTS(i) AND
         comp_value(in_source_arr(i), in_target_arr(i)) = 0 THEN
        NULL;
      ELSE
        -- not natch. Return 1, interrupt check
        RETURN 1;
      END IF;
    END LOOP;
    -- if reach to this posint then arrays are identical
    RETURN 0;
  END comp_array;

  -- Compares string arrays and return 0 if they identical, otherwise return 1
  FUNCTION comp_array(
    in_source_arr IN arrays.gt_str_arr,
    in_target_arr IN arrays.gt_str_arr
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    -- number of elements must be the same
    IF in_source_arr.COUNT <> in_target_arr.COUNT THEN
      RETURN 1;
    END IF;
    -- loop for every element from 1 position. Check also that array is dense.
    FOR i IN 1..in_source_arr.COUNT LOOP
      IF in_source_arr.EXISTS(i) AND
         in_target_arr.EXISTS(i) AND
         comp_value(in_source_arr(i), in_target_arr(i)) = 0 THEN
        NULL;
      ELSE
        -- not natch. Return 1, interrupt check
        RETURN 1;
      END IF;
    END LOOP;
    -- if reach to this posint then arrays are identical
    RETURN 0;
  END comp_array;

  -- Compares string arrays and return 0 if they identical, otherwise return 1
  FUNCTION comp_array(
    in_source_arr IN arrays.gt_lstr_arr,
    in_target_arr IN arrays.gt_lstr_arr
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    -- number of elements must be the same
    IF in_source_arr.COUNT <> in_target_arr.COUNT THEN
      RETURN 1;
    END IF;
    -- loop for every element from 1 position. Check also that array is dense.
    FOR i IN 1..in_source_arr.COUNT LOOP
      IF in_source_arr.EXISTS(i) AND
         in_target_arr.EXISTS(i) AND
         comp_value(in_source_arr(i), in_target_arr(i)) = 0 THEN
        NULL;
      ELSE
        -- not natch. Return 1, interrupt check
        RETURN 1;
      END IF;
    END LOOP;
    -- if reach to this posint then arrays are identical
    RETURN 0;
  END comp_array;

  -- Compares string arrays and return 0 if they identical, otherwise return 1
  FUNCTION comp_array(
    in_source_arr IN arrays.gt_xlstr_arr,
    in_target_arr IN arrays.gt_xlstr_arr
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    -- number of elements must be the same
    IF in_source_arr.COUNT <> in_target_arr.COUNT THEN
      RETURN 1;
    END IF;
    -- loop for every element from 1 position. Check also that array is dense.
    FOR i IN 1..in_source_arr.COUNT LOOP
      IF in_source_arr.EXISTS(i) AND
         in_target_arr.EXISTS(i) AND
         comp_value(in_source_arr(i), in_target_arr(i)) = 0 THEN
        NULL;
      ELSE
        -- not natch. Return 1, interrupt check
        RETURN 1;
      END IF;
    END LOOP;
    -- if reach to this posint then arrays are identical
    RETURN 0;
  END comp_array;

  FUNCTION comp_array(
    in_source_arr IN arrays.gt_int_arr,
    in_target_arr IN arrays.gt_int_arr
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    -- number of elements must be the same
    IF in_source_arr.COUNT <> in_target_arr.COUNT THEN
      RETURN 1;
    END IF;
    -- loop for every element from 1 position. Check also that array is dense.
    FOR i IN 1..in_source_arr.COUNT LOOP
      IF in_source_arr.EXISTS(i) AND
         in_target_arr.EXISTS(i) AND
         comp_value(in_source_arr(i), in_target_arr(i)) = 0 THEN
        NULL;
      ELSE
        -- not natch. Return 1, interrupt check
        RETURN 1;
      END IF;
    END LOOP;
    -- if reach to this posint then arrays are identical
    RETURN 0;
  END comp_array;

  -- Compares columns data types
  FUNCTION comp_data_type(
    in_source_column_rec   IN cort_exec_pkg.gt_column_rec,
    in_target_column_rec   IN cort_exec_pkg.gt_column_rec
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF comp_value(in_source_column_rec.data_type, in_target_column_rec.data_type) = 1 OR
       comp_value(in_source_column_rec.data_type_mod, in_target_column_rec.data_type_mod) = 1 OR
       comp_value(in_source_column_rec.data_type_owner, in_target_column_rec.data_type_owner) = 1
    THEN
      debug('comp_data_type: '||in_source_column_rec.column_name||' vs '||in_target_column_rec.column_name||'
        data type: '||in_source_column_rec.data_type||' - '||in_target_column_rec.data_type||'
        data_type_mod: '||in_source_column_rec.data_type_mod||' - '||in_target_column_rec.data_type_mod||'
        data_type_owner: '||in_source_column_rec.data_type_owner||' - '||in_target_column_rec.data_type_owner);
      RETURN 1;
    ELSIF in_source_column_rec.data_type NOT IN ('CLOB','NCLOB','BLOB','LONG','LONG RAW') AND
          (comp_value(in_source_column_rec.data_length, in_target_column_rec.data_length) = 1 OR
           comp_value(in_source_column_rec.data_precision, in_target_column_rec.data_precision) = 1 OR
           comp_value(in_source_column_rec.data_scale, in_target_column_rec.data_scale) = 1)
    THEN
      debug('comp_data_type: '||in_source_column_rec.column_name||' vs '||in_target_column_rec.column_name||'
      data_length: '||in_source_column_rec.data_length||' - '||in_target_column_rec.data_length||'
      data_precision: '||in_source_column_rec.data_precision||' - '||in_target_column_rec.data_precision||'
      data_scale: '||in_source_column_rec.data_scale||' - '||in_target_column_rec.data_scale);
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END comp_data_type;

  -- compares array of column names
  FUNCTION comp_column_arr(
    in_source_table_rec  IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec  IN cort_exec_pkg.gt_table_rec,
    in_source_column_arr IN arrays.gt_name_arr,
    in_target_column_arr IN arrays.gt_name_arr,
    in_check_position    IN BOOLEAN DEFAULT TRUE
  )
  RETURN PLS_INTEGER
  AS
    l_indx              PLS_INTEGER;
    l_source_column_rec cort_exec_pkg.gt_column_rec;
    l_target_column_rec cort_exec_pkg.gt_column_rec;
  BEGIN
    debug('Compare column names');
    -- number of elements must be the same
    IF in_source_column_arr.COUNT <> in_target_column_arr.COUNT THEN
      debug('columns count mismatch');
      RETURN 1;
    END IF;
    -- loop for every element from 1 position. Check also that array is dense.
    FOR i IN 1..in_source_column_arr.COUNT LOOP
      --
      IF in_source_column_arr.EXISTS(i) THEN
        debug('source column  - '||in_source_column_arr(i));
      ELSE
        debug('no source column specified on position '||i);
      END IF;

      IF in_target_column_arr.EXISTS(i) THEN
        debug('target column  - '||in_target_column_arr(i));
      ELSE
        debug('no target column specified on position '||i);
      END IF;
      IF in_source_column_arr.EXISTS(i) AND
         in_target_column_arr.EXISTS(i) THEN
        IF in_source_table_rec.column_indx_arr.EXISTS(in_source_column_arr(i)) THEN
          l_indx := in_source_table_rec.column_indx_arr(in_source_column_arr(i));
          l_source_column_rec := in_source_table_rec.column_arr(l_indx);
          debug('source column found - '||l_source_column_rec.column_name);
        ELSE
          RETURN 1;
        END IF;
        IF in_target_table_rec.column_indx_arr.EXISTS(in_target_column_arr(i)) THEN
          l_indx := in_target_table_rec.column_indx_arr(in_target_column_arr(i));
          l_target_column_rec := in_target_table_rec.column_arr(l_indx);
          debug('target column found - '||l_target_column_rec.column_name);
        ELSE
          RETURN 1;
        END IF;
        IF comp_data_type(l_source_column_rec, l_target_column_rec) = 1 THEN
          debug('column types do not match each other');
          RETURN 1;
        END IF;
        IF in_check_position THEN
          IF l_source_column_rec.matched_column_id = l_target_column_rec.column_id AND
             l_source_column_rec.column_id = l_target_column_rec.matched_column_id
          THEN
            debug('columns positions match each other');
          ELSE
            debug('source column_id/matched_column_id = '||l_source_column_rec.column_id||'/'||l_source_column_rec.matched_column_id);
            debug('target column_id/matched_column_id = '||l_target_column_rec.column_id||'/'||l_target_column_rec.matched_column_id);
            RETURN 1;
          END IF;
        END IF;
          
/*
        IF l_source_column_rec.virtual_column = l_target_column_rec.virtual_column
        THEN
          IF l_target_column_rec.virtual_column = 'NO' THEN
            -- for segment columns check that column name, data type and position are the same
            IF comp_value(NVL(l_source_column_rec.new_column_name,l_source_column_rec.column_name), l_target_column_rec.column_name) = 0
            THEN
              IF in_check_type THEN
                IF comp_data_type(l_source_column_rec, l_target_column_rec) = 1 THEN
                  RETURN 1;
                END IF;
              END IF;
            ELSE
              RETURN 1;
            END IF;
          ELSIF l_target_column_rec.virtual_column = 'YES' THEN
            -- for virtual columns check that expression is the same
            IF l_source_column_rec.data_default = l_target_column_rec.data_default
            THEN
              NULL;
            ELSE
              RETURN 1;
            END IF;
          END IF;
        ELSE
          RETURN 1;
        END IF;
*/
      ELSE
        -- not natch. Return 1, interrupt check
        RETURN 1;
      END IF;

    END LOOP;
    -- if reach to this posint then arrays are identical
    RETURN 0;
  END comp_column_arr;

  -- compares REF constraints by their names
  FUNCTION comp_ref_ptn_constraint(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec
  )
  RETURN NUMBER
  AS
    l_indx                    PLS_INTEGER;
    l_source_constraint_rec   cort_exec_pkg.gt_constraint_rec;
    l_target_constraint_rec   cort_exec_pkg.gt_constraint_rec;
  BEGIN
    IF in_source_table_rec.ref_ptn_constraint_name IS NULL AND
       in_target_table_rec.ref_ptn_constraint_name IS NULL
    THEN
      RETURN 0;
    END IF;

    IF in_source_table_rec.constraint_indx_arr.EXISTS(in_source_table_rec.ref_ptn_constraint_name) THEN
      l_indx := in_source_table_rec.constraint_indx_arr(in_source_table_rec.ref_ptn_constraint_name);
      l_source_constraint_rec := in_source_table_rec.constraint_arr(l_indx);
    ELSE
      RETURN 1;
    END IF;
    IF in_target_table_rec.constraint_indx_arr.EXISTS(in_target_table_rec.ref_ptn_constraint_name) THEN
      l_indx := in_target_table_rec.constraint_indx_arr(in_target_table_rec.ref_ptn_constraint_name);
      l_target_constraint_rec := in_target_table_rec.constraint_arr(l_indx);
    ELSE
      RETURN 1;
    END IF;
    IF l_source_constraint_rec.constraint_type = l_target_constraint_rec.constraint_type AND
       l_source_constraint_rec.constraint_type = 'R'
    THEN
      IF l_source_constraint_rec.r_table_name = l_target_constraint_rec.r_table_name AND
         l_source_constraint_rec.r_owner = l_target_constraint_rec.r_owner AND
         l_source_constraint_rec.r_constraint_name = l_target_constraint_rec.r_constraint_name AND
         comp_array(l_source_constraint_rec.column_arr, l_target_constraint_rec.column_arr) = 0
      THEN
        RETURN 0;
      ELSE
        RETURN 1;
      END IF;
    ELSE
      RETURN 1;
    END IF;
  END comp_ref_ptn_constraint;

  -- Returns 1 if same partitiong used, for same columns/ref_constraint and partitions could be preserved. Otherwise returns 0
  FUNCTION comp_partitioning(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_check_position   IN BOOLEAN DEFAULT TRUE
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF in_source_table_rec.partitioning_type = in_target_table_rec.partitioning_type THEN
      CASE in_source_table_rec.partitioning_type
      WHEN 'SYSTEM' THEN
        RETURN 0;
      WHEN 'REFERENCE' THEN
        RETURN comp_ref_ptn_constraint(
                 in_source_table_rec  => in_source_table_rec,
                 in_target_table_rec  => in_target_table_rec
               );
      ELSE
        RETURN comp_column_arr(
                 in_source_table_rec  => in_source_table_rec,
                 in_target_table_rec  => in_target_table_rec,
                 in_source_column_arr => in_source_table_rec.part_key_column_arr,
                 in_target_column_arr => in_target_table_rec.part_key_column_arr,
                 in_check_position    => in_check_position
               );
      END CASE;
    ELSE
      RETURN 1;
    END IF;
  END comp_partitioning;

  -- Returns 1 if same partitiong used, for same columns/ref_constraint and partitions could be preserved. Otherwise returns 0
  FUNCTION comp_subpartitioning(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_check_position   IN BOOLEAN DEFAULT TRUE
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF in_source_table_rec.subpartitioning_type = in_target_table_rec.subpartitioning_type THEN
      RETURN comp_column_arr(
               in_source_table_rec  => in_source_table_rec,
               in_target_table_rec  => in_target_table_rec,
               in_source_column_arr => in_source_table_rec.subpart_key_column_arr, 
               in_target_column_arr => in_target_table_rec.subpart_key_column_arr,
               in_check_position    => in_check_position
             );
    ELSE
      RETURN 1;
    END IF;
  END comp_subpartitioning;

  FUNCTION get_partitioning_option
  RETURN v$option.value%TYPE
  AS
    l_partitioning_option v$option.value%TYPE;
  BEGIN
    BEGIN
      SELECT value
        INTO l_partitioning_option
        FROM v$option
       WHERE parameter = 'Partitioning';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_partitioning_option := 'TRUE';
    END;
    RETURN l_partitioning_option;
  END get_partitioning_option;

  -- checks if it's possible to create partitioned swap table
  FUNCTION available_for_exchange(
    in_table_rec  IN cort_exec_pkg.gt_table_rec
  )
  RETURN BOOLEAN
  AS
    l_result BOOLEAN;
  BEGIN
    -- Partitioning option is not available for
    IF get_partitioning_option = 'FALSE' THEN
      RETURN FALSE;
    END IF;

    IF in_table_rec.cluster_name IS NOT NULL THEN
      RETURN FALSE;
    END IF;

    IF in_table_rec.policy_arr.COUNT > 0 THEN
      RETURN FALSE;
    END IF;

    l_result := FALSE;
    FOR i IN 1..in_table_rec.column_arr.COUNT LOOP
      IF in_table_rec.column_arr(i).data_type IN ('LONG','LONG RAW','CLOB','BLOB','NCLOB','XMLTYPE') OR
         in_table_rec.column_arr(i).data_type_owner IS NOT NULL -- user-defined type
      THEN
        RETURN FALSE;
      END IF;
      IF in_table_rec.column_arr(i).data_type IN ('NUMBER','FLOAT','VARCHAR2','VARCHAR','CHAR','NVARCHAR2','NCHAR','DATE','RAW','TIMESTAMP','INTERVAL YEAR TO MONTH','INTERVAL DAY TO SECOND') THEN
        -- use list partition
        l_result := TRUE;
      END IF;
    END LOOP;

    IF dbms_db_version.version >= 11 AND in_table_rec.partitioned = 'NO' THEN
      -- use system partition
      l_result := TRUE;
    END IF;

    RETURN l_result;
  END available_for_exchange;

  -- Returns TRUE if it's possible to create subpartitioning for SWAP table, otherwise - FALSE
  FUNCTION is_subpartitioning_available(
    in_table_rec IN cort_exec_pkg.gt_table_rec
  )
  RETURN BOOLEAN
  AS
    l_result BOOLEAN;
  BEGIN
    -- Partitioning option is not available for
    IF get_partitioning_option = 'FALSE' THEN
      RETURN FALSE;
    END IF;

    IF in_table_rec.partitioning_type IN ('REFERENCE','SYSTEM') OR
       in_table_rec.iot_name IS NOT NULL
    THEN
      l_result := FALSE;
    ELSE
      l_result := TRUE;
    END IF;
    RETURN l_result;
  END is_subpartitioning_available;

  -- Returns first column suitable for partitioning
  FUNCTION get_column_for_partitioning(
    in_table_rec    IN cort_exec_pkg.gt_table_rec
  )
  RETURN VARCHAR2
  AS
  BEGIN
    FOR i IN 1..in_table_rec.column_arr.COUNT LOOP
      IF in_table_rec.column_arr(i).data_type IN ('NUMBER','FLOAT','VARCHAR2','VARCHAR','CHAR','NVARCHAR2','NCHAR','DATE','RAW','TIMESTAMP','INTERVAL YEAR TO MONTH','INTERVAL DAY TO SECOND') THEN
        RETURN in_table_rec.column_arr(i).column_name;
      END IF;
    END LOOP;
    RETURN NULL;
  END get_column_for_partitioning;

  -- Generic function to format statements
  FUNCTION get_clause_by_name(
    in_index_arr   IN arrays.gt_xlstr_indx,
    in_clause_name IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    IF in_index_arr.EXISTS(in_clause_name) THEN
      RETURN in_index_arr(in_clause_name);
    ELSE
      RETURN NULL;
    END IF;
  END get_clause_by_name;

  PROCEDURE add_alter_table_stmt(
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    in_table_name    IN VARCHAR2,
    in_owner         IN VARCHAR2,
    in_frwd_clause   IN CLOB,
    in_rlbk_clause   IN CLOB
  )
  AS
    l_stmt CLOB;
  BEGIN
    IF in_frwd_clause IS NOT NULL THEN
      l_stmt := 'ALTER TABLE "'||in_owner||'"."'||in_table_name||'" '||in_frwd_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_stmt;
    IF in_rlbk_clause IS NOT NULL THEN
      l_stmt := 'ALTER TABLE "'||in_owner||'"."'||in_table_name||'" '||in_rlbk_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_stmt;
  END add_alter_table_stmt;

  PROCEDURE add_alter_sequence_stmt(
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    in_sequence_name IN VARCHAR2,
    in_owner         IN VARCHAR2,
    in_frwd_clause   IN CLOB,
    in_rlbk_clause   IN CLOB
  )
  AS
    l_stmt CLOB;
  BEGIN
    IF in_frwd_clause IS NOT NULL THEN
      l_stmt := 'ALTER SEQUENCE "'||in_owner||'"."'||in_sequence_name||'" '||in_frwd_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_stmt;
    IF in_rlbk_clause IS NOT NULL THEN
      l_stmt := 'ALTER SEQUENCE "'||in_owner||'"."'||in_sequence_name||'" '||in_rlbk_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_stmt;
  END add_alter_sequence_stmt;

  PROCEDURE add_alter_index_stmt(
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    in_index_name    IN VARCHAR2,
    in_owner         IN VARCHAR2,
    in_frwd_clause   IN CLOB,
    in_rlbk_clause   IN CLOB
  )
  AS
    l_stmt CLOB;
  BEGIN
    IF in_frwd_clause IS NOT NULL THEN
      l_stmt := 'ALTER INDEX "'||in_owner||'"."'||in_index_name||'" '||in_frwd_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_stmt;
    IF in_rlbk_clause IS NOT NULL THEN
      l_stmt := 'ALTER INDEX "'||in_owner||'"."'||in_index_name||'" '||in_rlbk_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_stmt;
  END add_alter_index_stmt;

  PROCEDURE add_alter_type_stmt(
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    in_type_name     IN VARCHAR2,
    in_owner         IN VARCHAR2,
    in_frwd_clause   IN CLOB,
    in_rlbk_clause   IN CLOB
  )
  AS
    l_stmt CLOB;
  BEGIN
    IF in_frwd_clause IS NOT NULL THEN
      l_stmt := 'ALTER TYPE "'||in_owner||'"."'||in_type_name||'" '||in_frwd_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_stmt;
    IF in_rlbk_clause IS NOT NULL THEN
      l_stmt := 'ALTER TYPE "'||in_owner||'"."'||in_type_name||'" '||in_rlbk_clause;
    ELSE
      l_stmt := NULL;
    END IF;
    io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_stmt;
  END add_alter_type_stmt;

  PROCEDURE add_stmt(
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    in_frwd_stmt     IN CLOB,
    in_rlbk_stmt     IN CLOB
  )
  AS
  BEGIN
    io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := in_frwd_stmt;
    io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := in_rlbk_stmt;
  END add_stmt;

  -- returns SQL clause
  FUNCTION get_clause(
    in_clause   IN VARCHAR2,
    in_value    IN VARCHAR2,
    in_left_ch  IN VARCHAR2 DEFAULT NULL,
    in_right_ch IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_value   VARCHAR2(32767);
    l_clause  VARCHAR2(32767);
    l_stmt    VARCHAR2(32767);
  BEGIN
    IF in_value IS NOT NULL THEN
      CASE in_clause
      WHEN gc_logging THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'LOGGING'
                     WHEN 'NO' THEN 'NOLOGGING'
                     WHEN 'NONE' THEN NULL
                     ELSE NULL
                   END;
        l_clause := NULL;
      WHEN gc_cache THEN
        l_value := CASE in_value
                     WHEN 'Y' THEN 'CACHE'
                     WHEN 'N' THEN 'NOCACHE'
                     WHEN 'YES' THEN 'CACHE'
                     WHEN 'NO' THEN 'NOCACHE'
                     WHEN 'CACHEREADS' THEN 'CACHE READS'
                   END;
        l_clause := NULL;
      WHEN gc_row_movement THEN
        l_value := CASE in_value
                     WHEN 'ENABLED'  THEN 'ENABLE ROW MOVEMENT'
                     WHEN 'DISABLED' THEN 'DISABLE ROW MOVEMENT'
                   END;
        l_clause := NULL;
      WHEN gc_monitoring THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'MONITORING'
                     WHEN 'NO' THEN 'NOMONITORING'
                   END;
        l_clause := NULL;
      WHEN gc_mapping THEN
        l_value := CASE in_value
                     WHEN 'Y' THEN 'MAPPING TABLE'
                     WHEN 'N' THEN 'NOMAPPING'
                   END;
        l_clause := NULL;
      WHEN gc_including_column THEN
        l_value := CASE
                     WHEN in_value IS NOT NULL
                     THEN 'INCLUDNG "'||in_value||'"'
                     ELSE NULL
                   END;
        l_clause := NULL;
      WHEN gc_char_used THEN
        l_value := CASE in_value
                     WHEN 'B' THEN 'BYTE'
                     WHEN 'C' THEN 'CHAR'
                   END;
        l_clause := NULL;
      WHEN gc_encryption THEN
        l_value := CASE
                     WHEN in_value IS NULL THEN 'DECRYPT'
                     ELSE 'ENCRYPT USING '''||in_value||''''
                   END;
        l_clause := NULL;
      WHEN gc_encrypt THEN
        l_value := CASE
                     WHEN in_value = 'NO' THEN 'DECRYPT'
                     WHEN in_value = 'YES' THEN 'ENCRYPT'
                     WHEN in_value = 'NONE' THEN NULL
                   END;
        l_clause := NULL;
      WHEN gc_salt THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'SALT'
                     WHEN 'NO' THEN 'NO SALT'
                   END;
        l_clause := NULL;
      WHEN gc_nullable THEN
        l_value := CASE in_value
                     WHEN 'Y' THEN 'NULL'
                     WHEN 'N' THEN 'NOT NULL'
                   END;
        l_clause := NULL;
      WHEN gc_constraint_type THEN
        l_value := CASE in_value
                     WHEN 'C' THEN 'CHECK'
                     WHEN 'P' THEN 'PRIMARY KEY'
                     WHEN 'U' THEN 'UNIQUE'
                     WHEN 'R' THEN 'FOREIGN KEY'
                     WHEN 'N' THEN 'NOT NULL'
                   END;
        l_clause := NULL;
      WHEN gc_delete_rule THEN
        l_value := CASE in_value
                     WHEN 'CASCADE' THEN 'ON DELETE CASCADE'
                     WHEN 'SET NULL' THEN 'ON DELETE SET NULL'
                   END;
        l_clause := NULL;
      WHEN gc_status THEN
        l_value := CASE in_value
                     WHEN 'ENABLED' THEN 'ENABLE'
                     WHEN 'DISABLED' THEN 'DISABLE'
                   END;
        l_clause := NULL;
      WHEN gc_validated THEN
        l_value := CASE in_value
                     WHEN 'VALIDATED' THEN 'VALIDATE'
                     WHEN 'NOT VALIDATED' THEN 'NOVALIDATE'
                   END;
        l_clause := NULL;
      WHEN gc_key_compression THEN
        l_value := CASE in_value
                     WHEN 'ENABLED' THEN 'COMPRESS'
                     WHEN 'PREFIX' THEN 'COMPRESS'
                     WHEN 'DISABLED' THEN 'NOCOMPRESS'
                     WHEN 'ADVANCED LOW' THEN 'COMPRESS ADVANCED LOW'
                     WHEN 'ADVANCED HIGH' THEN 'COMPRESS ADVANCED HIGH'
                   END;
        l_clause := NULL;
      WHEN gc_log_data THEN
        l_value := CASE in_value
                     WHEN 'PRIMARY KEY LOGGING' THEN 'PRIMARY KEY'
                     WHEN 'UNIQUE KEY LOGGING'  THEN 'UNIQUE'
                     WHEN 'FOREIGN KEY LOGGING' THEN 'FOREIGN KEY'
                     WHEN 'ALL COLUMN LOGGING'  THEN 'ALL'
                   END;
        l_clause := in_clause||' ';
      WHEN gc_log_always THEN
        l_value := CASE in_value
                     WHEN 'ALWAYS' THEN 'ALWAYS'
                   END;
        l_clause := NULL;
      WHEN gc_hierarchy_option THEN
        IF in_value = 'YES' THEN
          l_value := in_clause;
        ELSE
          l_value := NULL;
        END IF;
        l_clause := NULL;
      WHEN gc_grant_option THEN
        IF in_value = 'YES' THEN
          l_value := in_clause;
        ELSE
          l_value := NULL;
        END IF;
        l_clause := NULL;
      WHEN gc_trigger_type THEN
        l_value := CASE in_value
                     WHEN 'BEFORE STATEMENT' THEN 'BEFORE'
                     WHEN 'BEFORE EACH ROW' THEN 'BEFORE'
                     WHEN 'AFTER STATEMENT' THEN 'AFTER'
                     WHEN 'AFTER EACH ROW' THEN 'AFTER'
                     WHEN 'INSTEAD OF' THEN 'INSTEAD OF'
                     WHEN 'COMPOUND' THEN 'FOR'
                   END;
        l_clause := NULL;
      WHEN gc_for_each_row THEN
        l_value := CASE in_value
                     WHEN 'AFTER EACH ROW' THEN 'FOR EACH ROW'
                     WHEN 'BEFORE EACH ROW' THEN 'FOR EACH ROW'
                   END;
        l_clause := NULL;
      WHEN gc_securefile THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'SECUREFILE'
                     WHEN 'NO' THEN 'BASICFILE'
                   END;
        l_clause := NULL;
      WHEN gc_deduplication THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'DEDUPICATE'
                     WHEN 'NO' THEN 'KEEP_DUPLICATES'
                   END;
        l_clause := NULL;
      WHEN gc_lob_compression THEN
        l_value := CASE in_value
                     WHEN 'MEDIUM' THEN 'COMPRESS MEDIUM'
                     WHEN 'HIGH' THEN 'COMPRESS HIGH'
                     WHEN 'NO' THEN 'NOCOMPRESS'
                     WHEN 'NONE' THEN NULL
                   END;
        l_clause := NULL;
      WHEN gc_in_row THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'ENABLE STORAGE IN ROW'
                     WHEN 'NO' THEN 'DISABLE STORAGE IN ROW'
                   END;
        l_clause := NULL;
      WHEN gc_retention THEN
        IF in_value IN ('DEFAULT','INVALID') THEN
          l_value := NULL;
        ELSE
          l_value := in_value;
        END IF;
        l_clause := in_clause;
      WHEN gc_xml_storage_type THEN
        l_value := CASE in_value
                     WHEN 'OBJECT-RELATIONAL' THEN 'OBJECT RELATIONAL'
                     WHEN 'BINARY' THEN 'BINARY XML'
                     WHEN 'CLOB' THEN 'CLOB'
                   END;
        l_clause := NULL;
      WHEN gc_xml_anyschema THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'ALLOW ANYSCHEMA'
                     WHEN 'NO' THEN 'DISALLOW ANYSCHEMA'
                   END;
        l_clause := NULL;
      WHEN gc_xml_nonschema THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'ALLOW NONSCHEMA'
                     WHEN 'NO' THEN 'DISALLOW NONSCHEMA'
                   END;
        l_clause := NULL;
      WHEN gc_cycle_flag THEN
        l_value := CASE in_value
                     WHEN 'Y' THEN 'CYCLE'
                     WHEN 'N' THEN 'NOCYCLE'
                   END;
        l_clause := NULL;
      WHEN gc_order_flag THEN
        l_value := CASE in_value
                     WHEN 'Y' THEN 'ORDER'
                     WHEN 'N' THEN 'NOORDER'
                   END;
        l_clause := NULL;
      WHEN gc_cache_size THEN
        l_value := CASE in_value
                     WHEN '0' THEN 'NOCACHE'
                     ELSE 'CACHE '||in_value
                   END;
        l_clause := NULL;
      WHEN gc_final THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'FINAL'
                     WHEN 'NO'  THEN 'NOT FINAL'
                   END;
      WHEN gc_instantiable THEN
        l_value := CASE in_value
                     WHEN 'YES' THEN 'INSTANTIABLE'
                     WHEN 'NO'  THEN 'NOT INSTANTIABLE'
                   END;
      ELSE
        IF in_clause LIKE '<%>' AND in_clause = LOWER(in_clause) THEN
          l_value := in_value;
          l_clause := NULL;
        ELSE
          l_value := in_value;
          l_clause := in_clause||' ';
        END IF;
      END CASE;
      l_stmt := ' '||l_clause||in_left_ch||l_value||in_right_ch||' ';
    END IF;
    RETURN l_stmt;
  END get_clause;

  -- return from clause for join bitmap index
  FUNCTION get_from_clause(
    in_join_inner_owner_arr IN arrays.gt_name_arr,      -- Array of owner of join inner table
    in_join_inner_table_arr IN arrays.gt_name_arr,      -- Array of name of join inner table
    in_join_outer_owner_arr IN arrays.gt_name_arr,      -- Array of owner of join inner table
    in_join_outer_table_arr IN arrays.gt_name_arr       -- Array of name of join inner table
  )
  RETURN VARCHAR2
  AS
    l_owner_table_ind   arrays.gt_int_indx;
    l_full_name         VARCHAR2(65);
    l_result            VARCHAR2(32767);
  BEGIN
    l_result := NULL;
    FOR i IN 1..in_join_inner_table_arr.COUNT LOOP
      l_full_name := '"'||in_join_inner_owner_arr(i)||'"."'||cort_parse_pkg.get_original_name('TABLE',in_join_inner_table_arr(i))||'"';
      IF NOT l_owner_table_ind.EXISTS(l_full_name) THEN
        l_result := l_result || l_full_name ||',';
        l_owner_table_ind(l_full_name) := i;
      END IF;
      l_full_name := '"'||in_join_outer_owner_arr(i)||'"."'||cort_parse_pkg.get_original_name('TABLE',in_join_outer_table_arr(i))||'"';
      IF NOT l_owner_table_ind.EXISTS(l_full_name) THEN
        l_result := l_result || l_full_name ||',';
        l_owner_table_ind(l_full_name) := i;
      END IF;
    END LOOP;
    l_result := TRIM(',' FROM l_result);
    RETURN l_result;
  END get_from_clause;

  FUNCTION get_where_clause(
    in_join_inner_owner_arr  IN arrays.gt_name_arr,      -- Array of owner of join inner table
    in_join_inner_table_arr  IN arrays.gt_name_arr,      -- Array of name of join inner table
    in_join_inner_column_arr IN arrays.gt_name_arr,      -- Array of column of join inner table
    in_join_outer_owner_arr  IN arrays.gt_name_arr,      -- Array of owner of join inner table
    in_join_outer_table_arr  IN arrays.gt_name_arr,      -- Array of name of join inner table
    in_join_outer_column_arr IN arrays.gt_name_arr       -- Array of column of join inner table
  )
  RETURN VARCHAR2
  AS
    l_full_name1        VARCHAR2(100);
    l_full_name2        VARCHAR2(100);
    l_result            VARCHAR2(32767);
  BEGIN
    l_result := NULL;
    FOR i IN 1..in_join_inner_column_arr.COUNT LOOP
      l_full_name1 := '"'||in_join_inner_owner_arr(i)||'"."'||
                          cort_parse_pkg.get_original_name('TABLE',in_join_inner_table_arr(i))||'"."'||
                          in_join_inner_column_arr(i)||'"';
      l_full_name2 := '"'||in_join_outer_owner_arr(i)||'"."'||
                          cort_parse_pkg.get_original_name('TABLE',in_join_outer_table_arr(i))||'"."'||
                          in_join_outer_column_arr(i)||'"';

      l_result := l_result || l_full_name1 || ' = ' || l_full_name2;

      IF i < in_join_inner_column_arr.COUNT THEN
        l_result := l_result || ' AND ';
      END IF;
    END LOOP;
    RETURN l_result;
  END get_where_clause;

  -- returns COMPRESS clause
  FUNCTION get_compress_clause(
    in_compression_rec IN cort_exec_pkg.gt_compression_rec
  )
  RETURN VARCHAR2
  AS
    l_stmt    VARCHAR2(32767);
  BEGIN
    IF in_compression_rec.compression IS NOT NULL THEN
      CASE in_compression_rec.compression
      WHEN 'ENABLED' THEN
        l_stmt := ' COMPRESS ';
        $IF dbms_db_version.version >= 12 AND dbms_db_version.release >=1 $THEN
        l_stmt := l_stmt ||
                  CASE in_compression_rec.compress_for
                    WHEN 'BASIC' THEN 'BASIC '
                    WHEN 'ADVANCED' THEN 'FOR OLTP '
                    WHEN 'QUERY LOW' THEN 'FOR QUERY LOW '
                    WHEN 'QUERY HIGH' THEN 'FOR QUERY HIGH '
                    WHEN 'ARCHIVE LOW' THEN 'FOR ARCHIVE LOW '
                    WHEN 'ARCHIVE HIGH' THEN 'FOR ARCHIVE HIGH '
                    ELSE NULL
                  END;
        $ELSIF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
        l_stmt := l_stmt ||
                  CASE in_compression_rec.compress_for
                    WHEN 'BASIC' THEN 'BASIC '
                    WHEN 'OLTP' THEN 'FOR OLTP '
                    WHEN 'QUERY LOW' THEN 'FOR QUERY LOW '
                    WHEN 'QUERY HIGH' THEN 'FOR QUERY HIGH '
                    WHEN 'ARCHIVE LOW' THEN 'FOR ARCHIVE LOW '
                    WHEN 'ARCHIVE HIGH' THEN 'FOR ARCHIVE HIGH '
                    ELSE NULL
                  END;
        $ELSIF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
        l_stmt := l_stmt ||
                  CASE in_compression_rec.compress_for
                    WHEN 'DIRECT LOAD ONLY' THEN 'FOR DIRECT_LOAD OPERATIONS '
                    WHEN 'FOR ALL OPERATIONS' THEN 'FOR ALL OPERATIONS '
                    ELSE NULL
                  END;
        $END
      WHEN 'DISABLED' THEN
        l_stmt := ' NOCOMPRESS ';
      ELSE
        l_stmt := NULL;
      END CASE;
    END IF;
    RETURN l_stmt;
  END get_compress_clause;

  -- return column data type clause
  FUNCTION get_column_type_clause(
    in_column_rec IN cort_exec_pkg.gt_column_rec
  )
  RETURN VARCHAR2
  AS
  BEGIN
    IF in_column_rec.data_type_owner IS NULL THEN
      CASE
       WHEN in_column_rec.data_type IN ('CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2') THEN
         RETURN get_clause(in_column_rec.data_type, in_column_rec.char_length||' '||get_clause(gc_char_used,in_column_rec.char_used), '(', ')');
       WHEN in_column_rec.data_type = 'NUMBER' THEN
         IF in_column_rec.data_precision IS NULL AND in_column_rec.data_scale IS NULL THEN
           RETURN get_clause(in_column_rec.data_type, ' ');
         ELSE
           RETURN get_clause(in_column_rec.data_type, nvl(to_char(in_column_rec.data_precision),'*')||','||in_column_rec.data_scale, '(', ')');
         END IF;
       WHEN in_column_rec.data_type = 'RAW' THEN
         RETURN get_clause(in_column_rec.data_type, in_column_rec.data_length, '(', ')');
       WHEN in_column_rec.data_type = 'TIMESTAMP' THEN
         RETURN get_clause(in_column_rec.data_type, in_column_rec.data_scale, '(', ')');
       WHEN in_column_rec.data_type = 'INTERVAL YEAR TO MONTH' THEN
         RETURN 'INTERVAL YEAR('||in_column_rec.data_precision||') TO MONTH('||in_column_rec.data_scale||')';
       WHEN in_column_rec.data_type = 'INTERVAL DAY TO SECOND' THEN
         RETURN 'INTERVAL DAY('||in_column_rec.data_precision||') TO SECOND('||in_column_rec.data_scale||')';
       ELSE
         RETURN ' '||in_column_rec.data_type||' ';
      END CASE;
    ELSE
      RETURN ' '||in_column_rec.data_type_mod||' "'||in_column_rec.data_type_owner||'"."'||in_column_rec.data_type||'" ';
    END IF;
  END get_column_type_clause;

  -- returns qualified name: for system generated name equals to temp name
  FUNCTION get_cons_qualified_name(
    in_constraint_rec IN cort_exec_pkg.gt_constraint_rec
  )
  RETURN VARCHAR2
  AS
  BEGIN
    -- Replace system generated name with temp name
    IF in_constraint_rec.generated = 'USER NAME' THEN
      RETURN '"'||in_constraint_rec.constraint_name||'"';
    ELSE
      RETURN LOWER('"'||in_constraint_rec.constraint_name||'"');
    END IF;
  END get_cons_qualified_name;

  -- Returns ADD CONSTRAINT clause
  FUNCTION get_add_constraint_clause(
    in_constraint_rec   IN cort_exec_pkg.gt_constraint_rec,
    in_index_rec        IN cort_exec_pkg.gt_index_rec
  )
  RETURN CLOB
  AS
    l_clause            CLOB;
    l_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_clause_indx_arr(gc_add_constraint) := get_clause(gc_add_constraint, in_constraint_rec.rename_rec.current_name, '"', '"');
    l_clause_indx_arr(gc_constraint_type) := get_clause(gc_constraint_type, in_constraint_rec.constraint_type);
    CASE
      WHEN in_constraint_rec.constraint_type IN ('C','F') THEN
        l_clause_indx_arr(gc_constraint_columns) := get_clause(gc_constraint_columns, in_constraint_rec.search_condition, '(', ')');
      WHEN in_constraint_rec.constraint_type IN ('P','U') THEN
        l_clause_indx_arr(gc_constraint_columns) := get_clause(gc_constraint_columns, convert_arr_to_str(in_constraint_rec.column_arr), '(', ')');
        IF in_index_rec.index_name IS NOT NULL THEN
          l_clause_indx_arr(gc_constraint_index) := get_clause(gc_constraint_index, in_index_rec.owner||'"."'||in_index_rec.rename_rec.current_name, '"', '"');
        END IF;
      WHEN in_constraint_rec.constraint_type = 'R' THEN
        l_clause_indx_arr(gc_constraint_columns) := get_clause(gc_constraint_columns, convert_arr_to_str(in_constraint_rec.column_arr), '(', ')');
        l_clause_indx_arr(gc_references) := get_clause(gc_references, '"'||in_constraint_rec.r_owner||'"."'||in_constraint_rec.r_table_name||'"');
        l_clause_indx_arr(gc_ref_columns) := get_clause(gc_ref_columns, convert_arr_to_str(in_constraint_rec.r_column_arr), '(', ')');
        l_clause_indx_arr(gc_delete_rule) := get_clause(gc_delete_rule, in_constraint_rec.delete_rule);
    END CASE;

    l_clause_indx_arr(gc_deferrable) := get_clause(gc_deferrable, in_constraint_rec.deferrable);
    l_clause_indx_arr(gc_deferred) := get_clause(gc_deferred, in_constraint_rec.deferred);
    l_clause_indx_arr(gc_status) := get_clause(gc_status, in_constraint_rec.status);
    l_clause_indx_arr(gc_validated) := get_clause(gc_validated, in_constraint_rec.validated);

    l_clause := get_clause_by_name(l_clause_indx_arr, gc_add_constraint)||
                get_clause_by_name(l_clause_indx_arr, gc_constraint_type)||
                get_clause_by_name(l_clause_indx_arr, gc_constraint_columns)||
                get_clause_by_name(l_clause_indx_arr, gc_references)||
                get_clause_by_name(l_clause_indx_arr, gc_ref_columns)||
                get_clause_by_name(l_clause_indx_arr, gc_delete_rule)||
                get_clause_by_name(l_clause_indx_arr, gc_deferrable)||
                get_clause_by_name(l_clause_indx_arr, gc_deferred)||
                get_clause_by_name(l_clause_indx_arr, gc_constraint_index)||
                get_clause_by_name(l_clause_indx_arr, gc_status)||
                get_clause_by_name(l_clause_indx_arr, gc_validated);

    RETURN l_clause;
  END get_add_constraint_clause;

  -- builds ALTER statements to add constraint
  PROCEDURE add_constraint(
    in_constraint_rec        IN cort_exec_pkg.gt_constraint_rec,
    in_index_rec             IN cort_exec_pkg.gt_index_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_keep_index             VARCHAR2(30);
  BEGIN
    -- create forward statements
    l_frwd_clause := get_add_constraint_clause(in_constraint_rec, in_index_rec);
    -- create rollback statements
    IF in_constraint_rec.constraint_type IN ('P','U') THEN
      l_keep_index := ' KEEP INDEX';
    ELSE
      l_keep_index := NULL;
    END IF;
    l_rlbk_clause := get_clause(gc_drop_constraint, in_constraint_rec.rename_rec.current_name, '"', '"'||l_keep_index);
    add_alter_table_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_table_name    => in_constraint_rec.table_name,
      in_owner         => in_constraint_rec.owner,
      in_frwd_clause   => l_frwd_clause,
      in_rlbk_clause   => l_rlbk_clause
    );
  END add_constraint;

  -- builds ALTER statements to drop constraint
  PROCEDURE drop_constraint(
    in_constraint_rec        IN cort_exec_pkg.gt_constraint_rec,
    in_index_rec             IN cort_exec_pkg.gt_index_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_keep_index             VARCHAR2(30);
  BEGIN
    -- create forward statements
    IF in_constraint_rec.constraint_type IN ('P','U') THEN
      l_keep_index := ' KEEP INDEX';
    ELSE
      l_keep_index := NULL;
    END IF;
    l_frwd_clause := get_clause(gc_drop_constraint, in_constraint_rec.rename_rec.current_name, '"', '"'||l_keep_index);

    -- create rollback statements
    l_rlbk_clause := get_add_constraint_clause(in_constraint_rec, in_index_rec);

    add_alter_table_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_table_name    => in_constraint_rec.table_name,
      in_owner         => in_constraint_rec.owner,
      in_frwd_clause   => l_frwd_clause,
      in_rlbk_clause   => l_rlbk_clause
    );
  END drop_constraint;

  -- Returns ADD log_group clause
  FUNCTION get_add_log_group_clause(
    in_log_group_rec   IN cort_exec_pkg.gt_log_group_rec
  )
  RETURN CLOB
  AS
    l_clause            CLOB;
    l_columns_str       CLOB;
    l_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_clause_indx_arr(gc_add_supplemental_log) := gc_add_supplemental_log;
    IF in_log_group_rec.log_group_type = 'USER LOG GROUP' THEN
      l_columns_str := NULL;
      FOR i IN 1..in_log_group_rec.column_arr.COUNT LOOP
        IF in_log_group_rec.column_arr(i) NOT LIKE '"%"' THEN
          l_columns_str := l_columns_str||TRIM('"'||in_log_group_rec.column_arr(i)||'" '||NULLIF(in_log_group_rec.column_log_arr(i),'LOG'))||',';
        ELSE
          l_columns_str := l_columns_str||TRIM(in_log_group_rec.column_arr(i)||' '||NULLIF(in_log_group_rec.column_log_arr(i),'LOG'))||',';
        END IF;
      END LOOP;
      l_columns_str := TRIM(',' FROM l_columns_str);
      l_clause_indx_arr(gc_log_group) := get_clause(gc_log_group, in_log_group_rec.log_group_name, '"', '"');
      l_clause_indx_arr(gc_log_columns) := get_clause(gc_log_columns, l_columns_str, '(', ')');
      l_clause_indx_arr(gc_log_always) := get_clause(gc_log_always, in_log_group_rec.always);
    ELSE
      l_clause_indx_arr(gc_log_data) := get_clause(gc_log_data, in_log_group_rec.log_group_type, '(', ') COLUMNS');
    END IF;

    l_clause := get_clause_by_name(l_clause_indx_arr, gc_add_supplemental_log)||
                get_clause_by_name(l_clause_indx_arr, gc_log_group)||
                get_clause_by_name(l_clause_indx_arr, gc_log_columns)||
                get_clause_by_name(l_clause_indx_arr, gc_log_always)||
                get_clause_by_name(l_clause_indx_arr, gc_log_data);

    RETURN l_clause;
  END get_add_log_group_clause;

  -- Returns ADD log_group clause
  FUNCTION get_drop_log_group_clause(
    in_log_group_rec   IN cort_exec_pkg.gt_log_group_rec
  )
  RETURN VARCHAR2
  AS
    l_clause            CLOB;
  BEGIN
    IF in_log_group_rec.log_group_type  = 'USER LOG GROUP' THEN
      l_clause := gc_drop_supplemental_log||get_clause(gc_log_group, in_log_group_rec.log_group_name, '"', '"');
    ELSE
      l_clause := gc_drop_supplemental_log||get_clause(gc_log_data, in_log_group_rec.log_group_type, '(', ') COLUMNS');
    END IF;
    RETURN l_clause;
  END get_drop_log_group_clause;

  -- builds ALTER statements to add check log_group
  PROCEDURE add_log_group(
    in_log_group_rec         IN cort_exec_pkg.gt_log_group_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    -- create forward statements
    l_frwd_clause := get_add_log_group_clause(in_log_group_rec);
    -- create rollback statements
    l_rlbk_clause := get_drop_log_group_clause(in_log_group_rec);

    add_alter_table_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_table_name    => in_log_group_rec.table_name,
      in_owner         => in_log_group_rec.owner,
      in_frwd_clause   => l_frwd_clause,
      in_rlbk_clause   => l_rlbk_clause
    );
  END add_log_group;

  PROCEDURE drop_log_group(
    in_log_group_rec         IN cort_exec_pkg.gt_log_group_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    -- create forward statements
    l_frwd_clause := get_drop_log_group_clause(in_log_group_rec);
    -- create rollback statements
    l_rlbk_clause := get_add_log_group_clause(in_log_group_rec);

    add_alter_table_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_table_name    => in_log_group_rec.table_name,
      in_owner         => in_log_group_rec.owner,
      in_frwd_clause   => l_frwd_clause,
      in_rlbk_clause   => l_rlbk_clause
    );
  END drop_log_group;

  FUNCTION get_storage_clause(
    in_storage_rec    IN cort_exec_pkg.gt_storage_rec
  )
  RETURN CLOB
  AS
    l_clause  CLOB;
  BEGIN
    l_clause := l_clause||get_clause(gc_initial, in_storage_rec.initial_extent);
    l_clause := l_clause||get_clause(gc_minextents, in_storage_rec.min_extents);
    l_clause := l_clause||get_clause(gc_pctincrease, in_storage_rec.pct_increase);
    l_clause := l_clause||get_clause(gc_freelists, in_storage_rec.freelists);
    l_clause := l_clause||get_clause(gc_freelist_groups, in_storage_rec.freelist_groups);
    l_clause := l_clause||get_clause(gc_buffer_pool, in_storage_rec.buffer_pool);
    l_clause := get_clause(gc_storage, l_clause, '(', ')');
    RETURN l_clause;
  END get_storage_clause;

  FUNCTION get_lob_storage_clause(
    in_storage_rec    IN cort_exec_pkg.gt_storage_rec
  )
  RETURN CLOB
  AS
    l_clause   CLOB;
  BEGIN
    IF cort_exec_pkg.g_params.physical_attr.value_exists('TABLE') THEN
      l_clause := l_clause||get_clause(gc_initial, in_storage_rec.initial_extent);
      l_clause := l_clause||get_clause(gc_minextents, in_storage_rec.min_extents);
      l_clause := l_clause||get_clause(gc_maxsize, in_storage_rec.max_size);
      l_clause := l_clause||get_clause(gc_pctincrease, in_storage_rec.pct_increase);
      l_clause := l_clause||get_clause(gc_freelists, in_storage_rec.freelists);
      l_clause := l_clause||get_clause(gc_freelist_groups, in_storage_rec.freelist_groups);
      l_clause := l_clause||get_clause(gc_buffer_pool, in_storage_rec.buffer_pool);
      l_clause := get_clause(gc_storage, l_clause, '(', ')');
    END IF;
    RETURN l_clause;
  END get_lob_storage_clause;

  -- Compares storage attributes. Returns 0 if they are identical, otherwise returns 1
  FUNCTION comp_storage(
    in_source_storage_rec  IN cort_exec_pkg.gt_storage_rec,
    in_target_storage_rec  IN cort_exec_pkg.gt_storage_rec,
    out_frwd_clause        OUT NOCOPY CLOB,
    out_rlbk_clause        OUT NOCOPY CLOB
  )
  RETURN PLS_INTEGER
  AS
    l_result      PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;
    IF comp_value(in_source_storage_rec.initial_extent, in_target_storage_rec.initial_extent) = 1 THEN
      debug('Compare physical_attr_rec.storage.initial_extent - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_storage_rec.min_extents, in_target_storage_rec.min_extents) = 1 THEN
      debug('Compare physical_attr_rec.storage.min_extents - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_storage_rec.freelist_groups, in_target_storage_rec.freelist_groups) = 1 THEN
      debug('Compare physical_attr_rec.storage.freelist_groups - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_storage_rec.pct_increase, in_target_storage_rec.pct_increase) = 1 THEN
      l_result := gc_result_alter;
      out_frwd_clause := out_frwd_clause || get_clause(gc_pctincrease, in_target_storage_rec.pct_increase);
      out_rlbk_clause := out_rlbk_clause || get_clause(gc_pctincrease, in_source_storage_rec.pct_increase);
    END IF;
    IF comp_value(in_source_storage_rec.freelists, in_target_storage_rec.freelists) = 1 THEN
      l_result := gc_result_alter;
      out_frwd_clause := out_frwd_clause || get_clause(gc_freelists, in_target_storage_rec.freelists);
      out_rlbk_clause := out_rlbk_clause || get_clause(gc_freelists, in_source_storage_rec.freelists);
    END IF;
    IF comp_value(in_source_storage_rec.buffer_pool, in_target_storage_rec.buffer_pool) = 1 THEN
      l_result := gc_result_alter;
      out_frwd_clause := out_frwd_clause || get_clause(gc_buffer_pool, in_target_storage_rec.buffer_pool);
      out_rlbk_clause := out_rlbk_clause || get_clause(gc_buffer_pool, in_source_storage_rec.buffer_pool);
    END IF;
    -- Applicable for LOB Segments only
    IF comp_value(in_source_storage_rec.max_size, in_target_storage_rec.max_size) = 1 THEN
      l_result := gc_result_alter;
      out_frwd_clause := out_frwd_clause || get_clause(gc_maxsize, in_target_storage_rec.max_size);
      out_rlbk_clause := out_rlbk_clause || get_clause(gc_maxsize, in_source_storage_rec.max_size);
    END IF;
    RETURN l_result;
  END comp_storage;

  FUNCTION get_part_physical_attr_clause(
    in_partition_rec  IN cort_exec_pkg.gt_partition_rec
  )
  RETURN CLOB
  AS
    l_clause   CLOB;
  BEGIN
    IF cort_exec_pkg.g_params.physical_attr.value_exists(in_partition_rec.partition_level)
    THEN
      l_clause := get_clause(gc_pctfree, in_partition_rec.physical_attr_rec.pct_free)||
                  get_clause(gc_pctused, in_partition_rec.physical_attr_rec.pct_used)||
                  get_clause(gc_initrans, in_partition_rec.physical_attr_rec.ini_trans)||
                  get_storage_clause(in_partition_rec.physical_attr_rec.storage);
    END IF;
    RETURN l_clause;
  END get_part_physical_attr_clause;

  FUNCTION get_index_physical_attr_clause(
    in_physical_attr_rec  IN cort_exec_pkg.gt_physical_attr_rec
  )
  RETURN CLOB
  AS
    l_clause   CLOB;
  BEGIN
    IF cort_exec_pkg.g_params.physical_attr.value_exists('INDEX') THEN
      l_clause := get_clause(gc_initrans, in_physical_attr_rec.ini_trans)||
                  get_storage_clause(in_physical_attr_rec.storage);
    END IF;
    RETURN l_clause;
  END get_index_physical_attr_clause;


  -- Compares physicale attributes.
  FUNCTION comp_physical_attr(
    in_source_physical_attr_rec  IN cort_exec_pkg.gt_physical_attr_rec,
    in_target_physical_attr_rec  IN cort_exec_pkg.gt_physical_attr_rec,
    out_frwd_clause              OUT NOCOPY CLOB,
    out_rlbk_clause              OUT NOCOPY CLOB
  )
  RETURN PLS_INTEGER
  AS
    l_frwd_clause         CLOB;
    l_rlbk_clause         CLOB;
    l_storage_frwd_clause CLOB;
    l_storage_rlbk_clause CLOB;
    l_result              PLS_INTEGER;
    l_comp_result         PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;

    IF comp_value(in_source_physical_attr_rec.pct_free, in_target_physical_attr_rec.pct_free) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause := l_frwd_clause || get_clause(gc_pctfree, in_target_physical_attr_rec.pct_free);
      l_rlbk_clause := l_rlbk_clause || get_clause(gc_pctfree, in_source_physical_attr_rec.pct_free);
    END IF;
    IF comp_value(in_source_physical_attr_rec.pct_used, in_target_physical_attr_rec.pct_used) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause := l_frwd_clause || get_clause(gc_pctused, in_target_physical_attr_rec.pct_used);
      l_rlbk_clause := l_rlbk_clause || get_clause(gc_pctused, in_source_physical_attr_rec.pct_used);
    END IF;
    IF comp_value(in_source_physical_attr_rec.ini_trans, in_target_physical_attr_rec.ini_trans) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause := l_frwd_clause || get_clause(gc_initrans, in_target_physical_attr_rec.ini_trans);
      l_rlbk_clause := l_rlbk_clause || get_clause(gc_initrans, in_source_physical_attr_rec.ini_trans);
    END IF;
    -- storage clauses
    l_comp_result := comp_storage(
                       in_source_storage_rec => in_source_physical_attr_rec.storage,
                       in_target_storage_rec => in_target_physical_attr_rec.storage,
                       out_frwd_clause       => l_storage_frwd_clause,
                       out_rlbk_clause       => l_storage_rlbk_clause
                     );
    CASE l_comp_result
    WHEN gc_result_recreate THEN
      debug('Compare physical_attr.sorage - recreate');
      RETURN gc_result_recreate;
    WHEN gc_result_alter THEN
      debug('Compare physical_attr.sorage - alter');
      l_result := gc_result_alter;
      l_frwd_clause := l_frwd_clause || get_clause(gc_storage, l_storage_frwd_clause, '(', ')');
      l_rlbk_clause := l_rlbk_clause || get_clause(gc_storage, l_storage_rlbk_clause, '(', ')');
    ELSE
      NULL;
    END CASE;
    IF l_result = gc_result_alter THEN
      out_frwd_clause := l_frwd_clause;
      out_rlbk_clause := l_rlbk_clause;
    END IF;
    RETURN l_result;
  END comp_physical_attr;

  -- Compares index physicale attributes.
  FUNCTION comp_index_physical_attr(
    in_source_physical_attr_rec  IN cort_exec_pkg.gt_physical_attr_rec,
    in_target_physical_attr_rec  IN cort_exec_pkg.gt_physical_attr_rec,
    out_frwd_clause              OUT NOCOPY CLOB,
    out_rlbk_clause              OUT NOCOPY CLOB
  )
  RETURN PLS_INTEGER
  AS
    l_frwd_clause         CLOB;
    l_rlbk_clause         CLOB;
    l_storage_frwd_clause CLOB;
    l_storage_rlbk_clause CLOB;
    l_result              PLS_INTEGER;
    l_comp_result         PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;

    IF comp_value(in_source_physical_attr_rec.pct_free, in_target_physical_attr_rec.pct_free) = 1 THEN
      debug('Compare index physical_attr - recrate');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_physical_attr_rec.ini_trans, in_target_physical_attr_rec.ini_trans) = 1 THEN
      debug('Compare index physical_attr - alter');
      l_result := gc_result_alter;
      l_frwd_clause := l_frwd_clause || get_clause(gc_initrans, in_target_physical_attr_rec.ini_trans);
      l_rlbk_clause := l_rlbk_clause || get_clause(gc_initrans, in_source_physical_attr_rec.ini_trans);
    END IF;
    -- storage clauses
    l_comp_result := comp_storage(
                       in_source_storage_rec => in_source_physical_attr_rec.storage,
                       in_target_storage_rec => in_target_physical_attr_rec.storage,
                       out_frwd_clause       => l_storage_frwd_clause,
                       out_rlbk_clause       => l_storage_rlbk_clause
                     );
    CASE l_comp_result
    WHEN gc_result_recreate THEN
      debug('Compare index physical_attr.sorage - recreate');
      RETURN gc_result_recreate;
    WHEN gc_result_alter THEN
      debug('Compare index physical_attr.sorage - alter');
      l_result := gc_result_alter;
      l_frwd_clause := l_frwd_clause || get_clause(gc_storage, l_storage_frwd_clause, '(', ')');
      l_rlbk_clause := l_rlbk_clause || get_clause(gc_storage, l_storage_rlbk_clause, '(', ')');
    ELSE
      NULL;
    END CASE;
    IF l_result = gc_result_alter THEN
      out_frwd_clause := l_frwd_clause;
      out_rlbk_clause := l_rlbk_clause;
    END IF;
    RETURN l_result;
  END comp_index_physical_attr;

  -- Compares compressions attributes. Returns 0 if they are identical, otherwise returns 1
  FUNCTION comp_compression(
    in_source_compression_rec   IN cort_exec_pkg.gt_compression_rec,
    in_target_compression_rec   IN cort_exec_pkg.gt_compression_rec,
    out_frwd_clause             OUT NOCOPY VARCHAR2,
    out_rlbk_clause             OUT NOCOPY VARCHAR2
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF comp_value(in_source_compression_rec.compression, in_target_compression_rec.compression) = 1 OR
       comp_value(in_source_compression_rec.compress_for, in_target_compression_rec.compress_for) = 1
    THEN
      debug('Compare compression - 1');
      out_frwd_clause := get_compress_clause(in_target_compression_rec);
      out_rlbk_clause := get_compress_clause(in_source_compression_rec);
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END comp_compression;

  -- Compares parallel attributes. Returns 0 if they are identical, otherwise returns 1
  FUNCTION comp_parallel(
    in_source_parallel_rec   IN cort_exec_pkg.gt_parallel_rec,
    in_target_parallel_rec   IN cort_exec_pkg.gt_parallel_rec,
    out_frwd_clause          OUT NOCOPY VARCHAR2,
    out_rlbk_clause          OUT NOCOPY VARCHAR2
  )
  RETURN PLS_INTEGER
  AS
    l_frwd_clause         VARCHAR2(32767);
    l_rlbk_clause         VARCHAR2(32767);
  BEGIN
    IF comp_value(in_source_parallel_rec.degree, in_target_parallel_rec.degree) = 1 OR
       comp_value(in_source_parallel_rec.instances, in_target_parallel_rec.instances) = 1
    THEN
      out_frwd_clause := get_clause(gc_degree, in_target_parallel_rec.degree)||get_clause(gc_instances, in_target_parallel_rec.instances);
      out_rlbk_clause := get_clause(gc_degree, in_source_parallel_rec.degree)||get_clause(gc_instances, in_source_parallel_rec.instances);
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END comp_parallel;

  FUNCTION get_subpart_template_clause(
    in_subpart_templ_arr  IN cort_exec_pkg.gt_subpartition_template_arr
  )
  RETURN CLOB
  AS
    l_clause           CLOB;
    l_lob_template_rec cort_exec_pkg.gt_lob_template_rec;
  BEGIN
    l_clause := ' SET SUBPARTITION TEMPLATE (';
    FOR i IN 1..in_subpart_templ_arr.COUNT LOOP
      l_clause := l_clause||' SUBPARTITION "'||in_subpart_templ_arr(i).subpartition_name||'" ';
      CASE in_subpart_templ_arr(i).subpartition_type
      WHEN 'LIST' THEN
        l_clause := l_clause ||'VALUES ('||in_subpart_templ_arr(i).high_bound||') ';
      WHEN 'RANGE' THEN
        l_clause := l_clause ||'VALUES LESS THAN ('||in_subpart_templ_arr(i).high_bound||') ';
      ELSE
        NULL;
      END CASE;
      IF cort_exec_pkg.g_params.tablespace.value_exists('SUBPARTITION') AND in_subpart_templ_arr(i).tablespace_name IS NOT NULL THEN
        l_clause := l_clause || 'TABLESPACE "'||in_subpart_templ_arr(i).tablespace_name||'" ';
      END IF;
      FOR j IN 1..in_subpart_templ_arr(i).lob_template_arr.COUNT LOOP
        l_lob_template_rec := in_subpart_templ_arr(i).lob_template_arr(j);
        l_clause := l_clause || 'LOB("'||l_lob_template_rec.lob_column_name||'") STORE AS "'||l_lob_template_rec.lob_segment_name||'" ';
        IF cort_exec_pkg.g_params.tablespace.value_exists('LOB') AND l_lob_template_rec.tablespace_name IS NOT NULL THEN
          l_clause := l_clause || '(TABLESPACE "'||l_lob_template_rec.tablespace_name||'") ';
        END IF;
      END LOOP;
      IF i < in_subpart_templ_arr.COUNT THEN
        l_clause := l_clause || ',' || CHR(10);
      END IF;
    END LOOP;
    l_clause := l_clause ||')';
    RETURN l_clause;
  END get_subpart_template_clause;


  -- compares two structures of tables. Returns:
  --   0 - if they are identical,
  --   1 - if they differs and table must be recreated,
  --   2 - if source table could be changed  by ALTER command(s). out_alter_stmt_arr is also returned
  FUNCTION comp_tables(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_result                 PLS_INTEGER;
    l_comp_result            PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;

    -- check all non-alterable params first:
    IF comp_value(in_source_table_rec.cluster_name, in_target_table_rec.cluster_name) = 1 THEN
      debug('Compare cluster_name - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.cluster_owner, in_target_table_rec.cluster_owner) = 1 THEN
      debug('Compare cluster_owner - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.iot_name, in_target_table_rec.iot_name) = 1 THEN
      debug('Compare iot_name - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.iot_type, in_target_table_rec.iot_type) = 1 THEN
      debug('Compare iot_type - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.overflow_table_name, in_target_table_rec.overflow_table_name) = 1 AND in_target_table_rec.overflow_table_name IS NULL THEN
      debug('Compare iot_overflow - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.table_type_owner, in_target_table_rec.table_type_owner) = 1 OR
       comp_value(in_source_table_rec.table_type, in_target_table_rec.table_type) = 1
    THEN
      debug('Compare table_type - 1');
      RETURN gc_result_recreate;
    END IF;
--    IF comp_value(in_source_table_rec.iot_index_name, in_target_table_rec.iot_index_name) = 1 THEN
--      debug('Compare iot_index_name - 1');
--      RETURN gc_result_recreate;
--    END IF;
    IF comp_value(in_source_table_rec.partitioning_type, in_target_table_rec.partitioning_type) = 1 THEN
      debug('Compare partitioning_type - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.subpartitioning_type, in_target_table_rec.subpartitioning_type) = 1 THEN
      debug('Compare subpartitioning_type - 1');
      RETURN gc_result_recreate;
    END IF;
    l_comp_result := comp_partitioning(
                         in_source_table_rec => in_source_table_rec,
                         in_target_table_rec => in_target_table_rec
                       ); 
    debug('comp_partitioning - '||l_comp_result);
    
                      
    IF in_source_table_rec.subpartitioning_type <> 'NONE' OR 
       in_target_table_rec.subpartitioning_type <> 'NONE' 
    THEN
      l_comp_result := comp_subpartitioning(
                         in_source_table_rec => in_source_table_rec,
                         in_target_table_rec => in_target_table_rec
                       );
      debug('comp_subpartitioning - '||l_comp_result);                  
    END IF;  

    IF l_comp_result > 0 THEN
      RETURN gc_result_recreate;
    END IF;  
    
    IF comp_value(in_source_table_rec.temporary, in_target_table_rec.temporary) = 1 THEN
      debug('Compare temporary - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.duration, in_target_table_rec.duration) = 1 THEN
      debug('Compare duration - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.secondary, in_target_table_rec.secondary) = 1 THEN
       debug('Compare secondary - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.nested, in_target_table_rec.nested) = 1 THEN
      debug('Compare nested - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_value(in_source_table_rec.dependencies, in_target_table_rec.dependencies) = 1 THEN
      debug('Compare dependencies - 1');
      RETURN gc_result_recreate;
    END IF;

    -- check partitioning key for virtual columns

    -- Non move
    -- Parallel
    IF comp_parallel(
         in_source_parallel_rec  => in_source_table_rec.parallel_rec,
         in_target_parallel_rec  => in_target_table_rec.parallel_rec,
         out_frwd_clause         => l_frwd_clause,
         out_rlbk_clause         => l_rlbk_clause
       ) = 1
    THEN
      l_result := greatest(gc_result_alter,l_result);
      debug('Compare parallel - '||l_result);
      l_frwd_clause_indx_arr(gc_parallel) := get_clause(gc_parallel, l_frwd_clause, '(', ')');
      l_rlbk_clause_indx_arr(gc_parallel) := get_clause(gc_parallel, l_rlbk_clause, '(', ')');
    END IF;

    -- cache
    IF comp_value(in_source_table_rec.cache, in_target_table_rec.cache) = 1 THEN
      l_result := greatest(gc_result_alter,l_result);
      debug('Compare cache - '||l_result);
      l_frwd_clause_indx_arr(gc_cache) := get_clause(gc_cache, in_target_table_rec.cache);
      l_rlbk_clause_indx_arr(gc_cache) := get_clause(gc_cache, in_source_table_rec.cache);
    END IF;

    -- row movement
    IF comp_value(in_source_table_rec.row_movement, in_target_table_rec.row_movement) = 1 THEN
      l_result := greatest(gc_result_alter,l_result);
      debug('Compare row movement - '||l_result);
      l_frwd_clause_indx_arr(gc_row_movement) := get_clause(gc_row_movement, in_target_table_rec.row_movement);
      l_rlbk_clause_indx_arr(gc_row_movement) := get_clause(gc_row_movement, in_source_table_rec.row_movement);
    END IF;

    -- monitoring
    IF comp_value(in_source_table_rec.monitoring, in_target_table_rec.monitoring) = 1 THEN
      l_result := greatest(gc_result_alter,l_result);
      debug('Compare monitoring - '||l_result);
      l_frwd_clause_indx_arr(gc_monitoring) := get_clause(gc_monitoring, in_target_table_rec.monitoring);
      l_rlbk_clause_indx_arr(gc_monitoring) := get_clause(gc_monitoring, in_source_table_rec.monitoring);
    END IF;

    -- MOVE only
    -- Tablespace
    IF cort_exec_pkg.g_params.tablespace.value_exists('TABLE') AND
       in_source_table_rec.partitioned = 'NO' AND
       comp_value(in_source_table_rec.tablespace_name, in_target_table_rec.tablespace_name) = 1
    THEN
      l_result := gc_result_alter_move;
      debug('Compare tablespace - 1');
      l_frwd_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_target_table_rec.tablespace_name, '"', '"');
      l_rlbk_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_source_table_rec.tablespace_name, '"', '"');
    END IF;


    -- Move or Alter
    -- Logging
    IF comp_value(in_source_table_rec.logging, in_target_table_rec.logging) = 1 AND
       in_target_table_rec.logging <> 'NONE'
    THEN
      IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
        l_result := gc_result_alter_move;
      ELSE
        l_result := gc_result_alter;
      END IF;
      debug('Compare logging - 1');
      debug('Source logging = "'||in_source_table_rec.logging||'"');
      debug('Target logging = "'||in_target_table_rec.logging||'"');
      debug('Logging clause = '||get_clause(gc_logging, in_target_table_rec.logging));
      l_frwd_clause_indx_arr(gc_logging) := get_clause(gc_logging, in_target_table_rec.logging);
      l_rlbk_clause_indx_arr(gc_logging) := get_clause(gc_logging, in_source_table_rec.logging);
    END IF;

    -- storage physical attributes (including storage)
    IF cort_exec_pkg.g_params.physical_attr.value_exists('TABLE') THEN
      l_comp_result := comp_physical_attr(
                         in_source_physical_attr_rec => in_source_table_rec.physical_attr_rec,
                         in_target_physical_attr_rec => in_target_table_rec.physical_attr_rec,
                         out_frwd_clause             => l_frwd_clause,
                         out_rlbk_clause             => l_rlbk_clause
                       );
    ELSE

      l_comp_result := gc_result_nochange;
    END IF;

    CASE l_comp_result
    WHEN gc_result_recreate THEN
      debug('Compare physical_attr_rec - recreate');
      RETURN gc_result_recreate;
    WHEN gc_result_alter THEN
      IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
        l_result := gc_result_alter_move;
      ELSE
        l_result := gc_result_alter;
      END IF;
      debug('Compare physical_attr_rec - '||l_result);
      l_frwd_clause_indx_arr(gc_physical_attr) := l_frwd_clause;
      l_rlbk_clause_indx_arr(gc_physical_attr) := l_rlbk_clause;
    ELSE
      NULL;
    END CASE;

    -- Compression
    IF comp_compression(
         in_source_compression_rec  => in_source_table_rec.compression_rec,
         in_target_compression_rec  => in_target_table_rec.compression_rec,
         out_frwd_clause            => l_frwd_clause,
         out_rlbk_clause            => l_rlbk_clause
       ) = 1
    THEN
      IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
        l_result := gc_result_alter_move;
      ELSE
        l_result := gc_result_alter;
      END IF;
      debug('Compare compression - '||l_result);
      l_frwd_clause_indx_arr(gc_compression) := l_frwd_clause;
      l_rlbk_clause_indx_arr(gc_compression) := l_rlbk_clause;
    END IF;

    -- IOT attribytes
    -- pctthreshold
    IF comp_value(in_source_table_rec.iot_pct_threshold, in_target_table_rec.iot_pct_threshold) = 1
    THEN
      IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
        l_result := gc_result_alter_move;
      ELSE
        l_result := gc_result_alter;
      END IF;
      debug('Compare IOT pctthreshold - '||l_result);
      l_frwd_clause_indx_arr(gc_pct_threshold) := get_clause(gc_pct_threshold, in_target_table_rec.iot_pct_threshold);
      l_rlbk_clause_indx_arr(gc_pct_threshold) := get_clause(gc_pct_threshold, in_source_table_rec.iot_pct_threshold);
    END IF;

    -- key compression
    IF comp_value(in_source_table_rec.iot_prefix_length, in_target_table_rec.iot_prefix_length) = 1 OR
       comp_value(in_source_table_rec.iot_key_compression, in_target_table_rec.iot_key_compression) = 1
    THEN
      IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
        l_result := gc_result_alter_move;
      ELSE
        l_result := gc_result_alter;
      END IF;
      debug('Compare IOT key compression - '||l_result);
      l_frwd_clause_indx_arr(gc_key_compression) := get_clause(gc_key_compression, in_target_table_rec.iot_key_compression, NULL, ' '||in_target_table_rec.iot_prefix_length);
      l_rlbk_clause_indx_arr(gc_key_compression) := get_clause(gc_key_compression, in_source_table_rec.iot_key_compression, NULL, ' '||in_source_table_rec.iot_prefix_length);
    END IF;

    -- mapping
    IF comp_value(in_source_table_rec.mapping_table, in_target_table_rec.mapping_table) = 1
    THEN
      IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
        l_result := gc_result_alter_move;
      ELSE
        l_result := gc_result_alter;
      END IF;
      debug('Compare IOT mappoing - '||l_result);
      l_frwd_clause_indx_arr(gc_mapping) := get_clause(gc_mapping, in_target_table_rec.mapping_table);
      l_rlbk_clause_indx_arr(gc_mapping) := get_clause(gc_mapping, in_source_table_rec.mapping_table);
    END IF;

    -- IOT overflow clause
    -- including column
    IF in_target_table_rec.overflow_table_name IS NOT NULL THEN
      debug(' overflow checks');

      IF in_source_table_rec.overflow_table_name IS NULL
      THEN
        debug(' add overflow segment ');
        IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
          l_result := gc_result_alter_move;
        ELSE
          l_result := gc_result_alter;
        END IF;
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => in_source_table_rec.table_name,
          in_owner         => in_source_table_rec.owner,
          in_frwd_clause   => gc_add_overflow,
          in_rlbk_clause   => NULL
        );
      END IF;

      IF comp_value(in_source_table_rec.iot_include_column, in_target_table_rec.iot_include_column) = 1
      THEN
        IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
          l_result := gc_result_alter_move;
        ELSE
          l_result := gc_result_alter;
        END IF;
        debug('Compare IOT overflow - '||l_result);
        l_frwd_clause_indx_arr(gc_overflow) := gc_overflow;
        l_rlbk_clause_indx_arr(gc_overflow) := gc_overflow;
        l_frwd_clause_indx_arr(gc_including_column) := get_clause(gc_including_column, in_target_table_rec.iot_include_column);
        l_rlbk_clause_indx_arr(gc_including_column) := get_clause(gc_including_column, in_source_table_rec.iot_include_column);
      END IF;

      -- overflow tablespace
      IF cort_exec_pkg.g_params.tablespace.value_exists('TABLE') AND
         comp_value(in_source_table_rec.overflow_tablespace, in_target_table_rec.overflow_tablespace) = 1
      THEN
        IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
          l_result := gc_result_alter_move;
        ELSE
          l_result := gc_result_alter;
        END IF;
        debug('Compare IOT overflow tablespace - '||l_result);
        l_frwd_clause_indx_arr(gc_overflow) := gc_overflow;
        l_rlbk_clause_indx_arr(gc_overflow) := gc_overflow;
        l_frwd_clause_indx_arr(gc_overflow_tablespace) := get_clause(gc_tablespace, in_target_table_rec.overflow_tablespace, '"', '"');
        l_rlbk_clause_indx_arr(gc_overflow_tablespace) := get_clause(gc_tablespace, in_source_table_rec.overflow_tablespace, '"', '"');
      END IF;

      -- overflow logging
      IF comp_value(in_source_table_rec.overflow_logging, in_target_table_rec.overflow_logging) = 1 AND
         in_target_table_rec.overflow_logging <> 'NONE'
      THEN
        IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
          l_result := gc_result_alter_move;
        ELSE
          l_result := gc_result_alter;
        END IF;
        debug('Compare IOT overflow logging - '||l_result);
        l_frwd_clause_indx_arr(gc_overflow) := gc_overflow;
        l_rlbk_clause_indx_arr(gc_overflow) := gc_overflow;
        l_frwd_clause_indx_arr(gc_overflow_logging) := get_clause(gc_logging, in_target_table_rec.overflow_logging);
        l_rlbk_clause_indx_arr(gc_overflow_logging) := get_clause(gc_logging, in_source_table_rec.overflow_logging);
      END IF;

      -- Overflow storage physical attributes (including storage)
      IF cort_exec_pkg.g_params.physical_attr.value_exists('INDEX') THEN
        l_comp_result := comp_index_physical_attr(
                           in_source_physical_attr_rec => in_source_table_rec.overflow_physical_attr_rec,
                           in_target_physical_attr_rec => in_target_table_rec.overflow_physical_attr_rec,
                           out_frwd_clause             => l_frwd_clause,
                           out_rlbk_clause             => l_rlbk_clause
                         );

        CASE l_comp_result
        WHEN gc_result_recreate THEN
          debug('Compare IOT overflow physical_attr_rec - recreate');
          RETURN gc_result_recreate;
        WHEN gc_result_alter THEN
          IF cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
            l_result := gc_result_alter_move;
          ELSE
            l_result := gc_result_alter;
          END IF;
          debug('Compare IOT overflow physical_attr_rec - alter');
          l_frwd_clause_indx_arr(gc_overflow) := gc_overflow;
          l_rlbk_clause_indx_arr(gc_overflow) := gc_overflow;
          l_frwd_clause_indx_arr(gc_overflow_physical_attr) := l_frwd_clause;
          l_rlbk_clause_indx_arr(gc_overflow_physical_attr) := l_rlbk_clause;
        ELSE
          NULL;
        END CASE;
      END IF;
    -- end of overflow
    END IF;


    IF l_result = gc_result_alter_move THEN
      l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_parallel)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_cache)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_row_movement)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_monitoring);
      l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_parallel)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_cache)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_row_movement)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_monitoring);

      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );

      l_frwd_clause := gc_move||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_tablespace)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_logging)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_physical_attr)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_compression)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_pct_threshold)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_key_compression)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_mapping)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_including_column)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_tablespace)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_logging)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_physical_attr);

      l_rlbk_clause := gc_move||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_tablespace)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_logging)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_physical_attr)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_compression)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_pct_threshold)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_key_compression)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_mapping)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_including_column)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_tablespace)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_logging)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_physical_attr);

      IF cort_exec_pkg.g_params.parallel.get_num_value > 1 THEN
        l_frwd_clause := l_frwd_clause||get_clause(gc_parallel, cort_exec_pkg.g_params.parallel.get_num_value);
        l_rlbk_clause := l_rlbk_clause||get_clause(gc_parallel, cort_exec_pkg.g_params.parallel.get_num_value);
      END IF;


      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    ELSIF l_result = gc_result_alter THEN
      l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_logging)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_physical_attr)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_compression)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_parallel)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_cache)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_row_movement)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_monitoring)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_tablespace)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_logging)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_physical_attr);
      l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_logging)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_physical_attr)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_compression)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_parallel)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_cache)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_row_movement)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_monitoring)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_tablespace)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_logging)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_physical_attr);

      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    -- Individual alters for partitioned table
    IF in_source_table_rec.partitioned = 'YES' THEN
      IF comp_value(in_source_table_rec.interval, in_target_table_rec.interval) = 1 THEN
        l_result := greatest(gc_result_alter, l_result);
        debug('Compare partitioning interval - 1');
        l_frwd_clause := get_clause(gc_interval, nvl(in_target_table_rec.interval,' '), '(', ')');
        l_rlbk_clause := get_clause(gc_interval, nvl(in_source_table_rec.interval,' '), '(', ')');
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => in_source_table_rec.table_name,
          in_owner         => in_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END IF;

      IF cort_exec_pkg.g_params.tablespace.value_exists('TABLE')  AND
         comp_value(in_source_table_rec.tablespace_name, in_target_table_rec.tablespace_name) = 1 THEN
        l_result := greatest(gc_result_alter, l_result);
        debug('Compare partitioning table tablespace - 1');
        l_frwd_clause := get_clause(gc_modify_default_attrs, get_clause(gc_tablespace, in_target_table_rec.tablespace_name, '"', '"'));
        l_rlbk_clause := get_clause(gc_modify_default_attrs, get_clause(gc_tablespace, in_source_table_rec.tablespace_name, '"', '"'));
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => in_source_table_rec.table_name,
          in_owner         => in_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END IF;

      l_frwd_clause := get_subpart_template_clause(in_target_table_rec.subpartition_template_arr);
      l_rlbk_clause := get_subpart_template_clause(in_source_table_rec.subpartition_template_arr);
      IF comp_value(l_frwd_clause, l_rlbk_clause) = 1 THEN
        l_result := greatest(gc_result_alter, l_result);
        debug('Compare partitioning table subpartition template - 1');
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => in_source_table_rec.table_name,
          in_owner         => in_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END IF;
    END IF;

    RETURN l_result;

  END comp_tables;

  -- compares column name dependent table attributes
  FUNCTION comp_table_col_attrs(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;
    IF comp_array(in_source_table_rec.iot_pk_column_arr, in_target_table_rec.iot_pk_column_arr) = 1 THEN
      debug('Compare iot PK key columns - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_array(in_source_table_rec.iot_pk_column_sort_type_arr, in_target_table_rec.iot_pk_column_sort_type_arr) = 1 THEN
      debug('Compare iot PK key sort types - 1');
      RETURN gc_result_recreate;
    END IF;
    IF comp_ref_ptn_constraint(in_source_table_rec, in_target_table_rec) = 1 THEN
      debug('Compare ref_ptn_constraint - 1');
      RETURN gc_result_recreate;
    END IF;

    RETURN l_result;
  END comp_table_col_attrs;

  -- return lob segment name if it is user generated. otherwise return NULL
  FUNCTION get_lob_segment_name(in_lob_rec IN cort_exec_pkg.gt_lob_rec)
  RETURN VARCHAR2
  AS
  BEGIN
    IF in_lob_rec.rename_rec.generated = 'N' THEN
      RETURN '"'||in_lob_rec.lob_name||'"';
    ELSE
      RETURN NULL;
    END IF;
  END get_lob_segment_name;

  -- return LOB params clause
  FUNCTION get_lob_storage_params(in_lob_rec IN cort_exec_pkg.gt_lob_rec)
  RETURN CLOB
  AS
    l_clause_indx_arr  arrays.gt_xlstr_indx;
  BEGIN
      -- move only
    l_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_lob_rec.tablespace_name, '"', '"');
    l_clause_indx_arr(gc_chunk) := get_clause(gc_chunk, in_lob_rec.chunk);
    l_clause_indx_arr(gc_in_row) := get_clause(gc_in_row, in_lob_rec.in_row);
    IF in_lob_rec.securefile = 'NO' THEN
      l_clause_indx_arr(gc_pctversion) := get_clause(gc_pctversion, in_lob_rec.pctversion);
    END IF;
    l_clause_indx_arr(gc_freepools) := get_clause(gc_freepools, in_lob_rec.freepools);
    IF in_lob_rec.securefile = 'YES' THEN
      l_clause_indx_arr(gc_retention) := get_clause(gc_retention, in_lob_rec.retention, null, case when in_lob_rec.retention = 'MIN' then to_char(in_lob_rec.min_retention) end);
    END IF;
    l_clause_indx_arr(gc_deduplication) := get_clause(gc_deduplication, in_lob_rec.deduplication);
    l_clause_indx_arr(gc_lob_compression) := get_clause(gc_lob_compression, in_lob_rec.compression);
    l_clause_indx_arr(gc_encrypt) := get_clause(gc_encrypt, in_lob_rec.encrypt);
    l_clause_indx_arr(gc_cache) := get_clause(gc_cache, in_lob_rec.cache);
    l_clause_indx_arr(gc_logging) := get_clause(gc_logging, in_lob_rec.logging);
    l_clause_indx_arr(gc_storage) := get_clause(gc_storage, get_lob_storage_clause(in_lob_rec.storage), '(', ')');
    RETURN get_clause_by_name(l_clause_indx_arr, gc_tablespace)||
           get_clause_by_name(l_clause_indx_arr, gc_in_row)||
           get_clause_by_name(l_clause_indx_arr, gc_chunk)||
           get_clause_by_name(l_clause_indx_arr, gc_pctversion)||
           get_clause_by_name(l_clause_indx_arr, gc_freepools)||
           get_clause_by_name(l_clause_indx_arr, gc_retention)||
           get_clause_by_name(l_clause_indx_arr, gc_deduplication)||
           get_clause_by_name(l_clause_indx_arr, gc_lob_compression)||
           get_clause_by_name(l_clause_indx_arr, gc_encrypt)||
           get_clause_by_name(l_clause_indx_arr, gc_cache)||
           get_clause_by_name(l_clause_indx_arr, gc_logging)||
           get_clause_by_name(l_clause_indx_arr, gc_storage);

  END get_lob_storage_params;

  FUNCTION get_substitution_column_clause(in_column_rec IN cort_exec_pkg.gt_column_rec)
  RETURN VARCHAR2
  AS
  BEGIN
    -- Reserved
    RETURN NULL;
  END get_substitution_column_clause;

  FUNCTION get_column_properties(
    in_table_rec  IN cort_exec_pkg.gt_table_rec,
    in_column_rec IN cort_exec_pkg.gt_column_rec
  )
  RETURN CLOB
  AS
    l_clause_indx_arr  arrays.gt_xlstr_indx;
    l_clause           CLOB;
    l_column_name      arrays.gt_name;
    l_column_indx      PLS_INTEGER;
    l_lob_rec          cort_exec_pkg.gt_lob_rec;
    l_xml_rec          cort_exec_pkg.gt_xml_col_rec;
    l_varray_rec       cort_exec_pkg.gt_varray_rec;
  BEGIN
    l_column_name := in_column_rec.column_name;
    l_column_indx := in_column_rec.column_indx;
    CASE
    WHEN in_column_rec.data_type IN ('CLOB','BLOB','NCLOB') THEN
      IF in_table_rec.lob_arr.EXISTS(l_column_indx)
      THEN
        l_lob_rec := in_table_rec.lob_arr(l_column_indx);
        l_clause := get_clause(gc_lob_item, in_column_rec.column_name, '(', ')')||
                    get_clause(gc_store_as, get_clause(gc_securefile, l_lob_rec.securefile))||
                    get_clause(gc_lob_name, get_lob_segment_name(l_lob_rec))||
                    get_clause(gc_lob_params, get_lob_storage_params(l_lob_rec), '(', ')');
      END IF;
    WHEN in_column_rec.data_type = 'XMLTYPE' THEN
      IF in_table_rec.xml_col_arr.EXISTS(l_column_indx) THEN
        l_xml_rec := in_table_rec.xml_col_arr(l_column_indx);
        l_clause_indx_arr(gc_xml_type_column) := get_clause(gc_xml_type_column, in_column_rec.column_name, '"', '"');
        IF l_xml_rec.lob_column_indx IS NOT NULL AND
           in_table_rec.lob_arr.EXISTS(l_xml_rec.lob_column_indx)
        THEN
          l_lob_rec := in_table_rec.lob_arr(l_xml_rec.lob_column_indx);
          l_clause_indx_arr(gc_securefile) := get_clause(gc_securefile, l_lob_rec.securefile);
          l_clause_indx_arr(gc_lob_name) := get_clause(gc_lob_name, get_lob_segment_name(l_lob_rec));
          l_clause_indx_arr(gc_lob_params) := get_clause(gc_lob_params, get_lob_storage_params(l_lob_rec), '(', ')');
        END IF;
        l_clause_indx_arr(gc_store_as) := gc_store_as;
        l_clause_indx_arr(gc_xml_storage_type) := get_clause(gc_xml_storage_type, l_xml_rec.storage_type, get_clause_by_name(l_clause_indx_arr, gc_securefile));
        l_clause_indx_arr(gc_xml_schema) := get_clause(gc_xml_schema, l_xml_rec.xmlschema, '"', '"');
        l_clause_indx_arr(gc_xml_element) := get_clause(gc_xml_element, l_xml_rec.element_name, '"', '"');
        l_clause_indx_arr(gc_xml_anyschema) := get_clause(gc_xml_anyschema, l_xml_rec.anyschema);
        l_clause_indx_arr(gc_xml_nonschema) := get_clause(gc_xml_nonschema, l_xml_rec.nonschema);

        l_clause := get_clause_by_name(l_clause_indx_arr, gc_xml_type_column)||
                    get_clause_by_name(l_clause_indx_arr, gc_store_as)||
                    get_clause_by_name(l_clause_indx_arr, gc_xml_storage_type)||
                    get_clause_by_name(l_clause_indx_arr, gc_xml_schema)||
                    get_clause_by_name(l_clause_indx_arr, gc_xml_element)||
                    get_clause_by_name(l_clause_indx_arr, gc_xml_anyschema)||
                    get_clause_by_name(l_clause_indx_arr, gc_xml_nonschema);

      END IF;
    ELSE -- varray
      IF in_table_rec.varray_arr.EXISTS(l_column_indx)
      THEN
        l_column_indx := in_table_rec.column_indx_arr(l_column_name);
        l_varray_rec := in_table_rec.varray_arr(l_column_indx);
        l_clause_indx_arr(gc_varray) := get_clause(gc_varray, in_column_rec.column_name, '"', '"');
        IF l_varray_rec.lob_column_indx IS NOT NULL AND
           in_table_rec.lob_arr.EXISTS(l_varray_rec.lob_column_indx)
        THEN
          l_lob_rec := in_table_rec.lob_arr(l_varray_rec.lob_column_indx);
          l_clause_indx_arr(gc_store_as) := get_clause(gc_store_as, get_clause(gc_securefile, l_lob_rec.securefile));
          l_clause_indx_arr(gc_lob_item) := get_clause(gc_lob_item, get_lob_segment_name(l_lob_rec));
          l_clause_indx_arr(gc_lob_params) := get_clause(gc_lob_params, get_lob_storage_params(l_lob_rec), '(', ')');
        END IF;
        l_clause := get_clause_by_name(l_clause_indx_arr, gc_varray)||
                    get_clause_by_name(l_clause_indx_arr, gc_store_as)||
                    get_clause_by_name(l_clause_indx_arr, gc_lob_item)||
                    get_clause_by_name(l_clause_indx_arr, gc_lob_params);
      END IF;
    END CASE;
    RETURN l_clause;
  END get_column_properties;

  -- return column clause
  FUNCTION get_column_clause(
    in_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_column_rec  IN cort_exec_pkg.gt_column_rec
  )
  RETURN CLOB
  AS
    l_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    IF in_column_rec.virtual_column = 'NO' THEN
      l_clause_indx_arr(gc_data_type) := get_column_type_clause(in_column_rec);
      l_clause_indx_arr(gc_default) := get_clause(gc_default, in_column_rec.data_default);
      l_clause_indx_arr(gc_encryption) := get_clause(gc_encryption, in_column_rec.encryption_alg) || get_clause(gc_salt, in_column_rec.salt);
      l_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_column_rec.nullable);

      RETURN '"'||in_column_rec.column_name||'" '||
             get_clause_by_name(l_clause_indx_arr, gc_data_type)||
             get_clause_by_name(l_clause_indx_arr, gc_default)||
             get_clause_by_name(l_clause_indx_arr, gc_encryption)||
             get_clause_by_name(l_clause_indx_arr, gc_nullable);
    ELSE
      l_clause_indx_arr(gc_generated_always_as) := get_clause(gc_generated_always_as, in_column_rec.data_default, '(', ')');
      l_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_column_rec.nullable);

      RETURN '"'||in_column_rec.column_name||'" '||
             get_clause_by_name(l_clause_indx_arr, gc_generated_always_as)||
             get_clause_by_name(l_clause_indx_arr, gc_nullable);
    END IF;
  END get_column_clause;

  -- return column clause
  FUNCTION get_add_column_clause(
    in_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_column_rec  IN cort_exec_pkg.gt_column_rec
  )
  RETURN CLOB
  AS
    l_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    IF in_column_rec.virtual_column = 'NO' THEN
      l_clause_indx_arr(gc_data_type) := get_column_type_clause(in_column_rec);
      l_clause_indx_arr(gc_default) := get_clause(gc_default, in_column_rec.data_default);
      l_clause_indx_arr(gc_encryption) := get_clause(gc_encryption, in_column_rec.encryption_alg) || get_clause(gc_salt, in_column_rec.salt);
      l_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_column_rec.nullable);

      l_clause_indx_arr(gc_column_properties) := get_column_properties(in_table_rec, in_column_rec);

      RETURN '"'||in_column_rec.column_name||'" '||
             get_clause_by_name(l_clause_indx_arr, gc_data_type)||
             get_clause_by_name(l_clause_indx_arr, gc_default)||
             get_clause_by_name(l_clause_indx_arr, gc_encryption)||
             get_clause_by_name(l_clause_indx_arr, gc_nullable)||
             get_clause_by_name(l_clause_indx_arr, gc_column_properties);
    ELSE
      l_clause_indx_arr(gc_generated_always_as) := get_clause(gc_generated_always_as, in_column_rec.data_default, '(', ')');
      l_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_column_rec.nullable);

      RETURN '"'||in_column_rec.column_name||'" '||
             get_clause_by_name(l_clause_indx_arr, gc_generated_always_as)||
             get_clause_by_name(l_clause_indx_arr, gc_nullable);
    END IF;
  END get_add_column_clause;

  -- Add column.
  PROCEDURE add_column(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_column_rec          IN cort_exec_pkg.gt_column_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    l_frwd_clause := gc_add_column||' '||get_add_column_clause(in_target_table_rec, in_column_rec);
    l_rlbk_clause := get_clause(gc_drop_column, in_column_rec.column_name, '"', '"');

    add_alter_table_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_table_name    => in_source_table_rec.table_name,
      in_owner         => in_source_table_rec.owner,
      in_frwd_clause   => l_frwd_clause,
      in_rlbk_clause   => l_rlbk_clause
    );
  END add_column;

  -- Drop column
  PROCEDURE drop_column(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_column_rec          IN cort_exec_pkg.gt_column_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_column_rec             cort_exec_pkg.gt_column_rec;
  BEGIN

    CASE cort_exec_pkg.g_params.drop_column.get_value
    WHEN 'DROP' THEN
      l_frwd_clause := get_clause(gc_drop_column, in_column_rec.column_name, '"', '"')||gc_cascade_constraints;
      l_rlbk_clause := NULL;
    WHEN 'SET_UNUSED' THEN
      l_frwd_clause := get_clause(gc_set_unused_column, in_column_rec.column_name, '"', '"')||gc_cascade_constraints;
      l_rlbk_clause := NULL;
    WHEN 'SET_INVISIBLE' THEN
      l_frwd_clause := get_clause(gc_modify, in_column_rec.column_name, '"', '"')||gc_invisible;
      l_rlbk_clause := get_clause(gc_modify, in_column_rec.column_name, '"', '"')||gc_visible;
    WHEN 'RECREATE' THEN
       NULL;
    END CASE;

    IF cort_exec_pkg.g_params.drop_column.get_value IN ('DROP','SET_UNUSED') AND
       cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER')
    THEN
      l_frwd_clause := get_clause(gc_set_unused_column, in_column_rec.column_name, '"', '"')||gc_cascade_constraints;
      l_column_rec := in_column_rec;
      IF l_column_rec.nullable = 'N' AND l_column_rec.data_default IS NULL THEN
        l_column_rec.nullable := 'Y';
      END IF;
      l_rlbk_clause := gc_add_column||' '||get_add_column_clause(in_source_table_rec, l_column_rec);
    END IF;

    add_alter_table_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_table_name    => in_source_table_rec.table_name,
      in_owner         => in_source_table_rec.owner,
      in_frwd_clause   => l_frwd_clause,
      in_rlbk_clause   => l_rlbk_clause
    );
  END drop_column;


  -- Find column in string array
  FUNCTION find_column(
    in_column_name_arr IN arrays.gt_name_arr,
    in_column_name     IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
  BEGIN
    FOR i IN 1..in_column_name_arr.COUNT LOOP
      IF in_column_name_arr(i) = in_column_name THEN
        RETURN TRUE;
      END IF;
    END LOOP;
    RETURN FALSE;
  END find_column;

  FUNCTION find_column(
    in_column_name_arr IN arrays.gt_lstr_arr,
    in_column_name     IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
  BEGIN
    FOR i IN 1..in_column_name_arr.COUNT LOOP
      IF in_column_name_arr(i) = in_column_name OR
         in_column_name_arr(i) LIKE '%"'||in_column_name||'"%'
      THEN
        RETURN TRUE;
      END IF;
    END LOOP;
    RETURN FALSE;
  END find_column;

  FUNCTION find_column(
    in_column_name_arr IN arrays.gt_xlstr_arr,
    in_column_name     IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
  BEGIN
    FOR i IN 1..in_column_name_arr.COUNT LOOP
      IF in_column_name_arr(i) = in_column_name OR
         in_column_name_arr(i) LIKE '%"'||in_column_name||'"%'
      THEN
        RETURN TRUE;
      END IF;
    END LOOP;
    RETURN FALSE;
  END find_column;

  -- replace names for renaming column in expressions for virtual columns, index records, constraint records, partition records?
  PROCEDURE update_refs_on_renamed_columns(
    io_table_rec IN OUT NOCOPY cort_exec_pkg.gt_table_rec
  )
  AS
    l_renamed     arrays.gt_name_indx;
    l_col_name    arrays.gt_name;

    PROCEDURE update_column_arr(
      io_column_arr    IN OUT NOCOPY arrays.gt_name_arr,
      in_replace_names IN arrays.gt_name_indx
    )
    AS
    BEGIN
      FOR i IN 1..io_column_arr.COUNT LOOP
        l_col_name := io_column_arr(i);
        IF in_replace_names.EXISTS(l_col_name) THEN
          debug('replace column '||io_column_arr(i)||' with '||in_replace_names(l_col_name));
          io_column_arr(i) := in_replace_names(l_col_name);
        END IF;
      END LOOP;
    END update_column_arr;

  BEGIN
    FOR i IN 1..io_table_rec.column_arr.COUNT LOOP
      IF io_table_rec.column_arr(i).virtual_column = 'NO' AND
         io_table_rec.column_arr(i).hidden_column = 'NO' AND
         io_table_rec.column_arr(i).new_column_name IS NOT NULL
      THEN
        debug('updating references for renamed column '||io_table_rec.column_arr(i).column_name||' - new name = '||io_table_rec.column_arr(i).new_column_name);
        l_renamed(io_table_rec.column_arr(i).column_name) := io_table_rec.column_arr(i).new_column_name;
      END IF;
    END LOOP;

    IF l_renamed.COUNT = 0 THEN
      RETURN;
    END IF;

    update_column_arr(
      io_column_arr    => io_table_rec.iot_pk_column_arr,
      in_replace_names => l_renamed
    );
    update_column_arr(
      io_column_arr    => io_table_rec.part_key_column_arr,
      in_replace_names => l_renamed
    );
    update_column_arr(
      io_column_arr    => io_table_rec.subpart_key_column_arr,
      in_replace_names => l_renamed
    );

    -- update virtual column expressions
    FOR i IN 1..io_table_rec.column_arr.COUNT LOOP
      IF io_table_rec.column_arr(i).virtual_column = 'YES' AND io_table_rec.column_arr(i).hidden_column = 'NO' AND io_table_rec.column_arr(i).data_default IS NOT NULL THEN
        cort_parse_pkg.update_expression(
          io_expression    => io_table_rec.column_arr(i).data_default,
          in_replace_names => l_renamed
        );
      END IF;
    END LOOP;

    -- update constraint columns
    FOR i IN 1..io_table_rec.constraint_arr.COUNT LOOP
      update_column_arr(
        io_column_arr    => io_table_rec.constraint_arr(i).column_arr,
        in_replace_names => l_renamed
      );

      -- update ref columns for self-referencing FK constraints
      IF io_table_rec.constraint_arr(i).constraint_type = 'R' AND
         io_table_rec.constraint_arr(i).r_owner = io_table_rec.owner AND
         io_table_rec.constraint_arr(i).r_table_name = io_table_rec.table_name
      THEN
        update_column_arr(
          io_column_arr    => io_table_rec.constraint_arr(i).r_column_arr,
          in_replace_names => l_renamed
        );
      END IF;
    END LOOP;

    -- update reference constraint columns
    FOR i IN 1..io_table_rec.ref_constraint_arr.COUNT LOOP
      update_column_arr(
        io_column_arr    => io_table_rec.ref_constraint_arr(i).r_column_arr,
        in_replace_names => l_renamed
      );
    END LOOP;

    -- update log groups
    FOR i IN 1..io_table_rec.log_group_arr.COUNT LOOP
      FOR j IN 1..io_table_rec.log_group_arr(i).column_arr.COUNT LOOP
        l_col_name := io_table_rec.log_group_arr(i).column_arr(j);
        IF l_renamed.EXISTS(l_col_name) THEN
          io_table_rec.log_group_arr(i).column_arr(j) := l_renamed(l_col_name);
        END IF;
      END LOOP;
    END LOOP;

    -- update index columns
    FOR i IN 1..io_table_rec.index_arr.COUNT LOOP
      debug('updating index '||io_table_rec.index_arr(i).index_name);
      update_column_arr(
        io_column_arr    => io_table_rec.index_arr(i).column_arr,
        in_replace_names => l_renamed
      );
      update_column_arr(
        io_column_arr    => io_table_rec.index_arr(i).part_key_column_arr,
        in_replace_names => l_renamed
      );
      update_column_arr(
        io_column_arr    => io_table_rec.index_arr(i).subpart_key_column_arr,
        in_replace_names => l_renamed
      );
      FOR j IN 1..io_table_rec.index_arr(i).column_expr_arr.COUNT LOOP
        IF io_table_rec.index_arr(i).column_expr_arr(j) IS NOT NULL THEN
          cort_parse_pkg.update_expression(
            io_expression    => io_table_rec.index_arr(i).column_expr_arr(j),
            in_replace_names => l_renamed
          );
        END IF;
      END LOOP;

      io_table_rec.index_arr(i).recreate_flag := io_table_rec.index_arr(i).join_index = 'YES';
    END LOOP;

    -- update join-index columns
    FOR i IN 1..io_table_rec.join_index_arr.COUNT LOOP
      debug('updating join index '||io_table_rec.join_index_arr(i).index_name);

      FOR j IN 1..io_table_rec.join_index_arr(i).column_arr.COUNT LOOP
        l_col_name := io_table_rec.join_index_arr(i).column_arr(j);
        IF l_renamed.EXISTS(l_col_name) THEN
          io_table_rec.join_index_arr(i).column_arr(j) := l_renamed(l_col_name);
          io_table_rec.join_index_arr(i).recreate_flag := TRUE;
          debug('replace column '||io_table_rec.join_index_arr(i).column_arr(j) ||' with '||l_renamed(l_col_name));
        END IF;
      END LOOP;

      FOR j IN 1..io_table_rec.join_index_arr(i).join_inner_column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).join_inner_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).join_inner_table_arr(j) = io_table_rec.rename_rec.object_name
        THEN
          l_col_name := io_table_rec.join_index_arr(i).join_inner_column_arr(j);
          IF l_renamed.EXISTS(l_col_name) THEN
            io_table_rec.join_index_arr(i).join_inner_column_arr(j) := l_renamed(l_col_name);
            io_table_rec.join_index_arr(i).recreate_flag := TRUE;
            debug('replace join inner column '||io_table_rec.join_index_arr(i).join_inner_column_arr(j) ||' with '||l_renamed(l_col_name));
          END IF;
        END IF;
      END LOOP;
      FOR j IN 1..io_table_rec.join_index_arr(i).join_outer_column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).join_outer_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).join_outer_table_arr(j) = io_table_rec.rename_rec.object_name
        THEN
          l_col_name := io_table_rec.join_index_arr(i).join_outer_column_arr(j);
          IF l_renamed.EXISTS(l_col_name) THEN
            io_table_rec.join_index_arr(i).join_outer_column_arr(j) := l_renamed(l_col_name);
            io_table_rec.join_index_arr(i).recreate_flag := TRUE;
            debug('replace join outer column '||io_table_rec.join_index_arr(i).join_outer_column_arr(j) ||' with '||l_renamed(l_col_name));
          END IF;
        END IF;
      END LOOP;
    END LOOP;

  END update_refs_on_renamed_columns;


  -- mark constraints, indexes, log_groups refer to given column as dropped
  PROCEDURE drop_column_refereces(
    io_table_rec    IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    in_column_rec   IN cort_exec_pkg.gt_column_rec
  )
  AS
  BEGIN
    FOR i IN 1..io_table_rec.constraint_arr.COUNT LOOP
      IF find_column(io_table_rec.constraint_arr(i).column_arr, in_column_rec.column_name) THEN
        io_table_rec.constraint_arr(i).drop_flag := TRUE;
      END IF;
    END LOOP;
    FOR i IN 1..io_table_rec.index_arr.COUNT LOOP
      IF find_column(io_table_rec.index_arr(i).column_arr, in_column_rec.column_name) THEN
        io_table_rec.index_arr(i).drop_flag := TRUE;
      END IF;
    END LOOP;
    FOR i IN 1..io_table_rec.log_group_arr.COUNT LOOP
      IF find_column(io_table_rec.log_group_arr(i).column_arr, in_column_rec.column_name) THEN
        io_table_rec.index_arr(i).drop_flag := TRUE;
      END IF;
    END LOOP;

    -- update join-index columns
    FOR i IN 1..io_table_rec.join_index_arr.COUNT LOOP
      FOR j IN 1..io_table_rec.join_index_arr(i).column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).column_arr(j) = in_column_rec.column_name THEN
          io_table_rec.join_index_arr(i).drop_flag := TRUE;
        END IF;
      END LOOP;
      FOR j IN 1..io_table_rec.join_index_arr(i).join_inner_column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).join_inner_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).join_inner_table_arr(j) = io_table_rec.table_name AND
           io_table_rec.join_index_arr(i).join_inner_column_arr(j) = in_column_rec.column_name
        THEN
          io_table_rec.join_index_arr(i).drop_flag := TRUE;
        END IF;
      END LOOP;
      FOR j IN 1..io_table_rec.join_index_arr(i).join_outer_column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).join_outer_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).join_outer_table_arr(j) = io_table_rec.table_name AND
           io_table_rec.join_index_arr(i).join_outer_column_arr(j) = in_column_rec.column_name
        THEN
          io_table_rec.join_index_arr(i).drop_flag := TRUE;
        END IF;
      END LOOP;
    END LOOP;

  END drop_column_refereces;

  -- compare one column
  FUNCTION comp_column(
    io_source_table_rec    IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    in_source_column_rec   IN cort_exec_pkg.gt_column_rec,
    in_target_column_rec   IN cort_exec_pkg.gt_column_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- rollback alter statements
    in_check_name          IN BOOLEAN DEFAULT TRUE
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
    l_skip_comp_data_length  BOOLEAN := FALSE;
    l_revert_flag            BOOLEAN := TRUE;
  BEGIN
    debug('compare columns: '||in_source_column_rec.column_name||' and '||in_target_column_rec.column_name);
    l_result := gc_result_nochange;

    IF comp_value(in_source_column_rec.segment_column_id, in_target_column_rec.segment_column_id) = 1 AND
       NOT cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER')
    THEN
      debug('Column '||in_source_column_rec.column_name||' - changed segment position');
      RETURN gc_result_recreate;
    END IF;

    IF comp_value(in_source_column_rec.virtual_column, in_target_column_rec.virtual_column) = 1
    THEN
      debug('Column '||in_source_column_rec.column_name||' - changed column type: segment/virtual');
      RETURN gc_result_recreate;
    END IF;

    l_frwd_clause_indx_arr(gc_column_name) := get_clause(gc_modify_column, in_source_column_rec.column_name, '"', '"');
    l_rlbk_clause_indx_arr(gc_column_name) := get_clause(gc_modify_column, in_source_column_rec.column_name, '"', '"');
    IF in_source_column_rec.virtual_column = 'YES' THEN
      -- compare virtual columns
      -- Check expression
      IF comp_value(in_source_column_rec.data_default, in_target_column_rec.data_default) = 1
      THEN
        debug('Virtual column '||in_source_column_rec.column_name||' - changed expression');
        -- check if virtual column included into partitioning key
        IF in_source_column_rec.partition_key THEN
          RETURN gc_result_exchange;
        ELSE
          l_result := gc_result_alter;
          l_frwd_clause_indx_arr(gc_generated_always_as) := get_clause(gc_generated_always_as, in_target_column_rec.data_default, '(', ')');
          l_rlbk_clause_indx_arr(gc_generated_always_as) := get_clause(gc_generated_always_as, in_source_column_rec.data_default, '(', ')');
        END IF;
      END IF;

      -- Check nullability value
      IF comp_value(in_source_column_rec.nullable, in_target_column_rec.nullable) = 1 THEN
        l_result := gc_result_alter;
        IF in_target_column_rec.notnull_constraint_name IS NOT NULL THEN
          l_frwd_clause_indx_arr(gc_constraint) := get_clause(gc_constraint, in_target_column_rec.notnull_constraint_name, '"', '"');
        END IF;
        IF in_source_column_rec.notnull_constraint_name IS NOT NULL THEN
          l_rlbk_clause_indx_arr(gc_constraint) := get_clause(gc_constraint, in_source_column_rec.notnull_constraint_name, '"', '"');
        END IF;
        l_frwd_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_target_column_rec.nullable);
        l_rlbk_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_source_column_rec.nullable);
      END IF;
      -- end of virtual column section
    ELSE
      -- segment columns
      -- check data type
      IF comp_value(in_source_column_rec.data_type, in_target_column_rec.data_type) = 1 OR
         comp_value(in_source_column_rec.data_type_mod, in_target_column_rec.data_type_mod) = 1 OR
         comp_value(in_source_column_rec.data_type_owner, in_target_column_rec.data_type_owner) = 1 THEN

        IF in_target_column_rec.cort_value IS NOT NULL AND  -- there is cort-value
           in_source_column_rec.new_column_name IS NULL  -- but this cort-value is just reference on another column
        THEN
          debug('Column '||in_source_column_rec.column_name||' - changed type (with cort-value) - '||gc_result_recreate);
          -- if complex data type
          RETURN gc_result_recreate;
        END IF;
        -- if data type mismatch
        IF in_source_column_rec.data_type_owner IS NOT NULL OR
           in_target_column_rec.data_type_owner IS NOT NULL
        THEN
          debug('Column '||in_source_column_rec.column_name||' - changed type (complex datatype)');
          -- if complex data type
          RETURN gc_result_recreate;
        ELSE
          l_result := gc_result_alter;
          /* Quick forward changes are:
              CHAR(n) -> VARCHAR2(m)  m>=n
              NCHAR(n) -> NVARCHAR2(m)  m>=n
            But revert runs for them very slow.
            "ALTER" mode will be choosed by default to optimaze forward change
          */

          -- scalar data type
          -- revertable changes of types
          IF (in_source_column_rec.data_type = 'CHAR' AND in_target_column_rec.data_type = 'VARCHAR2' AND
             -- applicable only if we do not decrease length
             in_source_column_rec.char_length <= in_target_column_rec.char_length) OR
            (in_source_column_rec.data_type = 'NCHAR' AND in_target_column_rec.data_type = 'NVARCHAR2' AND
             -- applicable only if we do not decrease length
             in_source_column_rec.char_length <= in_target_column_rec.char_length)
          THEN
            -- non-revertable
            debug('Column '||in_source_column_rec.column_name||' - changed type (CHAR/VARCHAR2|NCHAR/NVARCHAR2) - '||gc_result_alter);
            l_frwd_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
            l_rlbk_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
          ELSIF
             (in_source_column_rec.data_type = 'TIMESTAMP' AND in_target_column_rec.data_type = 'DATE') OR
             (in_source_column_rec.data_type = 'DATE' AND in_target_column_rec.data_type = 'TIMESTAMP') OR
             (in_source_column_rec.data_type = 'NUMBER' AND in_target_column_rec.data_type = 'FLOAT' AND
              in_target_column_rec.data_precision BETWEEN 1 AND 63 AND
              in_source_column_rec.data_precision = 2*CEIL(in_target_column_rec.data_precision*0.30103) AND
              in_source_column_rec.data_scale = CEIL(in_target_column_rec.data_precision*0.30103)) OR
             (in_target_column_rec.data_type = 'NUMBER' AND in_source_column_rec.data_type = 'FLOAT' AND
              in_source_column_rec.data_precision BETWEEN 1 AND 63 AND
              in_target_column_rec.data_precision = 2*CEIL(in_source_column_rec.data_precision*0.30103) AND
              in_target_column_rec.data_scale = CEIL(in_source_column_rec.data_precision*0.30103))
          THEN
            l_skip_comp_data_length := TRUE;
            debug('Column '||in_source_column_rec.column_name||' - changed type (NUMBER/FLOAT)|(DATE/TIMESTAMP) - '||gc_result_alter);
            l_frwd_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
            l_rlbk_clause_indx_arr(gc_data_type) := get_column_type_clause(in_source_column_rec);
          ELSE
            debug('Column '||in_source_column_rec.column_name||' - non-revertable changed type (scalar datatype)');
            RETURN gc_result_recreate;
          END IF;
        END IF;
      END IF;

      IF in_source_column_rec.data_type IN ('CLOB','NCLOB','BLOB','LONG','LONG RAW') THEN
        l_skip_comp_data_length := TRUE;
      END IF;

      -- check column length, scale and precision
      IF (NOT l_skip_comp_data_length) AND
         (comp_value(in_source_column_rec.data_length, in_target_column_rec.data_length) = 1 OR
          comp_value(in_source_column_rec.char_length, in_target_column_rec.char_length) = 1 OR
          comp_value(in_source_column_rec.data_precision, in_target_column_rec.data_precision) = 1 OR
          comp_value(in_source_column_rec.data_scale, in_target_column_rec.data_scale) = 1)
      THEN
        IF in_target_column_rec.cort_value IS NOT NULL THEN
          debug('Column '||in_source_column_rec.column_name||' - changed data_length/data_scale/data_precision (with cort-value) - '||gc_result_recreate);
          -- if complex data type
          RETURN gc_result_recreate;
        END IF;
        l_result := gc_result_alter;
        -- revertable changes of length
        IF in_target_column_rec.data_type IN ('VARCHAR2','NVARCHAR2','RAW','CHAR','NCHAR')
        THEN
          debug('Column '||in_source_column_rec.column_name||' - VARCHAR2/NVARCHAR2/RAW/CHAR/NCHAR revertable changes of length - '||gc_result_alter);
          l_frwd_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
          l_rlbk_clause_indx_arr(gc_data_type) := get_column_type_clause(in_source_column_rec);
        ELSIF
          in_target_column_rec.data_type = 'FLOAT' AND
          in_source_column_rec.data_type = 'FLOAT' AND
          in_target_column_rec.data_precision BETWEEN FLOOR((CEIL(in_source_column_rec.data_precision*0.30103)-1)*3.32193)+1 AND
                                                      FLOOR(CEIL(in_source_column_rec.data_precision*0.30103)*3.32193)
        THEN
          debug('Column '||in_source_column_rec.column_name||' - FLOAT revertable change of length - '||gc_result_alter);
          l_frwd_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
          l_rlbk_clause_indx_arr(gc_data_type) := get_column_type_clause(in_source_column_rec);
        ELSE
          -- non-revertable increasing changes of length.
          -- These changes will not reverted BUT they do not lead to data losses or application invalidation.
          -- So no need to revert them at all
          IF ((in_target_column_rec.data_type = 'NUMBER' AND
               NVL(in_target_column_rec.data_precision,999) > in_source_column_rec.data_precision AND
               NVL(in_target_column_rec.data_scale,in_source_column_rec.data_scale)  = in_source_column_rec.data_scale) OR
              (in_target_column_rec.data_type = 'FLOAT' AND
               in_target_column_rec.data_precision > in_source_column_rec.data_precision) OR
              (in_target_column_rec.data_type = 'TIMESTAMP' AND
               in_target_column_rec.data_scale > in_source_column_rec.data_scale) OR
              (in_target_column_rec.data_type = 'TIMESTAMP WITH TIME ZONE' AND
               in_target_column_rec.data_scale > in_source_column_rec.data_scale) OR
              (in_target_column_rec.data_type = 'TIMESTAMP WITH LOCAL TIME ZONE' AND
               in_target_column_rec.data_scale > in_source_column_rec.data_scale) OR
              (in_target_column_rec.data_type = 'INTERVAL YEAR TO MONTH' AND
               in_target_column_rec.data_precision >= in_source_column_rec.data_precision AND
               in_target_column_rec.data_scale >= in_source_column_rec.data_scale) OR
              (in_target_column_rec.data_type = 'INTERVAL DAY TO SECOND' AND
               in_target_column_rec.data_precision >= in_source_column_rec.data_precision AND
               in_target_column_rec.data_scale >= in_source_column_rec.data_scale)
             )
          THEN
            -- non-revertable
            debug('Column '||in_source_column_rec.column_name||' - non-revertable increasing of length - '||gc_result_alter);
            l_frwd_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
            l_rlbk_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
            l_revert_flag := FALSE;
          ELSE
            debug('Column '||in_source_column_rec.column_name||' - changed data_length/data_scale/data_precision - '||gc_result_recreate);
            RETURN gc_result_recreate;
          END IF;
        END IF;
      END IF;

      -- check char_used. Only for varchar2 and char columns.
      IF in_source_column_rec.data_type IN ('CHAR', 'VARCHAR2') AND
         comp_value(in_source_column_rec.char_used, in_target_column_rec.char_used) = 1
      THEN
        l_result := gc_result_alter;
        l_frwd_clause_indx_arr(gc_data_type) := get_column_type_clause(in_target_column_rec);
        l_rlbk_clause_indx_arr(gc_data_type) := get_column_type_clause(in_source_column_rec);
      END IF;

      -- Check default value
      IF comp_value(NVL(in_source_column_rec.data_default,Q'{''}'), NVL(in_target_column_rec.data_default,Q'{''}')) = 1 THEN
        l_result := gc_result_alter;
        l_frwd_clause_indx_arr(gc_default) := get_clause(gc_default, NVL(in_target_column_rec.data_default, Q'{''}'));
        l_rlbk_clause_indx_arr(gc_default) := get_clause(gc_default, NVL(in_source_column_rec.data_default, Q'{''}'));
      END IF;

      -- Check encryption_alg and salt
      IF comp_value(in_source_column_rec.encryption_alg, in_target_column_rec.encryption_alg) = 1 OR
         comp_value(in_source_column_rec.salt, in_target_column_rec.salt) = 1 THEN
        l_result := gc_result_alter;
        l_frwd_clause := get_clause(gc_encryption, in_target_column_rec.encryption_alg);
        l_rlbk_clause := get_clause(gc_encryption, in_source_column_rec.encryption_alg);
        l_frwd_clause_indx_arr(gc_encryption) := l_frwd_clause || get_clause(gc_salt, in_target_column_rec.salt);
        l_rlbk_clause_indx_arr(gc_encryption) := l_rlbk_clause || get_clause(gc_salt, in_source_column_rec.salt);
      END IF;

      -- Check nullability value
      IF comp_value(in_source_column_rec.nullable, in_target_column_rec.nullable) = 1 THEN
        l_result := gc_result_alter;
        IF in_target_column_rec.notnull_constraint_name IS NOT NULL THEN
          l_frwd_clause_indx_arr(gc_constraint) := get_clause(gc_constraint, in_target_column_rec.notnull_constraint_name, '"', '"');
        END IF;
        IF in_source_column_rec.notnull_constraint_name IS NOT NULL THEN
          l_rlbk_clause_indx_arr(gc_constraint) := get_clause(gc_constraint, in_source_column_rec.notnull_constraint_name, '"', '"');
        END IF;
        l_frwd_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_target_column_rec.nullable);
        l_rlbk_clause_indx_arr(gc_nullable) := get_clause(gc_nullable, in_source_column_rec.nullable);
      END IF; -- end of normal column section

    END IF; -- end of compare section
    l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_column_name)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_data_type)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_default)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_generated_always_as)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_encryption)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_constraint)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_nullable);
    IF l_revert_flag THEN
      l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_column_name)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_data_type)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_default)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_generated_always_as)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_encryption)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_constraint)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_nullable);
    ELSE
      l_rlbk_clause := NULL;
    END IF;

    IF l_result = gc_result_alter THEN
      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => io_source_table_rec.table_name,
        in_owner         => io_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    $IF dbms_db_version.version >= 12 $THEN
    IF comp_value(in_source_column_rec.hidden_column, in_target_column_rec.hidden_column) = 1
    THEN
      debug('Column '||in_source_column_rec.column_name||' - changed column type: visible/invisible');
      l_result := gc_result_alter;
      l_frwd_clause := get_clause(gc_modify_column, in_source_column_rec.column_name, '"', '"')||case when in_target_column_rec.hidden_column = 'YES' then gc_invisible else gc_visible end;
      l_rlbk_clause := get_clause(gc_modify_column, in_source_column_rec.column_name, '"', '"')||case when in_source_column_rec.hidden_column = 'YES' then gc_invisible else gc_visible end;

      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => io_source_table_rec.table_name,
        in_owner         => io_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;
    $ELSE
    IF comp_value(in_source_column_rec.hidden_column, in_target_column_rec.hidden_column) = 1
    THEN
      debug('Column '||in_source_column_rec.column_name||' - changed column type: segment/hidden');
      RETURN gc_result_recreate;
    END IF;
    $END

    -- if columns have different names in source and dest tables
    IF in_check_name AND
       comp_value(in_source_column_rec.column_name, in_target_column_rec.column_name) = 1
    THEN
      debug('Column '||in_source_column_rec.column_name||' has another name - '||in_target_column_rec.column_name);
      -- Use 2 way process of renaming. To avoid cross reference issues
      -- For example:
      --A -> C --#=A
      --B -> B
      --C -> A --#=C
      -- First way:
      --   Rename A -> tmp1
      --   Rename C -> tmp3
      -- Second way at the end of all columns processing:
      --   Rename tmp1 -> A
      --   Rename tmp3 -> C

      -- Check is it same renamed column or not
      IF in_source_column_rec.column_name != in_target_column_rec.column_name AND
         in_source_column_rec.new_column_name = in_target_column_rec.column_name
      THEN
        l_frwd_clause := get_clause(gc_rename_column, '"'||in_source_column_rec.column_name||'" TO "'||in_source_column_rec.temp_column_name||'"');
        l_rlbk_clause := get_clause(gc_rename_column, '"'||in_source_column_rec.temp_column_name||'" TO "'||in_source_column_rec.column_name||'"');
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => io_source_table_rec.table_name,
          in_owner         => io_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
        l_result := gc_result_alter;
      ELSE
        debug('Column '||in_source_column_rec.column_name||' - changed column name');
        RETURN gc_result_recreate;
      END IF;
    END IF;

    RETURN l_result;

  END comp_column;

  -- Second way of renaming process - renaming all temp columns into new column_name
  PROCEDURE rename_temp_columns(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause VARCHAR2(32767);
    l_rlbk_clause VARCHAR2(32767);
  BEGIN
    FOR i IN 1..in_source_table_rec.column_arr.COUNT LOOP
      IF in_source_table_rec.column_arr(i).new_column_name IS NOT NULL THEN
        l_frwd_clause := get_clause(gc_rename_column, '"'||in_source_table_rec.column_arr(i).temp_column_name||'" TO "'||in_source_table_rec.column_arr(i).new_column_name||'"');
        l_rlbk_clause := get_clause(gc_rename_column, '"'||in_source_table_rec.column_arr(i).new_column_name||'" TO "'||in_source_table_rec.column_arr(i).temp_column_name||'"');
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => in_source_table_rec.table_name,
          in_owner         => in_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END IF;
    END LOOP;
  END rename_temp_columns;

  -- compare all table columns
  FUNCTION comp_table_columns(
    io_source_table_rec    IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec    IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                  PLS_INTEGER;
    l_comp_result             PLS_INTEGER;
    l_segment_result          PLS_INTEGER;
    l_virtual_result          PLS_INTEGER;
    l_source_segment_col_arr  arrays.gt_int_arr;
    l_target_segment_col_arr  arrays.gt_int_arr;
    l_source_virtual_col_arr  arrays.gt_int_arr;
    l_target_virtual_col_arr  arrays.gt_int_arr;
    l_source_xref_arr         arrays.gt_int_arr;
    l_target_xref_arr         arrays.gt_int_arr;
    l_drop_column_arr         arrays.gt_int_arr;
    l_add_column_arr          arrays.gt_int_arr;
    l_indx1                   PLS_INTEGER;
    l_indx2                   PLS_INTEGER;
    l_cnt                     PLS_INTEGER;
    l_column_name             arrays.gt_name;
    l_source_column_id        PLS_INTEGER;
    l_target_column_id        PLS_INTEGER;
    l_first_new_virt_col_indx PLS_INTEGER;
    l_first_old_virt_col_indx PLS_INTEGER;
    l_column_rec              cort_exec_pkg.gt_column_rec;
  BEGIN
    l_result := gc_result_nochange;
    l_segment_result := gc_result_nochange;
    l_virtual_result := gc_result_nochange;

     -- update column names - replace name for renamed columns with cort values
     update_refs_on_renamed_columns(
       io_table_rec    => io_source_table_rec
     );

    -- Copy all indexes of segment columns columns into separate collections
    -- for source table
    FOR i IN 1..io_source_table_rec.column_arr.COUNT LOOP
      -- get segment non-hidden columns.
      IF io_source_table_rec.column_arr(i).virtual_column = 'NO' AND
         (io_source_table_rec.column_arr(i).hidden_column = 'NO' OR io_source_table_rec.column_arr(i).user_generated = 'YES')
      THEN
        l_source_segment_col_arr(i) := io_source_table_rec.column_arr(i).segment_column_id;
      END IF;
    END LOOP;

    -- for target table
    FOR i IN 1..io_target_table_rec.column_arr.COUNT LOOP
      -- get segment non-hidden columns.
      IF io_target_table_rec.column_arr(i).virtual_column = 'NO' AND
         (io_target_table_rec.column_arr(i).hidden_column = 'NO' OR io_target_table_rec.column_arr(i).user_generated = 'YES')
      THEN
        l_target_segment_col_arr(i) := io_target_table_rec.column_arr(i).segment_column_id;
        -- prepropulate list for potentially new columns. Will delete from that list when find matching column
        l_add_column_arr(i) := io_target_table_rec.column_arr(i).segment_column_id;
      END IF;
    END LOOP;


    -- find removed segment columns
    l_indx1 := l_source_segment_col_arr.FIRST;
    WHILE l_indx1 IS NOT NULL LOOP
      -- check if this is unused column
      l_column_name := NVL(io_source_table_rec.column_arr(l_indx1).new_column_name,io_source_table_rec.column_arr(l_indx1).column_name);
      debug('Matching column '||l_column_name);
      -- check if this column exists in new table definition
      IF io_target_table_rec.column_indx_arr.EXISTS(l_column_name) THEN
        l_indx2 := io_target_table_rec.column_indx_arr(l_column_name);
        -- not a new column. Delete from the add list
        l_add_column_arr.DELETE(l_indx2);

        l_source_xref_arr(l_indx1) := l_indx2;
        l_target_xref_arr(l_indx2) := l_indx1;

        debug('Source column on index '||l_indx1||' matched to target column on index '||l_indx2);

        io_source_table_rec.column_arr(l_indx1).matched_column_id := io_target_table_rec.column_arr(l_indx2).column_id;
        io_target_table_rec.column_arr(l_indx2).matched_column_id := io_source_table_rec.column_arr(l_indx1).column_id;
      ELSE
        debug('column '||l_column_name||' not found in target table');
        -- check if this is invisible user column
        IF io_source_table_rec.column_arr(l_indx1).hidden_column = 'YES' AND
           io_source_table_rec.column_arr(l_indx1).user_generated = 'YES' AND
           cort_exec_pkg.g_params.compare.value_exists('IGNORE_INVISIBLE')
        THEN
          -- do nothing
          NULL;
        ELSIF io_source_table_rec.column_arr(l_indx1).unused_flag AND
           cort_exec_pkg.g_params.compare.value_exists('IGNORE_UNUSED')
        THEN
          -- do nothing for hidden columns
          NULL;
        ELSE
          debug('Adding to drop list column  '||l_column_name||' (indx = '||l_indx1||')');
          l_drop_column_arr(l_indx1) := l_indx1;
        END IF;
      END IF;
      l_indx1 := l_source_segment_col_arr.NEXT(l_indx1);
    END LOOP;


    -- [Smart rename]
    -- check that removed columns have added on the same segment position column with the same data type
    IF l_drop_column_arr.COUNT > 0 THEN
      l_indx1 := l_drop_column_arr.FIRST;
      WHILE l_indx1 IS NOT NULL LOOP
        l_indx2 := l_add_column_arr.FIRST;
        WHILE l_indx2 IS NOT NULL LOOP
          IF comp_value(io_source_table_rec.column_arr(l_indx1).segment_column_id,
                        io_target_table_rec.column_arr(l_indx2).segment_column_id) = 0 AND
             io_target_table_rec.column_arr(l_indx2).cort_value IS NULL AND
             comp_data_type(
               in_source_column_rec => io_source_table_rec.column_arr(l_indx1),
               in_target_column_rec => io_target_table_rec.column_arr(l_indx2)
             ) = 0 AND
             comp_value(
               in_val1 => io_source_table_rec.column_arr(l_indx1).data_default,
               in_val2 => io_target_table_rec.column_arr(l_indx2).data_default
              ) = 0
          THEN
            -- column is get renamed
            io_source_table_rec.column_arr(l_indx1).new_column_name := io_target_table_rec.column_arr(l_indx2).column_name;
            debug('There is added column on the same position and with the same data type as dropped column - column '||io_source_table_rec.column_arr(l_indx1).column_name|| ': new column name = '||io_source_table_rec.column_arr(l_indx1).new_column_name);

            -- update reference by name
            io_source_table_rec.column_indx_arr.DELETE(io_source_table_rec.column_arr(l_indx1).column_name);
            io_source_table_rec.column_indx_arr(io_source_table_rec.column_arr(l_indx1).new_column_name) := l_indx1;

            l_source_xref_arr(l_indx1) := l_indx2;
            l_target_xref_arr(l_indx2) := l_indx1;

            io_source_table_rec.column_arr(l_indx1).matched_column_id := io_target_table_rec.column_arr(l_indx2).column_id;
            io_target_table_rec.column_arr(l_indx2).matched_column_id := io_source_table_rec.column_arr(l_indx1).column_id;

            l_drop_column_arr.DELETE(l_indx1);
            l_add_column_arr.DELETE(l_indx2);
            l_segment_result := gc_result_alter;
            EXIT;
          END IF;
          l_indx2 := l_add_column_arr.NEXT(l_indx2);
        END LOOP;
        l_indx1 := l_drop_column_arr.NEXT(l_indx1);
      END LOOP;
    END IF;

    -- if there are columns to drop
    IF l_drop_column_arr.COUNT > 0 THEN
      IF cort_exec_pkg.g_params.drop_column.get_value <> 'RECREATE' THEN
        l_cnt := 0;
        FOR i IN 1..io_source_table_rec.column_arr.COUNT LOOP
          l_indx1 := i;
          IF l_drop_column_arr.EXISTS(l_indx1) THEN
           -- remove dropping columns from source segment column array
            debug('mark segment column to drop - '||io_source_table_rec.column_arr(l_indx1).column_name);
            l_source_segment_col_arr.DELETE(l_indx1);
            l_cnt := l_cnt + 1;
          ELSE
            IF l_source_xref_arr.EXISTS(l_indx1) THEN
              l_indx2 := l_source_xref_arr(l_indx1);

              io_source_table_rec.column_arr(l_indx1).segment_column_id := io_source_table_rec.column_arr(l_indx1).segment_column_id - l_cnt;

              -- check that all matched columns on their initial positions
              -- decrease source segment_column_id by number of dropped columns for every existing segment column
              IF comp_value(io_source_table_rec.column_arr(l_indx1).segment_column_id,
                            io_target_table_rec.column_arr(l_indx2).segment_column_id) = 1
              THEN
                debug('There is moved existing segment column: '||io_source_table_rec.column_arr(l_indx1).column_name);
                debug('Source column column indx - '||l_indx1||', name - '||io_source_table_rec.column_arr(l_indx1).column_name||', segment_column_id = '||to_char(io_source_table_rec.column_arr(l_indx1).segment_column_id));
                debug('Target column column indx - '||l_indx2||', name - '||io_target_table_rec.column_arr(l_indx2).column_name||', segment_column_id = '||io_target_table_rec.column_arr(l_indx2).segment_column_id);
                IF cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER') THEN
                  NULL;
                  -- do nothing;
                  debug('IGNORE_ORDER param is enabled - no changes');
                ELSE
                  RETURN gc_result_recreate;
                END IF;
              END IF;

            END IF;
          END IF;
        END LOOP;
        l_segment_result := gc_result_alter;
      ELSE
        -- then need to recreate
        debug('RECREATE drop semantic is used to drop segment columns');
        l_segment_result := gc_result_recreate;
      END IF;
      -- if all column need to be dropped then recreate
      IF l_drop_column_arr.COUNT = l_source_segment_col_arr.COUNT THEN
        debug('All segment columns are dropped');
        RETURN gc_result_recreate;
      END IF;
    END IF;

    -- if there are columns to add
    IF l_add_column_arr.COUNT > 0
    THEN
      -- get the first new column
      debug('Index of adding new column : '||l_add_column_arr.FIRST||'  Segment ID of first new column : '||l_add_column_arr(l_add_column_arr.FIRST));
      debug('Segment ID of last source column : '||l_source_segment_col_arr(l_source_segment_col_arr.LAST));
      -- if not found then all new columns were added to the end and ALTER change is available
      -- OR
      -- ignore columns order option is enabled
      -- otherwise need to recreate
      IF l_add_column_arr(l_add_column_arr.FIRST) > l_source_segment_col_arr(l_source_segment_col_arr.LAST) OR cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER')
      THEN
        l_segment_result := gc_result_alter;
      ELSE
        debug('There are new segment column in the middle of the column list: - '||io_target_table_rec.column_arr(l_add_column_arr.FIRST).column_name);
        RETURN gc_result_recreate;
      END IF;
    END IF;


/*
-- test output
    l_indx1 := l_source_xref_arr.FIRST;
    WHILE l_indx1 IS NOT NULL LOOP
      debug(to_char(l_indx1)||') source column: '||io_source_table_rec.column_arr(l_indx1).column_name ||' - match id '||l_source_xref_arr(l_indx1));
      l_indx1 := l_source_xref_arr.NEXT(l_indx1);
    END LOOP;


    l_indx2 := l_target_xref_arr.FIRST;
    WHILE l_indx2 IS NOT NULL LOOP
      debug(to_char(l_indx2)||') target column: '||io_target_table_rec.column_arr(l_indx2).column_name ||' - match id '||l_target_xref_arr(l_indx2));
      l_indx2 := l_target_xref_arr.NEXT(l_indx2);
    END LOOP;
-- end of test
*/
    -- check all for matched segment columns
    l_indx1 := l_source_xref_arr.FIRST;
    l_indx2 := l_target_xref_arr.FIRST;
    WHILE l_indx1 IS NOT NULL AND l_indx2 IS NOT NULL LOOP
      IF cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER') THEN
        l_indx2 := l_source_xref_arr(l_indx1);
      END IF;
      l_comp_result := comp_column(
                         io_source_table_rec    => io_source_table_rec,
                         in_source_column_rec   => io_source_table_rec.column_arr(l_indx1),
                         in_target_column_rec   => io_target_table_rec.column_arr(l_indx2),
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare segment column result = '||l_comp_result);
      l_segment_result := GREATEST(l_segment_result, l_comp_result);
      IF l_segment_result = gc_result_recreate THEN
        RETURN l_segment_result;
      END IF;
      l_indx1 := l_source_xref_arr.NEXT(l_indx1);
      IF NOT cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER') THEN
        l_indx2 := l_target_xref_arr.NEXT(l_indx2);
      END IF;
    END LOOP;


    -- **** VIRTUAL COLUMNS ****

    -- Copy all indexes of virtual columns columns into separate collections
    -- for source table
    FOR i IN 1..io_source_table_rec.column_arr.COUNT LOOP
      -- get virtual non-hidden columns.
      IF io_source_table_rec.column_arr(i).virtual_column = 'YES' AND
         io_source_table_rec.column_arr(i).hidden_column = 'NO'
      THEN
        l_source_virtual_col_arr(i) := io_source_table_rec.column_arr(i).column_id;
      END IF;
    END LOOP;

    -- for target table
    FOR i IN 1..io_target_table_rec.column_arr.COUNT LOOP
      -- get virtual non-hidden columns.
      IF io_target_table_rec.column_arr(i).virtual_column = 'YES' AND
         io_target_table_rec.column_arr(i).hidden_column = 'NO'
      THEN
        l_target_virtual_col_arr(i) := io_target_table_rec.column_arr(i).column_id;
      END IF;
    END LOOP;

    -- find deleted and matched virtual columns
    l_indx1 := l_source_virtual_col_arr.FIRST;
    l_indx2 := l_target_virtual_col_arr.FIRST;
    l_cnt := 0;
    WHILE l_indx1 IS NOT NULL LOOP
      l_source_column_id := l_source_virtual_col_arr(l_indx1);
      debug('Check virtual columns on source_column_id = '||l_source_column_id);


      IF l_indx2 IS NOT NULL THEN
        l_target_column_id := l_target_virtual_col_arr(l_indx2);

        debug('Compare virtual columns on source_column_id = '||l_source_column_id||' and target_column_id = '||l_target_column_id);

        IF (l_source_column_id - l_cnt = l_target_column_id) OR cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER') THEN
          io_source_table_rec.column_arr(l_indx1).matched_column_id := io_target_table_rec.column_arr(l_indx2).column_id;
          io_target_table_rec.column_arr(l_indx2).matched_column_id := io_source_table_rec.column_arr(l_indx1).column_id;

          IF io_source_table_rec.column_arr(l_indx1).column_name != io_target_table_rec.column_arr(l_indx2).column_name THEN
            io_source_table_rec.column_arr(l_indx1).new_column_name := io_target_table_rec.column_arr(l_indx2).column_name;
          END IF;

           -- compare virtual columns
           l_comp_result := comp_column(
                              io_source_table_rec    => io_source_table_rec,
                              in_source_column_rec   => io_source_table_rec.column_arr(l_indx1),
                              in_target_column_rec   => io_target_table_rec.column_arr(l_indx2),
                              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                            );
           debug('Compare virtual columns '||io_source_table_rec.column_arr(l_indx1).column_name||' and '||io_target_table_rec.column_arr(l_indx2).column_name||' - '||l_comp_result);

           CASE l_comp_result
           WHEN gc_result_exchange THEN
             EXIT;
           WHEN gc_result_recreate THEN
             RETURN l_result;
           ELSE
             l_virtual_result := GREATEST(l_virtual_result, l_comp_result);
           END CASE;

           -- go to next target columns
           l_indx2 := l_target_virtual_col_arr.NEXT(l_indx2);
        ELSIF l_source_column_id - l_cnt < l_target_column_id  THEN
          -- delete virtual column
          l_drop_column_arr(l_indx1) := l_indx1;
          IF l_first_old_virt_col_indx IS NULL THEN
            l_first_old_virt_col_indx := l_indx1;
          END IF;
          l_cnt := l_cnt + 1;
        ELSE
          l_virtual_result := gc_result_exchange;
          EXIT;
        END IF;
      ELSE
        -- delete virtual column
        l_drop_column_arr(l_indx1) := l_indx1;
        IF l_first_old_virt_col_indx IS NULL THEN
          l_first_old_virt_col_indx := l_indx1;
        END IF;
        l_cnt := l_cnt + 1;
      END IF;

      l_indx1 := l_source_virtual_col_arr.NEXT(l_indx1);
    END LOOP;

    IF l_virtual_result < gc_result_exchange THEN
      -- if there are any left target virtual columns - add them
      WHILE l_indx2 IS NOT NULL LOOP
        l_add_column_arr(l_indx2) := l_indx2;
        IF l_first_new_virt_col_indx IS NULL THEN
          l_first_new_virt_col_indx := l_indx2;
        END IF;
        l_indx2 := l_target_virtual_col_arr.NEXT(l_indx2);
      END LOOP;
    END IF;

    IF l_first_new_virt_col_indx IS NOT NULL
    THEN
      -- get the first new column
      l_indx2 := l_first_new_virt_col_indx;
      -- find nearest next column in column list
      l_indx2 := l_target_segment_col_arr.NEXT(l_indx2);
      -- if not found then all new columns were added to the end and ALTER change is available
      -- otherwise need to recreate but with data move (exchange partition)
      IF l_indx2 IS NOT NULL AND NOT l_add_column_arr.EXISTS(l_indx2)
      THEN
        l_virtual_result := gc_result_exchange;
      ELSE
        l_virtual_result := GREATEST(l_virtual_result, gc_result_alter);
      END IF;
    END IF;

    -- if there are dropped virtual columns in the middle of the list and drop semantic is recreate
    IF l_first_old_virt_col_indx IS NOT NULL
    THEN
      -- get the first dropped virtual column
      l_indx1 := l_first_old_virt_col_indx;
      l_indx1 := l_source_segment_col_arr.NEXT(l_indx1);
      IF l_indx1 IS NOT NULL AND cort_exec_pkg.g_params.drop_column.get_value = 'RECREATE'
      THEN
        -- then need to recreate
        l_virtual_result := gc_result_exchange;
      ELSE
        l_virtual_result := GREATEST(l_virtual_result, gc_result_alter);
      END IF;
    END IF;

    debug('l_segment_result = '||l_segment_result);
    debug('l_virtual_result = '||l_virtual_result);
    debug('l_result = '||l_result);

    IF l_virtual_result = gc_result_exchange AND
       NOT available_for_exchange(io_source_table_rec)
    THEN
      debug('Exchange partition is not possible for this table');
      RETURN gc_result_recreate;
    END IF;

    IF l_virtual_result = gc_result_exchange AND
       cort_exec_pkg.g_params.data.get_value = 'NONE'
    THEN
      debug('Exchange partition is not prefereable if data are not kept');
      RETURN gc_result_recreate;
    END IF;

    IF l_virtual_result = gc_result_exchange AND
       io_source_table_rec.is_table_empty
    THEN
      debug('Exchange partition is not prefereable if table is empty');
      RETURN gc_result_recreate;
    END IF;

    l_result := GREATEST(l_segment_result,l_virtual_result);

    debug('l_segment_result = '||l_segment_result);
    debug('l_virtual_result = '||l_virtual_result);
    debug('l_result = '||l_result);

    -- Modify all dropping virtual column expressions - make them constant - to remove dependenceis on segment columns.
    l_indx1 := l_drop_column_arr.LAST;
    WHILE l_indx1 IS NOT NULL LOOP
      IF io_source_table_rec.column_arr(l_indx1).virtual_column = 'YES' THEN
        debug('Dropping column '||io_source_table_rec.column_arr(l_indx1).column_name);
        IF io_source_table_rec.column_arr(l_indx1).partition_key OR
           io_source_table_rec.column_arr(l_indx1).cluster_key OR
           io_source_table_rec.column_arr(l_indx1).iot_primary_key
          -- check other drop restrictions ????
        THEN
          debug('Unable to modify data_default for virtual column '||io_source_table_rec.column_arr(l_indx1).column_name);
          RETURN gc_result_recreate;
        ELSE
          l_column_rec := io_source_table_rec.column_arr(l_indx1);
          l_column_rec.data_default := '0';
          -- genmerate SQL to modify virt column expression
          l_comp_result := comp_column(
                             io_source_table_rec    => io_source_table_rec,
                             in_source_column_rec   => io_source_table_rec.column_arr(l_indx1),
                             in_target_column_rec   => l_column_rec,
                             io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                             io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                           );
        END IF;
      END IF;
      l_indx1 := l_drop_column_arr.PRIOR(l_indx1);
    END LOOP;

    -- Drop columns
    l_indx1 := l_drop_column_arr.LAST;
    WHILE l_indx1 IS NOT NULL LOOP
      debug('Dropping column '||io_source_table_rec.column_arr(l_indx1).column_name);
      IF io_source_table_rec.column_arr(l_indx1).partition_key OR
         io_source_table_rec.column_arr(l_indx1).cluster_key OR
         io_source_table_rec.column_arr(l_indx1).iot_primary_key OR
         --io_source_table_rec.compression_rec.compression = 'ENABLED' OR
         --io_source_table_rec.compressed_partitions OR
         cort_exec_pkg.g_params.drop_column.get_value = 'RECREATE'
      -- check other drop restrictions
      THEN
        debug('Unable to drop column '||io_source_table_rec.column_arr(l_indx1).column_name);
        RETURN gc_result_recreate;
      ELSE
        IF io_source_table_rec.column_arr(l_indx1).virtual_column = 'YES'
        THEN
          IF l_virtual_result = gc_result_alter THEN
            drop_column(
              in_source_table_rec    => io_source_table_rec,
              in_column_rec          => io_source_table_rec.column_arr(l_indx1),
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
            IF cort_exec_pkg.g_params.drop_column.get_value <> 'SET_INVISIBLE' THEN
              drop_column_refereces(
                io_table_rec    => io_source_table_rec,
                in_column_rec   => io_source_table_rec.column_arr(l_indx1)
              );
            END IF;
          END IF;
        ELSE
          drop_column(
            in_source_table_rec    => io_source_table_rec,
            in_column_rec          => io_source_table_rec.column_arr(l_indx1),
            io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
            io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
          );
          IF cort_exec_pkg.g_params.drop_column.get_value <> 'SET_INVISIBLE' THEN
            drop_column_refereces(
              io_table_rec    => io_source_table_rec,
              in_column_rec   => io_source_table_rec.column_arr(l_indx1)
            );
          END IF;
        END IF;
      END IF;
      l_indx1 := l_drop_column_arr.PRIOR(l_indx1);
    END LOOP;

    -- Add columns
    l_indx2 := l_add_column_arr.FIRST;
    WHILE l_indx2 IS NOT NULL LOOP
      debug('Adding column '||io_target_table_rec.column_arr(l_indx2).column_name);
      -- if new column is NOT NULL and doesn't have default value then recreate
      IF io_target_table_rec.column_arr(l_indx2).nullable = 'N' AND
         io_target_table_rec.column_arr(l_indx2).data_default IS NULL AND
         io_target_table_rec.column_arr(l_indx2).virtual_column = 'NO'
      THEN
        IF io_target_table_rec.column_arr(l_indx2).cort_value IS NOT NULL THEN
          debug('Column '||io_target_table_rec.column_arr(l_indx2).column_name||' is mandatory and has no default value. Need to recreate...');
          RETURN gc_result_recreate;
        ELSE
          IF NOT io_source_table_rec.is_table_empty THEN
            cort_exec_pkg.raise_error( 'New NOT NULL column '||io_target_table_rec.column_arr(l_indx2).column_name||' must have DEFAULT or CORT_VALUE value');
          END IF;
        END IF;
      END IF;
      IF (io_target_table_rec.column_arr(l_indx2).virtual_column = 'NO') OR
         (io_target_table_rec.column_arr(l_indx2).virtual_column = 'YES' AND l_virtual_result = gc_result_alter)
      THEN
        add_column(
          in_source_table_rec    => io_source_table_rec,
          in_target_table_rec    => io_target_table_rec,
          in_column_rec          => io_target_table_rec.column_arr(l_indx2),
          io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
        );
      END IF;
      l_indx2 := l_add_column_arr.NEXT(l_indx2);
    END LOOP;

    -- Second way of renaming process - renaming all temp columns into new column_name
    rename_temp_columns(
      in_source_table_rec    => io_source_table_rec,
      io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
    );

    -- update column names - replace name for smart renamed columns
    update_refs_on_renamed_columns(
      io_table_rec    => io_source_table_rec
    );

    RETURN l_result;

  END comp_table_columns;

  FUNCTION get_index_column_str(
    in_index_rec IN cort_exec_pkg.gt_index_rec
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(32767);
  BEGIN
    IF in_index_rec.index_type = 'BITMAP' AND in_index_rec.join_index = 'YES' THEN
      FOR i IN 1..in_index_rec.column_arr.COUNT LOOP
        l_result := l_result || '"'||in_index_rec.column_table_owner_arr(i)||'"."'||cort_parse_pkg.get_original_name('TABLE',in_index_rec.column_table_arr(i))||'"."'||in_index_rec.column_arr(i)||'",';
      END LOOP;
    ELSE
      FOR i IN 1..in_index_rec.column_arr.COUNT LOOP
        IF in_index_rec.column_expr_arr.EXISTS(i) AND in_index_rec.column_expr_arr(i) IS NOT NULL THEN
          l_result := l_result || in_index_rec.column_expr_arr(i) || ',';
        ELSE
          l_result := l_result || in_index_rec.column_arr(i) || ',';
        END IF;
      END LOOP;
    END IF;
    l_result := TRIM(',' FROM l_result);
    RETURN l_result;
  END get_index_column_str;

  FUNCTION get_index_hash_str(
    in_index_rec IN cort_exec_pkg.gt_index_rec
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN SUBSTR(in_index_rec.index_type||' '||
                  in_index_rec.uniqueness||' '||
                  in_index_rec.join_index||'('||
                  get_index_column_str(in_index_rec)||')', 1, 32767);
  END get_index_hash_str;

  -- Returns CREATE INDEX clause
  FUNCTION get_create_index_clause(
    in_index_name  IN VARCHAR2,
    in_table_name  IN VARCHAR2,
    in_index_rec   IN cort_exec_pkg.gt_index_rec
  )
  RETURN CLOB
  AS
    l_clause                  CLOB;
    l_clause_indx_arr         arrays.gt_xlstr_indx;
    l_index_type              VARCHAR2(50);
    l_str                     VARCHAR2(32767);
  BEGIN
    IF in_index_rec.index_type IN ('BITMAP','FUNCTION-BASED BITMAP') THEN
      l_index_type := ' BITMAP';
    ELSIF in_index_rec.uniqueness = 'UNIQUE' THEN
      l_index_type := ' UNIQUE';
    ELSE
      l_index_type := NULL;
    END IF;
    -- Use system generated name as user name
    l_clause_indx_arr(gc_index) := get_clause(gc_index, in_index_rec.owner||'"."'||in_index_name, '"', '"');
    l_clause_indx_arr(gc_on_table) := get_clause(gc_on_table, in_index_rec.table_owner||'"."'||in_table_name, '"', '"');
    l_clause_indx_arr(gc_index_columns) := get_clause(gc_index_columns, get_index_column_str(in_index_rec), '(', ')');

    IF in_index_rec.join_index = 'YES' THEN
      l_str := get_from_clause(in_index_rec.join_inner_owner_arr, in_index_rec.join_inner_table_arr,
                               in_index_rec.join_outer_owner_arr, in_index_rec.join_outer_table_arr);
      l_clause_indx_arr(gc_from_clause) := get_clause(gc_from_clause, l_str);

      l_str := get_where_clause(in_index_rec.join_inner_owner_arr, in_index_rec.join_inner_table_arr,
                                in_index_rec.join_inner_column_arr, in_index_rec.join_outer_owner_arr,
                                in_index_rec.join_outer_table_arr, in_index_rec.join_outer_column_arr);

      l_clause_indx_arr(gc_where_clause) := get_clause(gc_where_clause, l_str);
    END IF;

    IF in_index_rec.index_type = 'DOMAIN' THEN
      l_clause_indx_arr(gc_indextype) := get_clause(gc_indextype, in_index_rec.ityp_owner||'"."'||in_index_rec.ityp_name, '"', '"');
      l_clause_indx_arr(gc_parameters) := get_clause(gc_parameters, in_index_rec.parameters, '(Q''{', '}'')');
    END IF;
    -- reverse
    IF in_index_rec.index_type IN ('NORMAL/REV', 'FUNCTION-BASED NORMAL/REV') THEN
      l_clause_indx_arr(gc_reverse) := get_clause(gc_reverse, ' ');
    END IF;
    -- key_compression
    l_clause_indx_arr(gc_key_compression) := get_clause(gc_key_compression, in_index_rec.compression, NULL, ' '||nullif(in_index_rec.prefix_length,0));

    -- locality
    l_clause_indx_arr(gc_locality) := get_clause(gc_locality, in_index_rec.locality);
    -- physical_attributes
    l_clause_indx_arr(gc_physical_attr) := get_index_physical_attr_clause(in_index_rec.physical_attr_rec);

    -- Logging
    l_clause_indx_arr(gc_logging) := get_clause(gc_logging, in_index_rec.logging);
    -- Parallel
    IF cort_exec_pkg.g_params.parallel.get_num_value > 1 THEN
      l_str := get_clause(gc_degree, cort_exec_pkg.g_params.parallel.get_num_value)||get_clause(gc_instances, in_index_rec.parallel_rec.instances);
    ELSE
    l_str := get_clause(gc_degree, in_index_rec.parallel_rec.degree)||get_clause(gc_instances, in_index_rec.parallel_rec.instances);
    END IF;
    l_clause_indx_arr(gc_parallel) := get_clause(gc_parallel, l_str, '(', ')');
    -- Tablespace
    IF cort_exec_pkg.g_params.tablespace.value_exists('INDEX') THEN
      l_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_index_rec.tablespace_name, '"', '"');
    END IF;
    -- visibility
    $IF dbms_db_version.version >= 11 $THEN
      IF in_index_rec.visibility IS NOT NULL THEN
        l_clause_indx_arr(gc_visible) := in_index_rec.visibility;
      END IF;
    $END
    -- indexing
    $IF (dbms_db_version.version > 12) OR (dbms_db_version.version = 12 AND dbms_db_version.release >= 2) $THEN
      IF in_index_rec.indexing <> 'FULL' and in_index_rec.partitioned = 'YES' THEN
        l_clause_indx_arr(gc_indexing) := get_clause(gc_indexing, in_index_rec.indexing);
      END IF;
    $END

    l_clause := gc_create||l_index_type||
                get_clause_by_name(l_clause_indx_arr, gc_index)||
                get_clause_by_name(l_clause_indx_arr, gc_on_table)||
                get_clause_by_name(l_clause_indx_arr, gc_index_columns)||
                get_clause_by_name(l_clause_indx_arr, gc_from_clause)||
                get_clause_by_name(l_clause_indx_arr, gc_where_clause)||
                get_clause_by_name(l_clause_indx_arr, gc_indextype)||
                get_clause_by_name(l_clause_indx_arr, gc_parameters)||
                get_clause_by_name(l_clause_indx_arr, gc_reverse)||
                get_clause_by_name(l_clause_indx_arr, gc_key_compression)||
                get_clause_by_name(l_clause_indx_arr, gc_locality)||
                get_clause_by_name(l_clause_indx_arr, gc_physical_attr)||
                get_clause_by_name(l_clause_indx_arr, gc_logging)||
                get_clause_by_name(l_clause_indx_arr, gc_parallel)||
                get_clause_by_name(l_clause_indx_arr, gc_visible)||
                get_clause_by_name(l_clause_indx_arr, gc_indexing)||
                get_clause_by_name(l_clause_indx_arr, gc_tablespace);

    RETURN l_clause;
  END get_create_index_clause;


  -- builds CREATE INDEX statements to add index
  PROCEDURE create_index(
    in_index_rec             IN cort_exec_pkg.gt_index_rec,
    in_table_name            IN VARCHAR2,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_stmt            CLOB;
    l_rlbk_stmt            CLOB;
  BEGIN
    -- create forward statements
    l_frwd_stmt := get_create_index_clause(in_index_rec.rename_rec.object_name, in_table_name, in_index_rec);
    debug('Create index SQL', l_frwd_stmt);
    -- create rollback statements
    l_rlbk_stmt := gc_drop||get_clause(gc_index, in_index_rec.owner||'"."'||in_index_rec.rename_rec.object_name, '"', '"');

    add_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_frwd_stmt     => l_frwd_stmt,
      in_rlbk_stmt     => l_rlbk_stmt
    );
    IF cort_exec_pkg.g_params.parallel.get_num_value > 1 THEN
      l_frwd_stmt := get_clause(gc_parallel, get_clause(gc_degree, in_index_rec.parallel_rec.degree)||get_clause(gc_instances, in_index_rec.parallel_rec.instances), '(', ')');
      l_rlbk_stmt := null;
      add_alter_index_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_index_name    => in_index_rec.rename_rec.object_name,
        in_owner         => in_index_rec.owner,
        in_frwd_clause   => l_frwd_stmt,
        in_rlbk_clause   => l_rlbk_stmt
      );
    END IF;
  END create_index;

  -- builds DROP INDEX ALTER statements to drop index
  PROCEDURE drop_index(
    in_index_rec             IN cort_exec_pkg.gt_index_rec,
    in_table_name            IN VARCHAR2,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_stmt            CLOB;
    l_rlbk_stmt            CLOB;
  BEGIN
    -- create rollback statements
    IF cort_exec_pkg.g_params.parallel.get_num_value > 1 THEN
      l_rlbk_stmt := get_clause(gc_parallel, get_clause(gc_degree, in_index_rec.parallel_rec.degree)||get_clause(gc_instances, in_index_rec.parallel_rec.instances), '(', ')');
      l_frwd_stmt := null;
      add_alter_index_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_index_name    => in_index_rec.index_name,
        in_owner         => in_index_rec.owner,
        in_frwd_clause   => l_frwd_stmt,
        in_rlbk_clause   => l_rlbk_stmt
      );
    END IF;
    -- create forward statements
    l_frwd_stmt := gc_drop||get_clause(gc_index, in_index_rec.owner||'"."'||in_index_rec.index_name, '"', '"');
    -- create rollback statements
    l_rlbk_stmt := get_create_index_clause(in_index_rec.index_name, in_table_name, in_index_rec);
    add_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_frwd_stmt     => l_frwd_stmt,
      in_rlbk_stmt     => l_rlbk_stmt
    );
  END drop_index;

  -- Returns index_rec assigned to given PK/UK
  FUNCTION get_pk_index_rec(
    in_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_constraint_rec IN cort_exec_pkg.gt_constraint_rec
  )
  RETURN cort_exec_pkg.gt_index_rec
  AS
    l_index_full_name VARCHAR2(65);
    l_index_rec       cort_exec_pkg.gt_index_rec;
  BEGIN
    IF in_constraint_rec.index_owner IS NOT NULL AND
       in_constraint_rec.index_name IS NOT NULL
    THEN
      l_index_full_name := '"'||in_constraint_rec.index_owner||'"."'||in_constraint_rec.index_name||'"';
      IF in_table_rec.index_indx_arr.EXISTS(l_index_full_name) THEN
        l_index_rec := in_table_rec.index_arr(in_table_rec.index_indx_arr(l_index_full_name));
      END IF;
    END IF;
    RETURN l_index_rec;
  END get_pk_index_rec;

  -- compares join conditions for BITMAP join indexes
  FUNCTION comp_join_condition(
    in_source_index_rec    IN cort_exec_pkg.gt_index_rec,
    in_target_index_rec    IN cort_exec_pkg.gt_index_rec
  )
  RETURN PLS_INTEGER
  AS
    l_owner_table_ind arrays.gt_int_indx;
    l_full_name       VARCHAR2(100);
  BEGIN
    FOR i IN 1..in_source_index_rec.join_inner_table_arr.COUNT LOOP
      l_full_name := '"'||in_source_index_rec.join_inner_owner_arr(i)||'"."'||
                          in_source_index_rec.join_inner_table_arr(i)||'"."'||
                          in_source_index_rec.join_inner_column_arr(i)||'"';
      l_owner_table_ind(l_full_name) := i;
      l_full_name := '"'||in_source_index_rec.join_outer_owner_arr(i)||'"."'||
                          in_source_index_rec.join_outer_table_arr(i)||'"."'||
                          in_source_index_rec.join_outer_column_arr(i)||'"';
      l_owner_table_ind(l_full_name) := i;
    END LOOP;
    FOR i IN 1..in_target_index_rec.join_inner_table_arr.COUNT LOOP
      l_full_name := '"'||in_target_index_rec.join_inner_owner_arr(i)||'"."'||
                          in_target_index_rec.join_inner_table_arr(i)||'"."'||
                          in_target_index_rec.join_inner_column_arr(i)||'"';
      IF NOT l_owner_table_ind.EXISTS(l_full_name) THEN
        RETURN gc_result_recreate;
      END IF;
      l_full_name := '"'||in_target_index_rec.join_outer_owner_arr(i)||'"."'||
                          in_target_index_rec.join_outer_table_arr(i)||'"."'||
                          in_target_index_rec.join_outer_column_arr(i)||'"';
      IF NOT l_owner_table_ind.EXISTS(l_full_name) THEN
        RETURN gc_result_recreate;
      END IF;
    END LOOP;
    RETURN gc_result_nochange;
  END comp_join_condition;

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
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_comp_result            PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_result := gc_result_nochange;
    IF comp_value(io_source_index_rec.index_type, io_target_index_rec.index_type) = 1 OR
       comp_value(io_source_index_rec.ityp_owner, io_target_index_rec.ityp_owner) = 1 OR
       comp_value(io_source_index_rec.ityp_name, io_target_index_rec.ityp_name) = 1 OR
       comp_value(io_source_index_rec.uniqueness, io_target_index_rec.uniqueness) = 1 OR
       comp_value(io_source_index_rec.locality, io_target_index_rec.locality) = 1 OR
       comp_value(io_source_index_rec.compression, io_target_index_rec.compression) = 1 OR
       comp_value(io_source_index_rec.prefix_length, io_target_index_rec.prefix_length) = 1 OR
       comp_value(io_source_index_rec.tablespace_name, io_target_index_rec.tablespace_name) = 1 OR
       comp_value(io_source_index_rec.join_index, io_target_index_rec.join_index) = 1
    THEN
      debug('Recreate index : '||io_source_index_rec.index_name||' - mismatch index_type/itype/uniqueness/locality/compression');
      RETURN gc_result_recreate;
    END IF;

    IF cort_exec_pkg.g_params.tablespace.value_exists('INDEX') AND
       comp_value(io_source_index_rec.tablespace_name, io_target_index_rec.tablespace_name) = 1
    THEN
      debug('Recreate index : '||io_source_index_rec.index_name||' - mismatch tablespace');
      RETURN gc_result_recreate;
    END IF;


    IF io_source_index_rec.index_type IN ('FUNCTION-BASED NORMAL', 'FUNCTION-BASED NORMAL/REV', 'FUNCTION-BASED BITMAP') THEN
      IF comp_array(io_source_index_rec.column_expr_arr, io_target_index_rec.column_expr_arr) = 1 THEN
        debug('Recreate index : '||io_source_index_rec.index_name||' - mismatch column expressions');
        RETURN gc_result_recreate;
      END IF;
    ELSE
      IF comp_array(io_source_index_rec.column_expr_arr, io_target_index_rec.column_expr_arr) = 1 OR
         comp_array(io_source_index_rec.column_arr, io_target_index_rec.column_arr) = 1 OR
         comp_array(io_source_index_rec.sort_order_arr, io_target_index_rec.sort_order_arr) = 1
      THEN
        debug('Recreate index : '||io_source_index_rec.index_name||' - mismatch column names/sort');
        RETURN gc_result_recreate;
      END IF;
    END IF;

    IF io_source_index_rec.join_index = 'YES' THEN
      IF comp_join_condition(io_source_index_rec, io_target_index_rec) = 1 THEN
        debug('Recreate index : mismatch join condition');
        RETURN gc_result_recreate;
      END IF;
    END IF;

    -- storage physical attributes (including storage)
    IF in_comp_phys_attr THEN
      IF cort_exec_pkg.g_params.physical_attr.value_exists('INDEX') THEN
        l_comp_result := comp_index_physical_attr(
                           in_source_physical_attr_rec => io_source_index_rec.physical_attr_rec,
                           in_target_physical_attr_rec => io_target_index_rec.physical_attr_rec,
                           out_frwd_clause             => l_frwd_clause,
                           out_rlbk_clause             => l_rlbk_clause
                         );
        CASE l_comp_result
        WHEN gc_result_recreate THEN
          debug('Recreate index: Compare index physical_attr_rec - '||gc_result_recreate);
          RETURN gc_result_recreate;
        WHEN gc_result_alter THEN
          debug('Alter index: Compare index physical_attr_rec - '||gc_result_alter);
          l_result := gc_result_alter;
          l_frwd_clause_indx_arr(gc_physical_attr) := l_frwd_clause;
          l_rlbk_clause_indx_arr(gc_physical_attr) := l_rlbk_clause;
        ELSE
          NULL;
        END CASE;
      END IF;
    END IF;

    -- Logging
    IF comp_value(io_source_index_rec.logging, io_target_index_rec.logging) = 1 THEN
      debug('Alter index: mismatch logging');
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_logging) := get_clause(gc_logging, io_target_index_rec.logging);
      l_rlbk_clause_indx_arr(gc_logging) := get_clause(gc_logging, io_source_index_rec.logging);
    END IF;

    -- Parallel
    IF comp_parallel(
         in_source_parallel_rec  => io_source_index_rec.parallel_rec,
         in_target_parallel_rec  => io_target_index_rec.parallel_rec,
         out_frwd_clause         => l_frwd_clause,
         out_rlbk_clause         => l_rlbk_clause
       ) = 1
    THEN
      debug('Alter index: mismatch parallel');
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_parallel) := get_clause(gc_parallel, l_frwd_clause, '(', ')');
      l_rlbk_clause_indx_arr(gc_parallel) := get_clause(gc_parallel, l_rlbk_clause, '(', ')');
    END IF;

    -- Parameters
    IF comp_value(io_source_index_rec.parameters, io_target_index_rec.parameters) = 1 THEN
      debug('Alter index: mismatch parameters');
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_parameters) := get_clause(gc_parameters, io_target_index_rec.parameters, '(Q''{', '}'')');
      l_rlbk_clause_indx_arr(gc_parameters) := get_clause(gc_parameters, io_source_index_rec.parameters, '(Q''{', '}'')');
    END IF;

    $IF dbms_db_version.version > 12 OR (dbms_db_version.version = 12 AND dbms_db_version.release >= 2) $THEN
    IF comp_value(io_source_index_rec.indexing, io_target_index_rec.indexing) = 1 THEN
      debug('Alter index: mismatch indexing');
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_indexing) := get_clause(gc_indexing, io_target_index_rec.indexing);
      l_rlbk_clause_indx_arr(gc_indexing) := get_clause(gc_indexing, io_source_index_rec.indexing);
    END IF;
    $END

    l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_physical_attr)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_logging)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_parallel)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_parameters)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_indexing);
    l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_physical_attr)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_logging)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_parallel)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_parameters)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_indexing);

    IF l_result = gc_result_alter THEN
      add_alter_index_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_index_name    => io_source_index_rec.index_name,
        in_owner         => io_source_index_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    IF io_target_index_rec.generated = 'N' AND
       comp_value(io_source_index_rec.index_name, io_target_index_rec.rename_rec.object_name) = 1
    THEN
      debug('Alter index: mismatch names');
      l_result := gc_result_alter;
      io_source_index_rec.rename_rec.object_name := io_target_index_rec.rename_rec.object_name;
      l_frwd_clause := 'ALTER INDEX "'||io_source_index_rec.owner||'"."'||io_source_index_rec.index_name||'" '||get_clause(gc_rename_to, io_target_index_rec.rename_rec.object_name, '"', '"');
      l_rlbk_clause := 'ALTER INDEX "'||io_source_index_rec.owner||'"."'||io_target_index_rec.rename_rec.object_name||'" '||get_clause(gc_rename_to, io_source_index_rec.index_name, '"', '"');
      io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := l_frwd_clause;
      io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := l_rlbk_clause;
    END IF;

    RETURN l_result;

  END comp_index;


  -- compare all indexes (exception PK/UK) on table
  FUNCTION comp_indexes(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_hash_index              VARCHAR2(32767);
    l_source_hash_index_arr   arrays.gt_int_indx;
    l_target_hash_index_arr   arrays.gt_int_indx;
    l_source_index_rec        cort_exec_pkg.gt_index_rec;
    l_target_index_rec        cort_exec_pkg.gt_index_rec;
    l_comp_result             PLS_INTEGER;
    l_result                  PLS_INTEGER;
    l_frwd_ddl_arr            arrays.gt_clob_arr;
    l_rlbk_ddl_arr            arrays.gt_clob_arr;
    l_frwd_drop_ddl_arr       arrays.gt_clob_arr;
    l_rlbk_drop_ddl_arr       arrays.gt_clob_arr;
    l_frwd_create_ddl_arr     arrays.gt_clob_arr;
    l_rlbk_create_ddl_arr     arrays.gt_clob_arr;
   -- l_index_stmt_rec          gt_index_statement_rec;
  BEGIN
    -- build for source indexes description indx-array
    FOR i IN 1..in_source_table_rec.index_arr.COUNT LOOP
      IF in_source_table_rec.index_arr(i).constraint_name IS NULL AND in_source_table_rec.index_arr(i).generated <> 'Y' THEN
        l_hash_index := get_index_hash_str(in_source_table_rec.index_arr(i));
        l_source_hash_index_arr(l_hash_index) := i;
--        debug('Source index name: '||in_source_table_rec.index_arr(i).index_name||'  hash = '||l_hash_index);
      END IF;
    END LOOP;

    -- build for target indexes description indx-array
    FOR i IN 1..in_target_table_rec.index_arr.COUNT LOOP
      IF in_target_table_rec.index_arr(i).constraint_name IS NULL AND in_target_table_rec.index_arr(i).generated <> 'Y' THEN
        l_hash_index := get_index_hash_str(in_target_table_rec.index_arr(i));
        l_target_hash_index_arr(l_hash_index) := i;
--        debug('Target index name: '||in_target_table_rec.index_arr(i).index_name||'  hash = '||l_hash_index);
      END IF;
    END LOOP;

    l_result := gc_result_nochange;

    -- run loop through source indexes
    l_hash_index := l_source_hash_index_arr.FIRST;
    WHILE l_hash_index IS NOT NULL LOOP
      l_source_index_rec := in_source_table_rec.index_arr(l_source_hash_index_arr(l_hash_index));

      -- if index exists on both tables
      IF l_target_hash_index_arr.EXISTS(l_hash_index) AND NOT l_source_index_rec.drop_flag THEN
        l_target_index_rec := in_target_table_rec.index_arr(l_target_hash_index_arr(l_hash_index));

        -- if indexed columsn were renamed we need drop index first then
        IF l_source_index_rec.recreate_flag THEN
          debug('index refers renamed columns. Recreate');

          l_result := greatest(gc_result_recreate, l_result);

          drop_index(
            in_index_rec           => l_source_index_rec,
            in_table_name          => in_source_table_rec.rename_rec.object_name,
            io_frwd_alter_stmt_arr => l_frwd_drop_ddl_arr,
            io_rlbk_alter_stmt_arr => l_rlbk_drop_ddl_arr
          );
          create_index(
            in_index_rec           => l_source_index_rec,
            in_table_name          => in_source_table_rec.rename_rec.object_name,
            io_frwd_alter_stmt_arr => l_frwd_create_ddl_arr,
            io_rlbk_alter_stmt_arr => l_rlbk_create_ddl_arr
          );

        ELSE
          -- compare indexes
          l_comp_result := comp_index(
                             in_source_table_rec    => in_source_table_rec,
                             in_target_table_rec    => in_target_table_rec,
                             io_source_index_rec    => l_source_index_rec,
                             io_target_index_rec    => l_target_index_rec,
                             in_comp_phys_attr      => CASE WHEN in_source_table_rec.iot_type IS NULL THEN TRUE ELSE FALSE END,
                             io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                             io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                           );

          debug('Comp index source='||l_source_index_rec.index_name||' and target='||l_target_index_rec.index_name||' result - '||l_comp_result);

          l_result := greatest(l_comp_result, l_result);

          IF l_comp_result = gc_result_recreate THEN
            -- if need to recreate
            -- add DROP/CREATE INDEX statements into output arrays
            drop_index(
              in_index_rec           => l_source_index_rec,
              in_table_name          => in_source_table_rec.rename_rec.object_name,
              io_frwd_alter_stmt_arr => l_frwd_drop_ddl_arr,
              io_rlbk_alter_stmt_arr => l_rlbk_drop_ddl_arr
            );
            create_index(
              in_index_rec           => l_target_index_rec,
              in_table_name          => in_source_table_rec.rename_rec.object_name,
              io_frwd_alter_stmt_arr => l_frwd_create_ddl_arr,
              io_rlbk_alter_stmt_arr => l_rlbk_create_ddl_arr
            );
          END IF;
        END IF;


      ELSE
        l_result := greatest(gc_result_drop, l_result);
        -- if index doesn't exist in target table
        IF l_source_index_rec.drop_flag THEN
          debug('index refers dropped columns. Drop');
        ELSE
          debug('Comp index: drop index source='||l_source_index_rec.index_name);
        END IF;
        drop_index(
          in_index_rec           => l_source_index_rec,
          in_table_name          => in_source_table_rec.rename_rec.object_name,
          io_frwd_alter_stmt_arr => l_frwd_drop_ddl_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_drop_ddl_arr
        );
      END IF;
      l_hash_index := l_source_hash_index_arr.NEXT(l_hash_index);
    END LOOP;

    -- run loop through target indexes
    l_hash_index := l_target_hash_index_arr.FIRST;
    WHILE l_hash_index IS NOT NULL LOOP
      -- if index doesn't exist on source tables
      IF NOT l_source_hash_index_arr.EXISTS(l_hash_index) THEN
        l_target_index_rec := in_target_table_rec.index_arr(l_target_hash_index_arr(l_hash_index));

        l_result := greatest(gc_result_create, l_result);

        debug('Comp index: add index target='||l_target_index_rec.index_name);
        create_index(
          in_index_rec           => l_target_index_rec,
          in_table_name          => in_target_table_rec.rename_rec.object_name,
          io_frwd_alter_stmt_arr => l_frwd_create_ddl_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_create_ddl_arr
        );
      END IF;
      l_hash_index := l_target_hash_index_arr.NEXT(l_hash_index);
    END LOOP;

    -- update join indexes
    FOR i IN 1..in_source_table_rec.join_index_arr.COUNT LOOP
      l_source_index_rec := in_source_table_rec.join_index_arr(i);
      IF l_source_index_rec.drop_flag THEN
        debug('join index refers dropped column. Drop');
        cort_log_pkg.echo('Warning: Join index on removed column is dropped');
        l_result := greatest(gc_result_drop, l_result);
        drop_index(
          in_index_rec           => l_source_index_rec,
          in_table_name          => l_source_index_rec.table_name,
          io_frwd_alter_stmt_arr => l_frwd_drop_ddl_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_drop_ddl_arr
        );
      ELSIF l_source_index_rec.recreate_flag THEN
        debug('join index refers renamed column. Recreate');
        l_result := greatest(gc_result_create, l_result);
        drop_index(
          in_index_rec           => l_source_index_rec,
          in_table_name          => l_source_index_rec.table_name,
          io_frwd_alter_stmt_arr => l_frwd_drop_ddl_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_drop_ddl_arr
        );
        create_index(
          in_index_rec           => l_source_index_rec,
          in_table_name          => l_source_index_rec.table_name,
          io_frwd_alter_stmt_arr => l_frwd_create_ddl_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_create_ddl_arr
        );
      END IF;
    END LOOP;


    IF l_result > gc_result_alter THEN
      FOR i IN 1..l_frwd_drop_ddl_arr.COUNT LOOP
        add_stmt(
          io_frwd_stmt_arr => l_frwd_ddl_arr,
          io_rlbk_stmt_arr => l_rlbk_ddl_arr,
          in_frwd_stmt     => l_frwd_drop_ddl_arr(i),
          in_rlbk_stmt     => l_rlbk_drop_ddl_arr(i)
        );
      END LOOP;
      FOR i IN 1..io_frwd_alter_stmt_arr.COUNT LOOP
        add_stmt(
          io_frwd_stmt_arr => l_frwd_ddl_arr,
          io_rlbk_stmt_arr => l_rlbk_ddl_arr,
          in_frwd_stmt     => io_frwd_alter_stmt_arr(i),
          in_rlbk_stmt     => io_rlbk_alter_stmt_arr(i)
        );
      END LOOP;
      FOR i IN 1..l_frwd_create_ddl_arr.COUNT LOOP
        add_stmt(
          io_frwd_stmt_arr => l_frwd_ddl_arr,
          io_rlbk_stmt_arr => l_rlbk_ddl_arr,
          in_frwd_stmt     => l_frwd_create_ddl_arr(i),
          in_rlbk_stmt     => l_rlbk_create_ddl_arr(i)
        );
      END LOOP;

      io_frwd_alter_stmt_arr := l_frwd_ddl_arr;
      io_rlbk_alter_stmt_arr := l_rlbk_ddl_arr;
    END IF;

    RETURN l_result;
  END comp_indexes;

  -- copies table indexes
  PROCEDURE copy_indexes(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- rollback alter statements
    in_copy_pk_uk            IN BOOLEAN DEFAULT FALSE -- copy indexes for PK/UK
  )
  AS
    l_indx            PLS_INTEGER;
    l_index_full_name VARCHAR2(65);
    l_index_rec       cort_exec_pkg.gt_index_rec;
  BEGIN
    FOR i IN 1..in_source_table_rec.index_arr.COUNT LOOP
      IF in_source_table_rec.index_arr(i).constraint_name IS NULL OR in_copy_pk_uk THEN
        l_indx := io_target_table_rec.index_arr.COUNT+1;
        io_target_table_rec.index_arr(l_indx) := in_source_table_rec.index_arr(i);
        io_target_table_rec.index_arr(l_indx).index_name := in_source_table_rec.index_arr(i).rename_rec.temp_name;
        io_target_table_rec.index_arr(l_indx).rename_rec.object_name := in_source_table_rec.index_arr(i).rename_rec.temp_name;
        io_target_table_rec.index_arr(l_indx).rename_rec.current_name := in_source_table_rec.index_arr(i).rename_rec.temp_name;
        io_target_table_rec.index_arr(l_indx).table_name := io_target_table_rec.table_name;


        l_index_full_name := '"'||io_target_table_rec.index_arr(l_indx).owner||'"."'||io_target_table_rec.index_arr(l_indx).index_name||'"';
        io_target_table_rec.index_indx_arr(l_index_full_name) := l_indx;

        l_index_rec := io_target_table_rec.index_arr(l_indx);
        IF io_target_table_rec.partitioned = 'YES' AND l_index_rec.index_type = 'BITMAP' THEN
          l_index_rec.locality := 'LOCAL';
        END IF;

        create_index(
          in_index_rec             => l_index_rec,
          in_table_name            => l_index_rec.table_name,
          io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
          io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
        );
        io_target_table_rec.index_arr(l_indx).rename_rec.object_name := in_source_table_rec.index_arr(i).rename_rec.object_name;
      END IF;
    END LOOP;
    FOR i IN 1..in_source_table_rec.join_index_arr.COUNT LOOP
      io_target_table_rec.join_index_arr(i) := in_source_table_rec.index_arr(i);
      io_target_table_rec.join_index_arr(i).index_name := in_source_table_rec.index_arr(i).rename_rec.temp_name;
      io_target_table_rec.join_index_arr(i).rename_rec.object_name := in_source_table_rec.index_arr(i).rename_rec.temp_name;
      io_target_table_rec.join_index_arr(i).rename_rec.current_name := in_source_table_rec.index_arr(i).rename_rec.temp_name;
      io_target_table_rec.join_index_arr(i).table_name := io_target_table_rec.table_name;
      l_index_full_name := '"'||io_target_table_rec.join_index_arr(i).owner||'"."'||io_target_table_rec.join_index_arr(i).index_name||'"';
      io_target_table_rec.join_index_indx_arr(l_index_full_name) := i;
      create_index(
        in_index_rec             => io_target_table_rec.join_index_arr(i),
        in_table_name            => io_target_table_rec.join_index_arr(i).table_name,
        io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
        io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
      );
      io_target_table_rec.join_index_arr(i).rename_rec.object_name := in_source_table_rec.index_arr(i).rename_rec.object_name;
    END LOOP;
  END copy_indexes;

  -- compares all constraints (exception NOT NULL)
  FUNCTION comp_constraint(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_source_constraint_rec IN cort_exec_pkg.gt_constraint_rec,
    in_target_constraint_rec IN cort_exec_pkg.gt_constraint_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_result := gc_result_nochange;
    IF comp_value(in_source_constraint_rec.constraint_type, in_target_constraint_rec.constraint_type) = 1 OR
       comp_value(in_source_constraint_rec.deferrable, in_target_constraint_rec.deferrable) = 1 OR
       comp_value(in_source_constraint_rec.delete_rule, in_target_constraint_rec.delete_rule) = 1
    THEN
      debug('Recreate constraint '||in_source_constraint_rec.constraint_name||' - changed constraint type/deferrable/delete_rule');
      RETURN gc_result_recreate;
    END IF;

    IF comp_array(in_source_constraint_rec.column_arr, in_target_constraint_rec.column_arr) = 1 THEN
      debug('Recreate constraint '||in_source_constraint_rec.constraint_name||' - column list not match');
      RETURN gc_result_recreate;
    END IF;

    l_frwd_clause_indx_arr(gc_constraint_name) := get_clause(gc_modify_constraint, in_target_constraint_rec.rename_rec.object_name, '"', '"');
    l_rlbk_clause_indx_arr(gc_constraint_name) := get_clause(gc_modify_constraint, in_source_constraint_rec.constraint_name, '"', '"');

    IF comp_value(in_source_constraint_rec.deferred, in_target_constraint_rec.deferred) = 1 AND
       in_source_constraint_rec.deferrable = 'DEFERRABLE'
    THEN
      l_result := gc_result_alter;
      debug('Alter constraint '||in_source_constraint_rec.constraint_name||' - changed deferrable');
      l_frwd_clause_indx_arr(gc_deferred) := get_clause(gc_deferred, in_target_constraint_rec.deferred);
      l_rlbk_clause_indx_arr(gc_deferred) := get_clause(gc_deferred, in_source_constraint_rec.deferred);
    END IF;

    IF comp_value(in_source_constraint_rec.status, in_target_constraint_rec.status) = 1 THEN
      l_result := gc_result_alter;
      debug('Alter constraint '||in_source_constraint_rec.constraint_name||' - changed status');
      l_frwd_clause_indx_arr(gc_status) := get_clause(gc_status, in_target_constraint_rec.status);
      l_rlbk_clause_indx_arr(gc_status) := get_clause(gc_status, in_source_constraint_rec.status);
    END IF;


    IF comp_value(in_source_constraint_rec.validated, in_target_constraint_rec.validated) = 1 THEN
      l_result := gc_result_alter;
      debug('Alter constraint '||in_source_constraint_rec.constraint_name||' - changed validated');
      l_frwd_clause_indx_arr(gc_validated) := get_clause(gc_validated, in_target_constraint_rec.validated);
      l_rlbk_clause_indx_arr(gc_validated) := get_clause(gc_validated, in_source_constraint_rec.validated);
    END IF;

    l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_constraint_name)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_deferred)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_status)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_validated);
    l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_constraint_name)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_deferred)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_status)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_validated);

    IF l_result = gc_result_alter THEN
      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    IF in_target_constraint_rec.generated = 'USER NAME' AND
       comp_value(in_source_constraint_rec.constraint_name, in_target_constraint_rec.rename_rec.object_name) = 1
    THEN
      l_result := gc_result_alter;
      l_frwd_clause := get_clause(gc_rename_constraint , '"'||in_source_constraint_rec.constraint_name||'" TO "'||in_target_constraint_rec.rename_rec.object_name||'"');
      l_rlbk_clause := get_clause(gc_rename_constraint, '"'||in_target_constraint_rec.rename_rec.object_name||'" TO "'||in_source_constraint_rec.constraint_name||'"');
      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    RETURN l_result;

  END comp_constraint;

  -- compares all supplemental logs
  FUNCTION comp_log_group(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_source_log_group_rec  IN cort_exec_pkg.gt_log_group_rec,
    in_target_log_group_rec  IN cort_exec_pkg.gt_log_group_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    l_result := gc_result_nochange;
    IF comp_value(in_source_log_group_rec.log_group_type, in_target_log_group_rec.log_group_type) = 1 OR
       comp_array(in_source_log_group_rec.column_arr, in_target_log_group_rec.column_arr) = 1 OR
       comp_array(in_source_log_group_rec.column_log_arr, in_target_log_group_rec.column_log_arr) = 1 OR
       comp_value(in_source_log_group_rec.always, in_target_log_group_rec.always) = 1
    THEN
      RETURN gc_result_recreate;
    END IF;

    IF in_target_log_group_rec.generated = 'USER NAME' AND
       comp_value(in_source_log_group_rec.log_group_name, in_target_log_group_rec.rename_rec.object_name) = 1
    THEN
      l_result := gc_result_alter;
      l_frwd_clause := get_clause(gc_rename_constraint , '"'||in_source_log_group_rec.log_group_name||'" TO "'||in_target_log_group_rec.rename_rec.object_name||'"');
      l_rlbk_clause := get_clause(gc_rename_constraint, '"'||in_target_log_group_rec.rename_rec.object_name||'" TO "'||in_source_log_group_rec.log_group_name||'"');
      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;
  END comp_log_group;

  -- return unique hash string for constraint depending on it's type
  FUNCTION get_constraint_hash(
    in_constraint_rec IN cort_exec_pkg.gt_constraint_rec
  )
  RETURN VARCHAR2
  AS
    l_result       VARCHAR2(32767);
  BEGIN
    CASE
    WHEN in_constraint_rec.constraint_type IN ('C', 'F') THEN
      l_result := 'C:'||SUBSTR(in_constraint_rec.search_condition,1,32765);
    WHEN in_constraint_rec.constraint_type IN ('P', 'U') THEN
      l_result := 'P:'||convert_arr_to_str(in_constraint_rec.column_arr);
    WHEN in_constraint_rec.constraint_type IN ('R') THEN
      l_result := 'R:'||convert_arr_to_str(in_constraint_rec.column_arr)||' R "'||
                  in_constraint_rec.r_owner||'"."'||in_constraint_rec.r_table_name||'":'||
                  convert_arr_to_str(in_constraint_rec.r_column_arr);
    END CASE;
    RETURN l_result;
  END get_constraint_hash;

  -- returns array of FK constraints referencing on giving PK/UK constraint
  PROCEDURE drop_references(
    in_table_rec             IN cort_exec_pkg.gt_table_rec,
    in_pk_constraint_rec     IN cort_exec_pkg.gt_constraint_rec,
    out_ref_constraint_arr   OUT NOCOPY cort_exec_pkg.gt_constraint_arr,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
  BEGIN
    FOR i IN 1..in_table_rec.ref_constraint_arr.COUNT LOOP
      IF in_table_rec.ref_constraint_arr(i).r_constraint_name = in_pk_constraint_rec.constraint_name THEN
        out_ref_constraint_arr(out_ref_constraint_arr.COUNT+1) := in_table_rec.ref_constraint_arr(i);
      END IF;
    END LOOP;
    FOR i IN 1..out_ref_constraint_arr.COUNT LOOP
      drop_constraint(
        in_constraint_rec      => out_ref_constraint_arr(i),
        in_index_rec           => NULL,
        io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
      );
    END LOOP;
  END drop_references;

  PROCEDURE restore_references(
    io_ref_contraint_arr     IN OUT NOCOPY cort_exec_pkg.gt_constraint_arr,
    in_old_pk_constraint_rec IN cort_exec_pkg.gt_constraint_rec,
    in_new_pk_constraint_rec IN cort_exec_pkg.gt_constraint_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_comp_result             PLS_INTEGER;
    l_old_pk_column_indx_arr  arrays.gt_int_indx;
    l_new_pk_column_indx_arr  arrays.gt_int_indx;
    l_old_column_arr          arrays.gt_name_arr;
    l_indx                    arrays.gt_name;
  BEGIN
    -- find new order of columns
    FOR i IN 1..in_old_pk_constraint_rec.column_arr.COUNT LOOP
      l_old_pk_column_indx_arr(in_old_pk_constraint_rec.column_arr(i)) := i;
    END LOOP;

    l_comp_result := 1;
    FOR i IN 1..in_new_pk_constraint_rec.column_arr.COUNT LOOP
      l_new_pk_column_indx_arr(in_new_pk_constraint_rec.column_arr(i)) := i;
      IF l_old_pk_column_indx_arr.EXISTS(in_new_pk_constraint_rec.column_arr(i)) THEN
        l_comp_result := 0;
      ELSE
        l_comp_result := 1;
        EXIT;
      END IF;
    END LOOP;

    -- Check that set of KEY columns has not been changed
    IF l_comp_result = 0
    THEN
      -- If set of the key columns is the same but in different order then FK will be updated
      IF comp_array(in_old_pk_constraint_rec.column_arr, in_new_pk_constraint_rec.column_arr) = 1 THEN

        -- Update all references
        FOR i IN 1..io_ref_contraint_arr.COUNT LOOP
          -- Save existing columns
          l_old_column_arr := io_ref_contraint_arr(i).column_arr;

          -- Reorder columns
          io_ref_contraint_arr(i).column_arr.DELETE;
          l_indx := l_old_pk_column_indx_arr.FIRST;
          WHILE l_indx IS NOT NULL LOOP
            io_ref_contraint_arr(i).column_arr(l_new_pk_column_indx_arr(l_indx)) := l_old_column_arr(l_old_pk_column_indx_arr(l_indx));
            l_indx := l_old_pk_column_indx_arr.NEXT(l_indx);
          END LOOP;

          -- Save existing columns
          io_ref_contraint_arr(i).r_column_arr := in_new_pk_constraint_rec.column_arr;

        END LOOP;
      ELSE
        -- Keep references unchanged
        NULL;
      END IF;
    ELSE
      -- drop all references
      cort_log_pkg.echo('Warning: Unable to preserve references on changing primary/unique key');
      io_ref_contraint_arr.DELETE;
    END IF;

    FOR i IN 1..io_ref_contraint_arr.COUNT LOOP
      IF NOT cort_exec_pkg.g_params.validate.get_bool_value THEN
        io_ref_contraint_arr(i).validated := 'NOT VALIDATED';
        io_ref_contraint_arr(i).rename_rec.current_name := io_ref_contraint_arr(i).rename_rec.object_name;
      END IF;

      add_constraint(
        in_constraint_rec      => io_ref_contraint_arr(i),
        in_index_rec           => NULL,
        io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
      );
    END LOOP;
  END restore_references;

  -- compare all constraints
  FUNCTION comp_constraints(
    io_source_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_index_result           PLS_INTEGER;
    l_cons_result            PLS_INTEGER;
    l_source_hash_cons_arr   arrays.gt_int_indx;
    l_target_hash_cons_arr   arrays.gt_int_indx;
    l_source_constraint_rec  cort_exec_pkg.gt_constraint_rec;
    l_target_constraint_rec  cort_exec_pkg.gt_constraint_rec;
    l_source_index_rec       cort_exec_pkg.gt_index_rec;
    l_target_index_rec       cort_exec_pkg.gt_index_rec;
    l_hash_index             VARCHAR2(32767);
    l_ref_constraint_arr     cort_exec_pkg.gt_constraint_arr;
    l_indx                   PLS_INTEGER;
    l_dropped_index_arr      arrays.gt_int_indx;
  BEGIN
    l_result := gc_result_nochange;
    -- prepare hash-indexed array of check source constraints
    FOR i IN 1..io_source_table_rec.constraint_arr.COUNT LOOP
      IF io_source_table_rec.constraint_arr(i).constraint_type IN ('C', 'F', 'P', 'U', 'R') THEN
        l_hash_index := get_constraint_hash(io_source_table_rec.constraint_arr(i));
        l_source_hash_cons_arr(l_hash_index) := i;
        debug('Source constraint '||io_source_table_rec.constraint_arr(i).constraint_name||' - hash = '||l_hash_index);
      END IF;
    END LOOP;
    -- prepare hash-indexed array of check target constraints
    FOR i IN 1..io_target_table_rec.constraint_arr.COUNT LOOP
      IF io_target_table_rec.constraint_arr(i).constraint_type IN ('C', 'F', 'P', 'U', 'R') THEN
        l_hash_index := get_constraint_hash(io_target_table_rec.constraint_arr(i));
        l_target_hash_cons_arr(l_hash_index) := i;
        debug('Target constraint '||io_target_table_rec.constraint_arr(i).constraint_name||' - hash = '||l_hash_index);
      END IF;
    END LOOP;

    -- run loop through source constraints
    l_hash_index := l_source_hash_cons_arr.FIRST;
    WHILE l_hash_index IS NOT NULL LOOP
      -- if constraint exists on both tables
      IF NOT io_source_table_rec.constraint_arr(l_source_hash_cons_arr(l_hash_index)).drop_flag THEN
        IF l_target_hash_cons_arr.EXISTS(l_hash_index) THEN
          l_source_constraint_rec := io_source_table_rec.constraint_arr(l_source_hash_cons_arr(l_hash_index));
          l_target_constraint_rec := io_target_table_rec.constraint_arr(l_target_hash_cons_arr(l_hash_index));
          l_source_index_rec := get_pk_index_rec(io_source_table_rec, l_source_constraint_rec);
          l_target_index_rec := get_pk_index_rec(io_target_table_rec, l_target_constraint_rec);
          IF l_source_constraint_rec.constraint_type IN ('P','U') THEN
            IF l_source_constraint_rec.index_name IS NOT NULL AND
               l_target_constraint_rec.index_name IS NOT NULL
            THEN
              l_index_result := comp_index(
                                  in_source_table_rec    => io_source_table_rec,
                                  in_target_table_rec    => io_target_table_rec,
                                  io_source_index_rec    => l_source_index_rec,
                                  io_target_index_rec    => l_target_index_rec,
                                  in_comp_phys_attr      => CASE WHEN io_source_table_rec.iot_type IS NULL THEN TRUE ELSE FALSE END,
                                  io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                                  io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                                );
              debug('Compare constraints indexes: '||l_source_index_rec.index_name||' AND '||l_target_index_rec.index_name||' - '||l_index_result);
            ELSIF l_source_constraint_rec.index_name IS NULL AND
                  l_target_constraint_rec.index_name IS NULL THEN
              l_index_result := gc_result_nochange;
            ELSE
              l_index_result := gc_result_recreate;
              l_result := gc_result_alter;
            END IF;
            debug('Compare PK/UK indexes - '||l_index_result);
          ELSE
            l_index_result := gc_result_nochange;
          END IF;

          IF l_index_result = gc_result_recreate THEN
            -- force to recreate constraint
            l_cons_result := gc_result_recreate;
            l_result := gc_result_alter;
          ELSE
            -- compare constraints
            l_cons_result := comp_constraint(
                               in_source_table_rec      => io_source_table_rec,
                               in_target_table_rec      => io_target_table_rec,
                               in_source_constraint_rec => l_source_constraint_rec,
                               in_target_constraint_rec => l_target_constraint_rec,
                               io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
                               io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
                             );
            debug('Compare constraint '||l_source_constraint_rec.constraint_name||' - '||l_cons_result);
          END IF;

          -- if constraint need to be recreated
          IF l_cons_result = gc_result_recreate THEN

            -- for reference partitioning not possible to drop and change reference constraint
            IF l_source_constraint_rec.constraint_name = io_source_table_rec.ref_ptn_constraint_name THEN
              RETURN gc_result_recreate;
            END IF;

            -- for IOT PK is not possible to drop and recreate primary key constraint
            IF l_source_constraint_rec.index_name = io_source_table_rec.iot_index_name AND
               l_source_constraint_rec.index_owner = io_source_table_rec.iot_index_owner
            THEN
              RETURN gc_result_recreate;
            END IF;

            -- recreate constraint
            -- drop references for PK and UK
            IF l_source_constraint_rec.constraint_type IN ('P','U') AND
               l_source_constraint_rec.has_references
            THEN
              drop_references(
                in_table_rec           => io_source_table_rec,
                in_pk_constraint_rec   => l_source_constraint_rec,
                out_ref_constraint_arr => l_ref_constraint_arr,
                io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
              );
            END IF;

            drop_constraint(
              in_constraint_rec      => l_source_constraint_rec,
              in_index_rec           => l_source_index_rec,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
            IF l_index_result >= gc_result_recreate THEN
              IF l_source_constraint_rec.index_name IS NOT NULL AND NOT l_dropped_index_arr.EXISTS(l_source_index_rec.index_name) THEN
                drop_index(
                  in_index_rec           => l_source_index_rec,
                  in_table_name          => l_source_index_rec.table_name,
                  io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                  io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                );
                l_dropped_index_arr(l_source_index_rec.index_name) := 1;
              END IF;
              IF l_target_constraint_rec.index_name IS NOT NULL THEN
                l_target_index_rec.table_name := io_source_table_rec.table_name;
                l_target_index_rec.table_owner := io_source_table_rec.owner;
                l_target_index_rec.rename_rec.current_name := l_source_index_rec.constraint_name;
                create_index(
                  in_index_rec           => l_target_index_rec,
                  in_table_name          => l_target_index_rec.table_name,
                  io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                  io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                );
                l_dropped_index_arr.DELETE(l_source_index_rec.index_name);
              END IF;
            ELSE
              l_target_index_rec := l_source_index_rec;
            END IF;
            l_target_constraint_rec.rename_rec.current_name := l_target_constraint_rec.rename_rec.object_name;
            l_target_constraint_rec.table_name := io_source_table_rec.table_name;
            add_constraint(
              in_constraint_rec      => l_target_constraint_rec,
              in_index_rec           => l_target_index_rec,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
            -- For PK/UK
            IF l_source_constraint_rec.constraint_type IN ('P','U') AND
               l_source_constraint_rec.has_references AND
               cort_exec_pkg.g_params.keep_objects.value_exists('REFERENCES')
            THEN
              restore_references(
                io_ref_contraint_arr      => l_ref_constraint_arr,
                in_old_pk_constraint_rec  => l_source_constraint_rec,
                in_new_pk_constraint_rec  => l_target_constraint_rec,
                io_frwd_alter_stmt_arr    => io_frwd_alter_stmt_arr,
                io_rlbk_alter_stmt_arr    => io_rlbk_alter_stmt_arr
              );
            END IF;
            l_result := gc_result_alter;
          ELSE
            l_result := GREATEST(l_result, l_cons_result, l_index_result);
          END IF;
        ELSE
          -- if constraint doesn't exist in NEW table
          l_source_constraint_rec := io_source_table_rec.constraint_arr(l_source_hash_cons_arr(l_hash_index));
          l_source_index_rec := get_pk_index_rec(io_source_table_rec, l_source_constraint_rec);

          -- drop references
          IF l_source_constraint_rec.constraint_type IN ('P','U') AND
             l_source_constraint_rec.has_references
          THEN
            cort_log_pkg.echo('Warning! Unable to preserve references on droping primary/unique key');

            debug('Drop reference on '||l_source_constraint_rec.constraint_name);
            drop_references(
              in_table_rec           => io_source_table_rec,
              in_pk_constraint_rec   => l_source_constraint_rec,
              out_ref_constraint_arr => l_ref_constraint_arr,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
          END IF;
          debug('Drop constraint '||l_source_constraint_rec.constraint_name);
          l_source_constraint_rec.rename_rec.current_name := l_source_constraint_rec.constraint_name;
          -- drop constraint
          drop_constraint(
            in_constraint_rec      => l_source_constraint_rec,
            in_index_rec           => l_source_index_rec,
            io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
            io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
          );

          IF l_source_constraint_rec.index_name IS NOT NULL AND NOT l_dropped_index_arr.EXISTS(l_source_index_rec.index_name) THEN
            debug('Drop index '||l_source_index_rec.index_name);
            -- drop index
            drop_index(
              in_index_rec           => l_source_index_rec,
              in_table_name          => l_source_index_rec.table_name,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
            l_dropped_index_arr(l_source_index_rec.index_name) := 1;
          END IF;
          io_source_table_rec.constraint_arr(l_source_hash_cons_arr(l_hash_index)).drop_flag := TRUE;
          l_result := gc_result_alter;
        END IF;
      END IF;
      l_hash_index := l_source_hash_cons_arr.NEXT(l_hash_index);
    END LOOP;

    -- run loop through target constraints
    l_hash_index := l_target_hash_cons_arr.FIRST;
    WHILE l_hash_index IS NOT NULL LOOP
      -- if constraint doesn't exist on source tables
      IF NOT l_source_hash_cons_arr.EXISTS(l_hash_index) THEN
        l_target_constraint_rec := io_target_table_rec.constraint_arr(l_target_hash_cons_arr(l_hash_index));
        l_target_constraint_rec.rename_rec.current_name := l_target_constraint_rec.rename_rec.object_name;

        l_target_index_rec := get_pk_index_rec(io_target_table_rec, l_target_constraint_rec);
        l_target_index_rec.rename_rec.current_name := l_target_index_rec.rename_rec.object_name;

        IF l_target_index_rec.index_name IS NOT NULL THEN
          IF io_source_table_rec.index_indx_arr.EXISTS('"'||l_target_index_rec.owner||'"."'||l_target_index_rec.rename_rec.object_name||'"') THEN
            l_indx := io_source_table_rec.index_indx_arr('"'||l_target_index_rec.owner||'"."'||l_target_index_rec.rename_rec.object_name||'"');
          ELSE
            l_indx := NULL;
          END IF;
          IF l_indx IS NOT NULL THEN
            l_source_index_rec := io_source_table_rec.index_arr(l_indx);
            l_index_result := comp_index(
                                in_source_table_rec    => io_source_table_rec,
                                in_target_table_rec    => io_target_table_rec,
                                io_source_index_rec    => l_source_index_rec,
                                io_target_index_rec    => l_target_index_rec,
                                in_comp_phys_attr      => CASE WHEN io_source_table_rec.iot_type IS NULL THEN TRUE ELSE FALSE END,
                                io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                                io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                              );
            IF l_index_result = gc_result_recreate THEN
              IF NOT l_dropped_index_arr.EXISTS(l_source_index_rec.index_name) THEN
                drop_index(
                  in_index_rec           => l_source_index_rec,
                  in_table_name          => l_source_index_rec.table_name,
                  io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                  io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                );
                l_dropped_index_arr(l_source_index_rec.index_name) := 1;
              END IF;
              l_target_index_rec.table_name := io_source_table_rec.table_name;
              l_target_index_rec.table_owner := io_source_table_rec.owner;
              create_index(
                in_index_rec           => l_target_index_rec,
                in_table_name          => l_target_index_rec.table_name,
                io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
              );
              l_dropped_index_arr.DELETE(l_source_index_rec.index_name);
            END IF;
            l_result := gc_result_alter;
          ELSE
            l_target_index_rec.table_name := io_source_table_rec.table_name;
            l_target_index_rec.table_owner := io_source_table_rec.owner;
            create_index(
              in_index_rec           => l_target_index_rec,
              in_table_name          => l_target_index_rec.table_name,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
            l_result := gc_result_alter;
          END IF;
        END IF;
        l_target_constraint_rec.table_name := io_source_table_rec.table_name;
        -- add constraint
        add_constraint(
          in_constraint_rec      => l_target_constraint_rec,
          in_index_rec           => l_target_index_rec,
          io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
        );
        l_result := gc_result_alter;
      END IF;
      l_hash_index := l_target_hash_cons_arr.NEXT(l_hash_index);
    END LOOP;

    RETURN l_result;
  END comp_constraints;


  -- copy all references from source table to the target one
  PROCEDURE copy_references(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_result                     PLS_INTEGER;
    l_source_hash_cons_arr       arrays.gt_int_indx;
    l_target_hash_cons_arr       arrays.gt_int_indx;
    l_source_constraint_rec      cort_exec_pkg.gt_constraint_rec;
    l_source_ref_constraint_rec  cort_exec_pkg.gt_constraint_rec;
    l_target_constraint_rec      cort_exec_pkg.gt_constraint_rec;
    l_hash_index                 VARCHAR2(32767);
  BEGIN
    l_result := gc_result_nochange;
    -- prepare hash-indexed array of check source constraints
    FOR i IN 1..in_source_table_rec.constraint_arr.COUNT LOOP
      IF in_source_table_rec.constraint_arr(i).constraint_type IN ('P', 'U') AND
         in_source_table_rec.constraint_arr(i).has_references
      THEN
        l_hash_index := get_constraint_hash(in_source_table_rec.constraint_arr(i));
        l_source_hash_cons_arr(l_hash_index) := i;
      END IF;
    END LOOP;
    -- prepare hash-indexed array of check target constraints
    FOR i IN 1..io_target_table_rec.constraint_arr.COUNT LOOP
      IF io_target_table_rec.constraint_arr(i).constraint_type IN ('P', 'U') THEN
        l_hash_index := get_constraint_hash(io_target_table_rec.constraint_arr(i));
        l_target_hash_cons_arr(l_hash_index) := i;
      END IF;
    END LOOP;

    io_target_table_rec.ref_constraint_arr.DELETE;
    io_target_table_rec.ref_constraint_indx_arr.DELETE;

    -- run loop through source constraints
    l_hash_index := l_source_hash_cons_arr.FIRST;
    WHILE l_hash_index IS NOT NULL LOOP
      -- if constraint exists on both tables
      IF l_target_hash_cons_arr.EXISTS(l_hash_index) THEN
        l_source_constraint_rec := in_source_table_rec.constraint_arr(l_source_hash_cons_arr(l_hash_index));
        l_target_constraint_rec := io_target_table_rec.constraint_arr(l_target_hash_cons_arr(l_hash_index));

        -- get references for PK and UK
        FOR i IN 1..in_source_table_rec.ref_constraint_arr.COUNT LOOP
          IF in_source_table_rec.ref_constraint_arr(i).r_constraint_name = l_source_constraint_rec.constraint_name THEN
            io_target_table_rec.ref_constraint_arr(io_target_table_rec.ref_constraint_arr.COUNT+1) := in_source_table_rec.ref_constraint_arr(i);
            io_target_table_rec.ref_constraint_indx_arr(in_source_table_rec.ref_constraint_arr(i).constraint_name) := io_target_table_rec.ref_constraint_arr.COUNT;
            -- drop reference constraint to original table
            l_source_ref_constraint_rec := in_source_table_rec.ref_constraint_arr(i);
            l_source_ref_constraint_rec.validated := 'NOT VALIDATED';
            drop_constraint(
              in_constraint_rec      => l_source_ref_constraint_rec,
              in_index_rec           => NULL,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
          END IF;
        END LOOP;

        -- remap references
        FOR i IN 1..io_target_table_rec.ref_constraint_arr.COUNT LOOP
          -- remap to new table
          io_target_table_rec.ref_constraint_arr(i).r_owner := io_target_table_rec.owner;
          io_target_table_rec.ref_constraint_arr(i).r_table_name := io_target_table_rec.rename_rec.current_name;
          io_target_table_rec.ref_constraint_arr(i).r_constraint_name := l_target_constraint_rec.constraint_name;
          IF NOT cort_exec_pkg.g_params.validate.get_bool_value THEN
            io_target_table_rec.ref_constraint_arr(i).validated := 'NOT VALIDATED';
          END IF;
          io_target_table_rec.ref_constraint_arr(i).rename_rec.current_name := io_target_table_rec.ref_constraint_arr(i).rename_rec.temp_name;
        END LOOP;

        restore_references(
          io_ref_contraint_arr      => io_target_table_rec.ref_constraint_arr,
          in_old_pk_constraint_rec  => l_source_constraint_rec,
          in_new_pk_constraint_rec  => l_target_constraint_rec,
          io_frwd_alter_stmt_arr    => io_frwd_alter_stmt_arr,
          io_rlbk_alter_stmt_arr    => io_rlbk_alter_stmt_arr
        );

      ELSE
        -- if constraint doesn't exist in NEW table
        l_source_constraint_rec := in_source_table_rec.constraint_arr(l_source_hash_cons_arr(l_hash_index));

        cort_log_pkg.echo('Warning! Unable to preserve references on droping primary/unique key');

      END IF;
      l_hash_index := l_source_hash_cons_arr.NEXT(l_hash_index);
    END LOOP;

  END copy_references;

  -- returns DDL to drop renamed ref and 2 step rollback: create without validation + validation
  PROCEDURE drop_references(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_constraint_rec         cort_exec_pkg.gt_constraint_rec;
  BEGIN
    FOR i IN 1..in_source_table_rec.ref_constraint_arr.COUNT LOOP
      l_constraint_rec := in_source_table_rec.ref_constraint_arr(i);
      l_constraint_rec.validated := 'NOT VALIDATED';
      drop_constraint(
        in_constraint_rec      => l_constraint_rec,
        in_index_rec           => NULL,
        io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
      );
    END LOOP;
  END drop_references;

  -- return unique hash string for supplemental log depending on it's type
  FUNCTION get_log_group_hash(in_log_group_rec IN cort_exec_pkg.gt_log_group_rec)
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN in_log_group_rec.log_group_type||':'||convert_arr_to_str(in_log_group_rec.column_arr)||':'||convert_arr_to_str(in_log_group_rec.column_log_arr);
  END get_log_group_hash;

  -- compare lob rec
  FUNCTION comp_lob_params(
    in_source_lob_rec    IN cort_exec_pkg.gt_lob_rec,
    in_target_lob_rec    IN cort_exec_pkg.gt_lob_rec,
    out_frwd_lob_parames OUT NOCOPY CLOB,
    out_rlbk_lob_parames OUT NOCOPY CLOB
  )
  RETURN NUMBER
  AS
    l_result                 PLS_INTEGER;
    l_comp_result            PLS_INTEGER;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
    l_storage_frwd_clause    CLOB;
    l_storage_rlbk_clause    CLOB;
  BEGIN
    l_result := gc_result_nochange;

    -- move or modify
    IF comp_value(in_source_lob_rec.pctversion, in_target_lob_rec.pctversion) = 1 AND
       in_target_lob_rec.securefile = 'NO'
    THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_pctversion) := get_clause(gc_pctversion, in_target_lob_rec.pctversion);
      l_rlbk_clause_indx_arr(gc_pctversion) := get_clause(gc_pctversion, in_source_lob_rec.pctversion);
    END IF;

    IF comp_value(in_source_lob_rec.freepools, in_target_lob_rec.freepools) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_freepools) := get_clause(gc_freepools, in_target_lob_rec.freepools);
      l_rlbk_clause_indx_arr(gc_freepools) := get_clause(gc_freepools, in_source_lob_rec.freepools);
    END IF;
    IF in_target_lob_rec.securefile = 'YES' AND
       (comp_value(in_source_lob_rec.retention, in_target_lob_rec.retention) = 1 OR
       (in_target_lob_rec.retention = 'MIN' AND comp_value(in_source_lob_rec.min_retention, in_target_lob_rec.min_retention) = 1))
    THEN
      debug('in_source_lob_rec.securefile = '||in_source_lob_rec.securefile);
      debug('in_target_lob_rec.securefile = '||in_target_lob_rec.securefile);
      debug('in_source_lob_rec.retention = '||in_source_lob_rec.retention);
      debug('in_target_lob_rec.retention = '||in_target_lob_rec.retention);
      debug('in_source_lob_rec.minretention = '||in_source_lob_rec.min_retention);
      debug('in_target_lob_rec.minretention = '||in_target_lob_rec.min_retention);
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_retention) := get_clause(gc_retention, in_target_lob_rec.retention) || case when in_target_lob_rec.retention = 'MIN' then ' '||to_char(in_target_lob_rec.min_retention) end;
      l_rlbk_clause_indx_arr(gc_retention) := get_clause(gc_retention, in_source_lob_rec.retention)|| case when in_source_lob_rec.retention = 'MIN' then ' '||to_char(in_source_lob_rec.min_retention) end;
    END IF;

    IF comp_value(in_source_lob_rec.deduplication, in_target_lob_rec.deduplication) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_deduplication) := get_clause(gc_deduplication, in_target_lob_rec.deduplication);
      l_rlbk_clause_indx_arr(gc_deduplication) := get_clause(gc_deduplication, in_source_lob_rec.deduplication);
    END IF;

    IF comp_value(in_source_lob_rec.compression, in_target_lob_rec.compression) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_lob_compression) := get_clause(gc_lob_compression, in_target_lob_rec.compression);
      l_rlbk_clause_indx_arr(gc_lob_compression) := get_clause(gc_lob_compression, in_source_lob_rec.compression);
    END IF;

    IF comp_value(in_source_lob_rec.encrypt, in_target_lob_rec.encrypt) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_encrypt) := get_clause(gc_encrypt, in_target_lob_rec.encrypt);
      l_rlbk_clause_indx_arr(gc_encrypt) := get_clause(gc_encrypt, in_source_lob_rec.encrypt);
    END IF;

    IF comp_value(in_source_lob_rec.cache, in_target_lob_rec.cache) = 1 OR
       comp_value(in_source_lob_rec.logging, in_target_lob_rec.logging) = 1
    THEN
      l_result := gc_result_alter;
    debug('in_source_lob_rec.cache = '||in_source_lob_rec.cache);
    debug('in_target_lob_rec.cache = '||in_target_lob_rec.cache);
    debug('in_source_lob_rec.logging = '||in_source_lob_rec.logging);
    debug('in_target_lob_rec.logging = '||in_target_lob_rec.logging);
      l_frwd_clause_indx_arr(gc_cache) := get_clause(gc_cache, in_target_lob_rec.cache);
      l_rlbk_clause_indx_arr(gc_cache) := get_clause(gc_cache, in_source_lob_rec.cache);
      l_frwd_clause_indx_arr(gc_logging) := get_clause(gc_logging, in_target_lob_rec.logging);
      l_rlbk_clause_indx_arr(gc_logging) := get_clause(gc_logging, in_source_lob_rec.logging);
    END IF;

    l_comp_result := comp_storage(
                       in_source_storage_rec => in_source_lob_rec.storage,
                       in_target_storage_rec => in_target_lob_rec.storage,
                       out_frwd_clause       => l_storage_frwd_clause,
                       out_rlbk_clause       => l_storage_rlbk_clause
                     );

    CASE l_comp_result
    WHEN gc_result_recreate THEN
      RETURN gc_result_recreate;
    WHEN gc_result_alter THEN
      l_result := gc_result_alter;
      debug('l_storage_frwd_clause = '||l_storage_frwd_clause);
      l_frwd_clause_indx_arr(gc_storage) := get_clause(gc_storage, l_storage_frwd_clause, '(', ')');
      l_rlbk_clause_indx_arr(gc_storage) := get_clause(gc_storage, l_storage_rlbk_clause, '(', ')');
    ELSE
      NULL;
    END CASE;

    -- move only
    IF comp_value(in_source_lob_rec.chunk, in_target_lob_rec.chunk) = 1 THEN
      l_result := gc_result_alter_move;
      l_frwd_clause_indx_arr(gc_chunk) := get_clause(gc_chunk, in_target_lob_rec.chunk);
      l_rlbk_clause_indx_arr(gc_chunk) := get_clause(gc_chunk, in_source_lob_rec.chunk);
    END IF;

    IF comp_value(in_source_lob_rec.in_row, in_target_lob_rec.in_row) = 1 THEN
      l_result := gc_result_alter_move;
      l_frwd_clause_indx_arr(gc_in_row) := get_clause(gc_in_row, in_target_lob_rec.in_row);
      l_rlbk_clause_indx_arr(gc_in_row) := get_clause(gc_in_row, in_source_lob_rec.in_row);
    END IF;

    IF comp_value(in_source_lob_rec.tablespace_name, in_target_lob_rec.tablespace_name) = 1 THEN
      l_result := gc_result_alter_move;
      l_frwd_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_target_lob_rec.tablespace_name, '"', '"');
      l_rlbk_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_source_lob_rec.tablespace_name, '"', '"');
    END IF;

    IF l_result = gc_result_alter AND cort_exec_pkg.g_params.change.get_value = 'MOVE' THEN
      l_result := gc_result_alter_move;
    END IF;

    IF l_result = gc_result_alter_move THEN
      out_frwd_lob_parames := get_clause_by_name(l_frwd_clause_indx_arr, gc_tablespace)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_chunk)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_in_row)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_pctversion)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_freepools)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_retention)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_deduplication)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_lob_compression)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_encrypt)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_cache)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_logging)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_storage);

      out_rlbk_lob_parames := get_clause_by_name(l_rlbk_clause_indx_arr, gc_tablespace)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_chunk)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_in_row)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_pctversion)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_freepools)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_retention)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_deduplication)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_lob_compression)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_encrypt)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_cache)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_logging)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_storage);
    ELSIF l_result = gc_result_alter THEN
      out_frwd_lob_parames := get_clause_by_name(l_frwd_clause_indx_arr, gc_pctversion)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_freepools)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_retention)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_deduplication)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_lob_compression)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_encrypt)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_cache)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_logging)||
                              get_clause_by_name(l_frwd_clause_indx_arr, gc_storage);
      out_rlbk_lob_parames := get_clause_by_name(l_rlbk_clause_indx_arr, gc_pctversion)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_freepools)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_retention)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_deduplication)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_lob_compression)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_encrypt)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_cache)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_logging)||
                              get_clause_by_name(l_rlbk_clause_indx_arr, gc_storage);
    END IF;

    RETURN l_result;
  END comp_lob_params;

  -- compare lob rec
  FUNCTION comp_lob(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_source_lob_rec      IN cort_exec_pkg.gt_lob_rec,
    in_target_lob_rec      IN cort_exec_pkg.gt_lob_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN NUMBER
  AS
    l_result                 PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
    l_frwd_lob_parames       CLOB;
    l_rlbk_lob_parames       CLOB;
    l_column_rec             cort_exec_pkg.gt_column_rec;
    l_column_name            arrays.gt_name;
  BEGIN
    l_result := gc_result_nochange;

    l_column_rec := in_source_table_rec.column_arr(in_source_lob_rec.column_indx);
    l_column_name := NVL(l_column_rec.new_column_name,l_column_rec.column_name);

    l_result := comp_lob_params(
                  in_source_lob_rec    => in_source_lob_rec,
                  in_target_lob_rec    => in_target_lob_rec,
                  out_frwd_lob_parames => l_frwd_lob_parames,
                  out_rlbk_lob_parames => l_rlbk_lob_parames
                );
    IF l_result > gc_result_nochange AND
       l_frwd_lob_parames IS NOT NULL AND
       l_rlbk_lob_parames IS NOT NULL
    THEN
      l_frwd_clause_indx_arr(gc_lob_params) := get_clause(gc_lob_params, l_frwd_lob_parames, '(', ')');
      l_rlbk_clause_indx_arr(gc_lob_params) := get_clause(gc_lob_params, l_rlbk_lob_parames, '(', ')');
    END IF;

    -- move only
    IF comp_value(in_source_lob_rec.securefile, in_target_lob_rec.securefile) = 1 THEN
      l_result := gc_result_alter_move;
    END IF;

    IF l_result = gc_result_alter_move THEN

      l_frwd_clause := gc_move||
                       -- use new_column_name in case column was renamed
                       get_clause(gc_lob_item, l_column_name, '(', ')')||
                       get_clause(gc_store_as, get_clause(gc_securefile, in_target_lob_rec.securefile))||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_lob_params)||
                       get_clause(gc_parallel, cort_exec_pkg.g_params.parallel.get_value);

      l_rlbk_clause := gc_move||
                       -- use new_column_name in case column was renamed
                       get_clause(gc_lob_item, l_column_name, '(', ')')||
                       get_clause(gc_store_as, get_clause(gc_securefile, in_source_lob_rec.securefile))||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_lob_params)||
                       get_clause(gc_parallel, cort_exec_pkg.g_params.parallel.get_value);

      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    ELSIF l_result = gc_result_alter THEN
      -- use new_column_name in case column was renamed
      l_frwd_clause := get_clause(gc_modify_lob_item, l_column_name, '(', ')')||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_lob_params);
      l_rlbk_clause := get_clause(gc_modify_lob_item, l_column_name, '(', ')')||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_lob_params);

      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => in_source_table_rec.table_name,
        in_owner         => in_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    RETURN l_result;
  END comp_lob;

  -- compare lob columns (CLOB/BLOB/BFILE) - lobs.
  -- Why not to include into compare_columns ????
  FUNCTION comp_lobs(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result       PLS_INTEGER;
    l_comp_result  PLS_INTEGER;
    l_target_indx  PLS_INTEGER;
    l_source_indx  PLS_INTEGER;
    l_column_name  VARCHAR2(4000);
  BEGIN
    l_result := gc_result_nochange;
    l_column_name := in_target_table_rec.lob_indx_arr.FIRST;
    WHILE l_column_name IS NOT NULL LOOP
      IF in_source_table_rec.lob_indx_arr.EXISTS(l_column_name)
      THEN
        l_target_indx := in_target_table_rec.lob_indx_arr(l_column_name);
        l_source_indx := in_source_table_rec.lob_indx_arr(l_column_name);

        -- existing lob columns
        l_comp_result := comp_lob(
                           in_source_table_rec    => in_source_table_rec,
                           in_target_table_rec    => in_target_table_rec,
                           in_source_lob_rec      => in_source_table_rec.lob_arr(l_source_indx),
                           in_target_lob_rec      => in_target_table_rec.lob_arr(l_target_indx),
                           io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                           io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                         );
        debug('compare lob '||in_source_table_rec.lob_arr(l_source_indx).column_name||' - '||l_comp_result);
        l_result := GREATEST(l_result, l_comp_result);
      END IF;

      IF l_result = gc_result_recreate THEN
        RETURN l_result;
      END IF;

      l_column_name := in_target_table_rec.lob_indx_arr.NEXT(l_column_name);
    END LOOP;
    RETURN l_result;
  END comp_lobs;

  -- compare xml rec
  FUNCTION comp_xml(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_source_xml_rec      IN cort_exec_pkg.gt_xml_col_rec,
    in_target_xml_rec      IN cort_exec_pkg.gt_xml_col_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN NUMBER
  AS
    l_result                 PLS_INTEGER;
    l_comp_result            PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;

    IF comp_value(in_source_xml_rec.xmlschema, in_target_xml_rec.xmlschema) = 1 OR
       comp_value(in_source_xml_rec.schema_owner, in_target_xml_rec.schema_owner) = 1 OR
       comp_value(in_source_xml_rec.element_name, in_target_xml_rec.element_name) = 1 OR
       comp_value(in_source_xml_rec.storage_type, in_target_xml_rec.storage_type) = 1 OR
       comp_value(in_source_xml_rec.anyschema, in_target_xml_rec.anyschema) = 1 OR
       comp_value(in_source_xml_rec.nonschema, in_target_xml_rec.nonschema) = 1
    THEN
      RETURN gc_result_recreate;
    END IF;

    IF in_source_xml_rec.lob_column_indx IS NOT NULL AND
       in_target_xml_rec.lob_column_indx IS NOT NULL
    THEN
      l_result := comp_lob(
                    in_source_table_rec    => in_source_table_rec,
                    in_target_table_rec    => in_target_table_rec,
                    in_source_lob_rec      => in_source_table_rec.lob_arr(in_source_xml_rec.lob_column_indx),
                    in_target_lob_rec      => in_target_table_rec.lob_arr(in_target_xml_rec.lob_column_indx),
                    io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                  );
      debug('compare xml lob '||in_source_xml_rec.column_name||' - '||l_result);
    END IF;

    RETURN l_result;
  END comp_xml;

  -- compare xml columns
  FUNCTION comp_xml_columns(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result       PLS_INTEGER;
    l_comp_result  PLS_INTEGER;
    l_indx         PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;
    l_indx := in_target_table_rec.xml_col_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF in_source_table_rec.xml_col_arr.EXISTS(l_indx) THEN
        l_comp_result := comp_xml(
                           in_source_table_rec    => in_source_table_rec,
                           in_target_table_rec    => in_target_table_rec,
                           in_source_xml_rec      => in_source_table_rec.xml_col_arr(l_indx),
                           in_target_xml_rec      => in_target_table_rec.xml_col_arr(l_indx),
                           io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                           io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                         );
        debug('compare xml '||in_source_table_rec.xml_col_arr(l_indx).column_name||' - '||l_comp_result);
        l_result := GREATEST(l_result, l_comp_result);

        IF l_result = gc_result_recreate THEN
          RETURN l_result;
        END IF;
      END IF;

      l_indx := in_target_table_rec.xml_col_arr.NEXT(l_indx);
    END LOOP;

    RETURN l_result;
  END comp_xml_columns;

  -- compare varray rec
  FUNCTION comp_varray(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_source_varray_rec   IN cort_exec_pkg.gt_varray_rec,
    in_target_varray_rec   IN cort_exec_pkg.gt_varray_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;

    IF comp_value(in_source_varray_rec.type_owner, in_target_varray_rec.type_owner) = 1 OR
       comp_value(in_source_varray_rec.type_name, in_target_varray_rec.type_name) = 1 OR
       (in_source_varray_rec.lob_name IS NULL AND in_target_varray_rec.lob_name IS NOT NULL) OR
       (in_source_varray_rec.lob_name IS NOT NULL AND in_target_varray_rec.lob_name IS NULL) OR
       comp_value(in_source_varray_rec.storage_spec, in_target_varray_rec.storage_spec) = 1 OR
       comp_value(in_source_varray_rec.return_type, in_target_varray_rec.return_type) = 1 OR
       comp_value(in_source_varray_rec.element_substitutable, in_target_varray_rec.element_substitutable) = 1
    THEN
      RETURN gc_result_recreate;
    END IF;

    IF in_source_varray_rec.lob_column_indx IS NOT NULL AND
       in_target_varray_rec.lob_column_indx IS NOT NULL
    THEN
      l_result := comp_lob(
                    in_source_table_rec    => in_source_table_rec,
                    in_target_table_rec    => in_target_table_rec,
                    in_source_lob_rec      => in_source_table_rec.lob_arr(in_source_varray_rec.lob_column_indx),
                    in_target_lob_rec      => in_target_table_rec.lob_arr(in_target_varray_rec.lob_column_indx),
                    io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                  );
      debug('compare varray lob '||in_source_varray_rec.column_name||' - '||l_result);
    END IF;

    RETURN l_result;
  END comp_varray;

  -- compare varray columns
  FUNCTION comp_varray_columns(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result       PLS_INTEGER;
    l_comp_result  PLS_INTEGER;
    l_column_indx  PLS_INTEGER;
    l_indx         PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;
    l_indx := in_target_table_rec.varray_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF in_source_table_rec.varray_arr.EXISTS(l_indx) THEN
        l_comp_result := comp_varray(
                           in_source_table_rec    => in_source_table_rec,
                           in_target_table_rec    => in_target_table_rec,
                           in_source_varray_rec   => in_source_table_rec.varray_arr(l_indx),
                           in_target_varray_rec   => in_target_table_rec.varray_arr(l_indx),
                           io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                           io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                         );
        debug('compare varray '||in_source_table_rec.varray_arr(l_indx).column_name||' - '||l_comp_result);
        l_result := GREATEST(l_result, l_comp_result);

        IF l_result = gc_result_recreate THEN
          RETURN l_result;
        END IF;
      END IF;

      l_indx := in_target_table_rec.varray_arr.NEXT(l_indx);
    END LOOP;

    RETURN l_result;
  END comp_varray_columns;

  -- compare all supplemental logs
  FUNCTION comp_log_groups(
    io_source_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_log_result             PLS_INTEGER;
    l_source_hash_logs_arr   arrays.gt_int_indx;
    l_target_hash_logs_arr   arrays.gt_int_indx;
    l_source_log_group_rec   cort_exec_pkg.gt_log_group_rec;
    l_target_log_group_rec   cort_exec_pkg.gt_log_group_rec;
    l_hash_index             VARCHAR2(32767);
  BEGIN
    l_result := gc_result_nochange;
    -- prepare hash-indexed array of check source log_groups
    FOR i IN 1..io_source_table_rec.log_group_arr.COUNT LOOP
      l_hash_index := get_log_group_hash(io_source_table_rec.log_group_arr(i));
      l_source_hash_logs_arr(l_hash_index) := i;
    END LOOP;
    -- prepare hash-indexed array of check target log_groups
    FOR i IN 1..io_target_table_rec.log_group_arr.COUNT LOOP
      l_hash_index := get_log_group_hash(io_target_table_rec.log_group_arr(i));
      l_target_hash_logs_arr(l_hash_index) := i;
    END LOOP;

    -- run loop through source log_groups
    l_hash_index := l_source_hash_logs_arr.FIRST;
    WHILE l_hash_index IS NOT NULL LOOP
      -- if log_group exists on both tables
      IF NOT io_source_table_rec.log_group_arr(l_source_hash_logs_arr(l_hash_index)).drop_flag THEN
        IF l_target_hash_logs_arr.EXISTS(l_hash_index) THEN
          l_source_log_group_rec := io_source_table_rec.log_group_arr(l_source_hash_logs_arr(l_hash_index));
          l_target_log_group_rec := io_target_table_rec.log_group_arr(l_target_hash_logs_arr(l_hash_index));
          -- compare log_groups
          l_log_result := comp_log_group(
                            in_source_table_rec      => io_source_table_rec,
                            in_source_log_group_rec  => l_source_log_group_rec,
                            in_target_log_group_rec  => l_target_log_group_rec,
                            io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
                            io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
                          );
          IF l_log_result = gc_result_recreate THEN
            -- if log_group need to be recreated
            -- recreate log_group
            drop_log_group(
              in_log_group_rec       => l_source_log_group_rec,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );

            l_target_log_group_rec.table_name := io_source_table_rec.table_name;
            l_target_log_group_rec.owner := io_source_table_rec.owner;

            add_log_group(
              in_log_group_rec       => l_target_log_group_rec,
              io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
              io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
            );
            l_result := gc_result_alter;
          ELSE
            l_result := GREATEST(l_result, l_log_result);
          END IF;
        ELSE
          -- if log_group doesn't exist in NEW table
          l_source_log_group_rec := io_source_table_rec.log_group_arr(l_source_hash_logs_arr(l_hash_index));

          -- drop log_group
          drop_log_group(
            in_log_group_rec       => l_source_log_group_rec,
            io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
            io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
          );

          io_source_table_rec.log_group_arr(l_source_hash_logs_arr(l_hash_index)).drop_flag := TRUE;

          l_result := gc_result_alter;
        END IF;
      END IF;
      l_hash_index := l_source_hash_logs_arr.NEXT(l_hash_index);
    END LOOP;

    -- run loop through target log_groups
    l_hash_index := l_target_hash_logs_arr.FIRST;
    WHILE l_hash_index IS NOT NULL LOOP
      -- if log_group doesn't exist on source tables
      IF NOT l_source_hash_logs_arr.EXISTS(l_hash_index) THEN
        l_target_log_group_rec := io_target_table_rec.log_group_arr(l_target_hash_logs_arr(l_hash_index));
        l_target_log_group_rec.table_name := io_source_table_rec.table_name;
        l_target_log_group_rec.owner := io_source_table_rec.owner;
        -- add log_group
        add_log_group(
          in_log_group_rec       => l_target_log_group_rec,
          io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
        );
        l_result := gc_result_alter;
      END IF;
      l_hash_index := l_target_hash_logs_arr.NEXT(l_hash_index);
    END LOOP;

    RETURN l_result;
  END comp_log_groups;

  FUNCTION get_partition_hash(in_partition_rec IN cort_exec_pkg.gt_partition_rec)
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN in_partition_rec.high_value;
  END get_partition_hash;

  FUNCTION get_partition_storage_clause(in_partition_rec IN cort_exec_pkg.gt_partition_rec)
  RETURN VARCHAR2
  AS
    l_clause_indx_arr         arrays.gt_xlstr_indx;
  BEGIN
    -- Tablespace
    IF cort_exec_pkg.g_params.tablespace.value_exists(in_partition_rec.partition_level) THEN
      l_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_partition_rec.tablespace_name, '"', '"');
      -- Overflow Tablespace
      l_clause_indx_arr(gc_overflow_tablespace) := get_clause(gc_overflow_tablespace, in_partition_rec.overflow_tablespace, '"', '"');
    END IF;
    -- Compression
    IF in_partition_rec.iot_key_compression IS NOT NULL THEN
      l_clause_indx_arr(gc_key_compression) := get_clause(gc_key_compression, in_partition_rec.iot_key_compression);
    ELSE
      IF cort_exec_pkg.g_params.compression.value_exists(in_partition_rec.partition_level) THEN 
      l_clause_indx_arr(gc_compression) := get_compress_clause(in_partition_rec.compression_rec);
      END IF;  
    END IF;
    -- Lob partitioning storage
    -- varray
    RETURN get_clause_by_name(l_clause_indx_arr, gc_tablespace)||
           get_clause_by_name(l_clause_indx_arr, gc_overflow_tablespace)||
           get_clause_by_name(l_clause_indx_arr, gc_compression)||
           get_clause_by_name(l_clause_indx_arr, gc_key_compression);
  END get_partition_storage_clause;

  -- returns clause for partition
  FUNCTION get_partition_clause(
    in_partition_rec   IN cort_exec_pkg.gt_partition_rec,
    in_partition_level IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_clause VARCHAR2(32767);
  BEGIN
    l_clause := NVL(in_partition_level, in_partition_rec.partition_level)||' "'||in_partition_rec.partition_name||'" ';
    CASE in_partition_rec.partition_type
    WHEN 'LIST' THEN
      l_clause := l_clause ||'VALUES ('||in_partition_rec.high_value||') ';
    WHEN 'RANGE' THEN
      l_clause := l_clause ||'VALUES LESS THAN ('||in_partition_rec.high_value||') ';
    ELSE
      NULL;
    END CASE;
    RETURN l_clause;
  END get_partition_clause;

  FUNCTION get_partition_desc_clause(
    in_partition_rec    IN cort_exec_pkg.gt_partition_rec,
    in_subpartition_arr IN cort_exec_pkg.gt_partition_arr
  )
  RETURN CLOB
  AS
    l_clause_indx_arr         arrays.gt_xlstr_indx;
    l_subpartitions           CLOB;
  BEGIN
    -- segment attr:
    IF in_partition_rec.partition_type <> 'HASH' AND in_partition_rec.partition_level = 'PARTITION' THEN
      -- physical attrs
      l_clause_indx_arr(gc_physical_attr) := get_part_physical_attr_clause(in_partition_rec);
      -- Logging
      l_clause_indx_arr(gc_logging) := get_clause(gc_logging, in_partition_rec.logging);
    END IF;
    -- Tablespace
    IF cort_exec_pkg.g_params.tablespace.value_exists(in_partition_rec.partition_level) THEN
      l_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, in_partition_rec.tablespace_name, '"', '"');
    END IF;
    -- compression
    IF in_partition_rec.iot_key_compression IS NOT NULL THEN
      l_clause_indx_arr(gc_key_compression) := get_clause(gc_key_compression, in_partition_rec.iot_key_compression);
    ELSE
      IF cort_exec_pkg.g_params.compression.value_exists(in_partition_rec.partition_level) THEN 
      l_clause_indx_arr(gc_compression) := get_compress_clause(in_partition_rec.compression_rec);
      END IF;  
    END IF;

    -- Overflow
    IF in_partition_rec.overflow_tablespace IS NOT NULL AND
       in_partition_rec.partition_level = 'PARTITION'
    THEN
      l_clause_indx_arr(gc_overflow_tablespace) := get_clause(gc_overflow_tablespace, in_partition_rec.overflow_tablespace, '"', '"');
      -- Physical attrs
      l_clause_indx_arr(gc_overflow_physical_attr) := get_index_physical_attr_clause(in_partition_rec.overflow_physical_attr_rec);
      -- logging
      l_clause_indx_arr(gc_overflow_logging) := get_clause(gc_logging, in_partition_rec.overflow_logging);
    END IF;

    -- lob storage
    -- varray storage

    -- subpartitions
    IF in_partition_rec.subpartition_from_indx > 0 AND
       in_partition_rec.subpartition_to_indx > 0 AND
       in_partition_rec.partition_level = 'PARTITION' AND
       in_subpartition_arr.COUNT > 0
    THEN
      FOR i IN in_partition_rec.subpartition_from_indx..in_partition_rec.subpartition_to_indx LOOP
        l_subpartitions := l_subpartitions ||
                           get_partition_clause(in_subpartition_arr(i),'SUBPARTITION') ||
                           get_partition_storage_clause(in_subpartition_arr(i));
        IF i < in_partition_rec.subpartition_to_indx THEN
          l_subpartitions := l_subpartitions || ',';
        END IF;
      END LOOP;
      l_clause_indx_arr(gc_subpartitions) := CHR(10)||get_clause(gc_subpartitions, l_subpartitions, '(', ')');
    END IF;

    -- Overflow Tablespace
    RETURN get_clause_by_name(l_clause_indx_arr, gc_physical_attr)||
           get_clause_by_name(l_clause_indx_arr, gc_logging)||
           get_clause_by_name(l_clause_indx_arr, gc_tablespace)||
           get_clause_by_name(l_clause_indx_arr, gc_compression)||
           get_clause_by_name(l_clause_indx_arr, gc_key_compression)||
           get_clause_by_name(l_clause_indx_arr, gc_overflow_physical_attr)||
           get_clause_by_name(l_clause_indx_arr, gc_overflow_logging)||
           get_clause_by_name(l_clause_indx_arr, gc_overflow_tablespace)||
           get_clause_by_name(l_clause_indx_arr, gc_subpartitions);
  END get_partition_desc_clause;

  -- Forward declaration. compares subpartitions for two given partitions
  FUNCTION comp_subpartitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_rec IN OUT NOCOPY cort_exec_pkg.gt_partition_rec,
    io_target_partition_rec IN OUT NOCOPY cort_exec_pkg.gt_partition_rec,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER;

  -- compare two partitions
  FUNCTION comp_partition(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_rec IN OUT NOCOPY cort_exec_pkg.gt_partition_rec,
    io_target_partition_rec IN OUT NOCOPY cort_exec_pkg.gt_partition_rec,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_comp_result            PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
    l_move                   BOOLEAN := FALSE;
  BEGIN
    l_result := gc_result_nochange;

    debug('compare partititons: '||io_source_partition_rec.partition_name||' -- '||io_target_partition_rec.partition_name);

    io_source_partition_rec.comp_result := gc_result_nochange;
    io_target_partition_rec.comp_result := gc_result_nochange;

    -- Physical attrs
    IF cort_exec_pkg.g_params.physical_attr.value_exists(io_source_partition_rec.partition_level) THEN
      l_comp_result := comp_physical_attr(
                         in_source_physical_attr_rec => io_source_partition_rec.physical_attr_rec,
                         in_target_physical_attr_rec => io_target_partition_rec.physical_attr_rec,
                         out_frwd_clause             => l_frwd_clause,
                         out_rlbk_clause             => l_rlbk_clause
                       );

      io_source_partition_rec.comp_result := l_comp_result;
      io_target_partition_rec.comp_result := l_comp_result;

      CASE l_comp_result
      WHEN gc_result_recreate THEN
        debug('Compare partition physical_attr_rec - recreate');
        RETURN gc_result_part_exchange;
      WHEN gc_result_alter THEN
        debug('Compare partition physical_attr_rec - alter');
        l_result := gc_result_alter;
        l_frwd_clause_indx_arr(gc_physical_attr) := l_frwd_clause;
        l_rlbk_clause_indx_arr(gc_physical_attr) := l_rlbk_clause;
      ELSE
        NULL;
      END CASE;
    END IF;
    -- Logging
    IF comp_value(io_source_partition_rec.logging, io_target_partition_rec.logging) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_logging) := get_clause(gc_logging, io_target_partition_rec.logging);
      l_rlbk_clause_indx_arr(gc_logging) := get_clause(gc_logging, io_source_partition_rec.logging);
    END IF;

    -- Tablespace
    IF cort_exec_pkg.g_params.tablespace.value_exists(io_source_partition_rec.partition_level) AND
       comp_value(io_source_partition_rec.tablespace_name, io_target_partition_rec.tablespace_name) = 1
    THEN
      l_result := gc_result_alter;
      l_move := TRUE;
      l_frwd_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, io_target_partition_rec.tablespace_name, '"', '"');
      l_rlbk_clause_indx_arr(gc_tablespace) := get_clause(gc_tablespace, io_source_partition_rec.tablespace_name, '"', '"');
    END IF;

    -- Compression
    IF cort_exec_pkg.g_params.compression.value_exists(io_source_partition_rec.partition_level) AND 
       comp_compression(
         in_source_compression_rec  => io_source_partition_rec.compression_rec,
         in_target_compression_rec  => io_target_partition_rec.compression_rec,
         out_frwd_clause            => l_frwd_clause,
         out_rlbk_clause            => l_rlbk_clause
       ) = 1
    THEN
      l_result := gc_result_alter;
      l_move := TRUE;
      l_frwd_clause_indx_arr(gc_compression) := l_frwd_clause;
      l_rlbk_clause_indx_arr(gc_compression) := l_rlbk_clause;
    END IF;

    -- Key compression
    IF comp_value(io_source_partition_rec.iot_key_compression, io_target_partition_rec.iot_key_compression) = 1 THEN
      l_result := gc_result_alter;
      l_move := TRUE;
      l_frwd_clause_indx_arr(gc_key_compression) := get_clause(gc_key_compression, io_target_partition_rec.iot_key_compression);
      l_rlbk_clause_indx_arr(gc_key_compression) := get_clause(gc_key_compression, io_source_partition_rec.iot_key_compression);
    END IF;


    -- Overflow
    IF io_source_table_rec.overflow_table_name IS NOT NULL THEN
      l_frwd_clause_indx_arr(gc_overflow) := get_clause(gc_overflow, ' ');
      l_rlbk_clause_indx_arr(gc_overflow) := get_clause(gc_overflow, ' ');
      IF io_source_partition_rec.partition_level = 'PARTITION' THEN
        -- Physical attrs
        IF cort_exec_pkg.g_params.physical_attr.value_exists('INDEX_PARTITION') THEN
          l_comp_result := comp_index_physical_attr(
                             in_source_physical_attr_rec => io_source_partition_rec.overflow_physical_attr_rec,
                             in_target_physical_attr_rec => io_target_partition_rec.overflow_physical_attr_rec,
                             out_frwd_clause             => l_frwd_clause,
                             out_rlbk_clause             => l_rlbk_clause
                           );
          io_source_partition_rec.comp_result := l_comp_result;
          io_target_partition_rec.comp_result := l_comp_result;
          CASE l_comp_result
          WHEN gc_result_recreate THEN
            debug('Compare partition overflow physical_attr_rec - 1');
            RETURN gc_result_part_exchange;
          WHEN gc_result_alter THEN
            l_result := gc_result_alter;
            l_frwd_clause_indx_arr(gc_overflow_physical_attr) := l_frwd_clause;
            l_rlbk_clause_indx_arr(gc_overflow_physical_attr) := l_rlbk_clause;
          ELSE
            NULL;
          END CASE;
        END IF;
      END IF;

      -- Logging
      IF comp_value(io_source_partition_rec.overflow_logging, io_target_partition_rec.overflow_logging) = 1 THEN
        l_result := gc_result_alter;
        l_frwd_clause_indx_arr(gc_overflow_logging) := get_clause(gc_logging, io_target_partition_rec.overflow_logging);
        l_rlbk_clause_indx_arr(gc_overflow_logging) := get_clause(gc_logging, io_source_partition_rec.overflow_logging);
      END IF;

      -- Tablespace
      IF cort_exec_pkg.g_params.tablespace.value_exists(io_source_partition_rec.partition_level) AND
         comp_value(io_source_partition_rec.overflow_tablespace, io_target_partition_rec.overflow_tablespace) = 1
      THEN
        l_result := gc_result_alter;
        l_move := TRUE;
        l_frwd_clause_indx_arr(gc_overflow_tablespace) := get_clause(gc_tablespace, io_target_partition_rec.overflow_tablespace, '"', '"');
        l_rlbk_clause_indx_arr(gc_overflow_tablespace) := get_clause(gc_tablespace, io_source_partition_rec.overflow_tablespace, '"', '"');
      END IF;
    END IF;

    IF l_result = gc_result_alter THEN
      IF l_move THEN
        l_frwd_clause_indx_arr(gc_move_partition) := get_clause(gc_move||io_source_partition_rec.partition_level, io_source_partition_rec.partition_name, '"', '"');
        l_rlbk_clause_indx_arr(gc_move_partition) := get_clause(gc_move||io_source_partition_rec.partition_level, io_source_partition_rec.partition_name, '"', '"');
        l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_move_partition)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_physical_attr)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_logging)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_tablespace)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_compression)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_key_compression)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_physical_attr)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_logging)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_tablespace);
        l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_move_partition)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_physical_attr)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_logging)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_tablespace)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_compression)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_key_compression)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_physical_attr)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_logging)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_tablespace);
      ELSE
        l_frwd_clause_indx_arr(gc_modify_partition) := get_clause(gc_modify||io_source_partition_rec.partition_level, io_source_partition_rec.partition_name, '"', '"');
        l_rlbk_clause_indx_arr(gc_modify_partition) := get_clause(gc_modify||io_source_partition_rec.partition_level, io_source_partition_rec.partition_name, '"', '"');
        l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_modify_partition)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_physical_attr)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_logging)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_compression)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_physical_attr)||
                         get_clause_by_name(l_frwd_clause_indx_arr, gc_overflow_logging);
        l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_modify_partition)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_physical_attr)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_logging)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_compression)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_physical_attr)||
                         get_clause_by_name(l_rlbk_clause_indx_arr, gc_overflow_logging);
      END IF;
      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => io_source_table_rec.table_name,
        in_owner         => io_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    debug('partition comp = '||l_result);

    IF io_source_partition_rec.partition_level = 'PARTITION' AND
       io_source_table_rec.subpartitioning_type <> 'NONE' AND
       io_target_table_rec.subpartitioning_type <> 'NONE'
    THEN
      l_comp_result := comp_subpartitions(
                         io_source_table_rec     => io_source_table_rec,
                         io_target_table_rec     => io_target_table_rec,
                         io_source_partition_rec => io_source_partition_rec,
                         io_target_partition_rec => io_target_partition_rec,
                         io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                       );
      io_source_partition_rec.subpart_comp_result := l_comp_result;
      io_target_partition_rec.subpart_comp_result := l_comp_result;

      IF l_comp_result = gc_result_recreate THEN
        l_comp_result := gc_result_part_exchange;
      END IF;

      l_result := greatest(l_result,l_comp_result);
    END IF;


    -- if partitions have different names in source and target tables
    IF comp_value(io_source_partition_rec.partition_name, io_target_partition_rec.partition_name) = 1 AND
       io_target_partition_rec.partition_name NOT LIKE 'SYS\_%' ESCAPE '\' THEN
      -- rename partition
      l_frwd_clause := get_clause(gc_rename||io_source_partition_rec.partition_level, '"'||io_source_partition_rec.partition_name||'" TO "'||io_target_partition_rec.partition_name||'"');
      l_rlbk_clause := get_clause(gc_rename||io_source_partition_rec.partition_level, '"'||io_target_partition_rec.partition_name||'" TO "'||io_source_partition_rec.partition_name||'"');
      add_alter_table_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_table_name    => io_source_table_rec.table_name,
        in_owner         => io_source_table_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
      l_result := gc_result_alter;
    END IF;

    RETURN l_result;
  END comp_partition;

  -- compare hash partitions
  FUNCTION comp_hash_partitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_part_result            PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    l_result := gc_result_nochange;
    FOR i IN 1..LEAST(io_target_partition_arr.COUNT,io_source_partition_arr.COUNT) LOOP
      l_part_result := comp_partition(
                          io_source_table_rec     => io_source_table_rec,
                          io_target_table_rec     => io_target_table_rec,
                          io_source_partition_rec => io_source_partition_arr(i),
                          io_target_partition_rec => io_target_partition_arr(i),
                          io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                          io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                        );
      io_target_partition_arr(i).matching_indx := io_source_partition_arr(i).indx;
      io_source_partition_arr(i).matching_indx := io_target_partition_arr(i).indx;
      l_result := GREATEST(l_result, l_part_result);
    END LOOP;
    IF l_result = gc_result_part_exchange THEN
      RETURN gc_result_part_exchange;
    END IF;
    IF io_source_partition_arr.COUNT > io_target_partition_arr.COUNT THEN
      FOR i IN io_target_partition_arr.COUNT+1..io_source_partition_arr.COUNT LOOP
        l_result := gc_result_alter;
        CASE io_source_partition_arr(i).partition_level
        WHEN 'PARTITION' THEN
          l_frwd_clause := get_clause(gc_coalesce_partition, ' ');
          l_rlbk_clause := get_clause(gc_add_partition, io_source_partition_arr(i).partition_name, '"', '"')||
                           get_partition_storage_clause(io_source_partition_arr(i));
        WHEN 'SUBPARTITION' THEN
          l_frwd_clause := get_clause(gc_modify_partition, io_source_partition_arr(i).parent_partition_name, '"', '"')||
                           get_clause(gc_coalesce_subpartition, ' ' );
          l_rlbk_clause := get_clause(gc_modify_partition, io_source_partition_arr(i).parent_partition_name, '"', '"')||
                           get_clause(gc_add_subpartition, io_source_partition_arr(i).partition_name, '"', '"')||
                           get_partition_storage_clause(io_source_partition_arr(i));
        END CASE;
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => io_source_table_rec.table_name,
          in_owner         => io_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END LOOP;
    ELSIF io_source_partition_arr.COUNT < io_target_partition_arr.COUNT THEN
      FOR i IN io_source_partition_arr.COUNT+1..io_target_partition_arr.COUNT LOOP
        l_result := gc_result_alter;
        CASE io_target_partition_arr(i).partition_level
        WHEN 'PARTITION' THEN
          l_frwd_clause := get_clause(gc_add_partition, io_target_partition_arr(i).partition_name, '"', '"')||
                           get_partition_storage_clause(io_target_partition_arr(i));
          l_rlbk_clause := get_clause(gc_coalesce_partition, ' ');
        WHEN 'SUBPARTITION' THEN
          l_frwd_clause := get_clause(gc_modify_partition, io_target_partition_arr(i).parent_partition_name, '"', '"')||
                           get_clause(gc_add_subpartition, io_target_partition_arr(i).partition_name, '"', '"')||
                           get_partition_storage_clause(io_target_partition_arr(i));
          l_rlbk_clause := get_clause(gc_modify_partition, io_target_partition_arr(i).parent_partition_name, '"', '"')||
                           get_clause(gc_coalesce_subpartition, ' ');
        END CASE;
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => io_source_table_rec.table_name,
          in_owner         => io_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END LOOP;
    END IF;
    RETURN l_result;
  END comp_hash_partitions;

  -- compare system partitions
  FUNCTION comp_system_partitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_part_result            PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    l_result := gc_result_nochange;
    FOR i IN 1..LEAST(io_target_partition_arr.COUNT,io_source_partition_arr.COUNT) LOOP
      l_part_result := comp_partition(
                          io_source_table_rec     => io_source_table_rec,
                          io_target_table_rec     => io_target_table_rec,
                          io_source_partition_rec => io_source_partition_arr(i),
                          io_target_partition_rec => io_target_partition_arr(i),
                          io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                          io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                        );
      io_target_partition_arr(i).matching_indx := i;
      io_source_partition_arr(i).matching_indx := i;
      l_result := GREATEST(l_result, l_part_result);
    END LOOP;
    IF l_result = gc_result_part_exchange THEN
      RETURN gc_result_part_exchange;
    END IF;

    IF io_source_partition_arr.COUNT > io_target_partition_arr.COUNT THEN
      FOR i IN io_target_partition_arr.COUNT+1..io_source_partition_arr.COUNT LOOP
        l_result := gc_result_alter;
        l_frwd_clause := get_clause(gc_drop_partition, io_source_partition_arr(i).partition_name, '"', '"');
        l_rlbk_clause := get_clause(gc_add_partition, io_source_partition_arr(i).partition_name, '"', '"')||
                         get_partition_desc_clause(io_source_partition_arr(i), io_source_table_rec.subpartition_arr);
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => io_source_table_rec.table_name,
          in_owner         => io_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END LOOP;
    ELSIF io_source_partition_arr.COUNT < io_target_partition_arr.COUNT THEN
      FOR i IN io_source_partition_arr.COUNT+1..io_target_partition_arr.COUNT LOOP
        l_result := gc_result_alter;
        l_frwd_clause := get_clause(gc_add_partition, io_target_partition_arr(i).partition_name, '"', '"')||
                         get_partition_desc_clause(io_target_partition_arr(i), io_target_table_rec.subpartition_arr);
        l_rlbk_clause := get_clause(gc_drop_partition, io_target_partition_arr(i).partition_name, '"', '"');
        add_alter_table_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_table_name    => io_source_table_rec.table_name,
          in_owner         => io_source_table_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
      END LOOP;
    END IF;
    RETURN l_result;
  END comp_system_partitions;

  -- compare reference partitions
  FUNCTION comp_reference_partitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_part_result            PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;
    IF io_target_partition_arr.COUNT <> io_source_partition_arr.COUNT THEN
      RETURN gc_result_recreate;
    END IF;

    FOR i IN 1..io_source_partition_arr.COUNT LOOP
      l_part_result := comp_partition(
                         io_source_table_rec     => io_source_table_rec,
                         io_target_table_rec     => io_target_table_rec,
                         io_source_partition_rec => io_source_partition_arr(i),
                         io_target_partition_rec => io_target_partition_arr(i),
                         io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                       );
      io_target_partition_arr(i).matching_indx := i;
      io_source_partition_arr(i).matching_indx := i;
      l_result := GREATEST(l_result, l_part_result);
    END LOOP;
    RETURN l_result;
  END comp_reference_partitions;

  -- compare list partitions
  FUNCTION comp_list_partitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_part_result            PLS_INTEGER;
    l_source_partition_arr   partition_utils.gt_partition_arr;
    l_target_partition_arr   partition_utils.gt_partition_arr;
    l_source_high_value_str  VARCHAR2(32767);
    l_target_high_value_str  VARCHAR2(32767);
    l_source_xref_arr        arrays.gt_int_arr;
    l_source_partition_indx  arrays.gt_int_indx;
    l_add_partition_arr      arrays.gt_int_arr;
    l_indx                   PLS_INTEGER;
    l_match_indx             PLS_INTEGER;
    l_def_part_exists        BOOLEAN;
    l_before_partition_name  arrays.gt_name;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    l_result := gc_result_nochange;

    FOR i IN 1..io_source_partition_arr.COUNT LOOP
      l_source_partition_arr(i).object_type := 'TABLE';
      l_source_partition_arr(i).object_name := io_source_partition_arr(i).table_name;
      l_source_partition_arr(i).object_owner := io_source_partition_arr(i).table_owner;
      l_source_partition_arr(i).partition_name := io_source_partition_arr(i).partition_name;
      l_source_partition_arr(i).parent_part_name := io_source_partition_arr(i).parent_partition_name;
      l_source_partition_arr(i).position := io_source_partition_arr(i).position;
      partition_utils.parse_high_value_str(io_source_partition_arr(i).high_value, l_source_partition_arr(i).high_values);
      l_source_high_value_str := partition_utils.convert_to_string(l_source_partition_arr(i).high_values);
      l_source_partition_indx(l_source_high_value_str) := i;
    END LOOP;

    l_def_part_exists := FALSE;
    l_match_indx := null;
    FOR i IN 1..io_target_partition_arr.COUNT LOOP
      l_target_partition_arr(i).object_type := 'TABLE';
      l_target_partition_arr(i).object_name := io_target_partition_arr(i).table_name;
      l_target_partition_arr(i).object_owner := io_target_partition_arr(i).table_owner;
      l_target_partition_arr(i).partition_name := io_target_partition_arr(i).partition_name;
      l_target_partition_arr(i).parent_part_name := io_target_partition_arr(i).parent_partition_name;
      l_target_partition_arr(i).position := io_target_partition_arr(i).position;
      partition_utils.parse_high_value_str(io_target_partition_arr(i).high_value, l_target_partition_arr(i).high_values);
      l_target_high_value_str := partition_utils.convert_to_string(l_target_partition_arr(i).high_values);
      IF l_target_partition_arr(i).high_values(1).typecode = partition_utils.typecode_default THEN
        l_def_part_exists := TRUE;
      END IF;

      IF l_source_partition_indx.EXISTS(l_target_high_value_str) THEN
        l_indx := l_source_partition_indx(l_target_high_value_str);
        l_source_xref_arr(i) := l_indx;

        debug('Match target partition '||to_char(i)||')'||io_target_partition_arr(i).partition_name||' to source partition '||to_char(l_indx)||')'||io_source_partition_arr(l_indx).partition_name);

        IF l_target_partition_arr(i).high_values(1).typecode = partition_utils.typecode_default AND l_add_partition_arr.COUNT > 0 AND NOT io_source_partition_arr(l_indx).is_partition_empty THEN
          l_part_result := cort_comp_pkg.gc_result_part_exchange;
        ELSE
          l_part_result := comp_partition(
                             io_source_table_rec     => io_source_table_rec,
                             io_target_table_rec     => io_target_table_rec,
                             io_source_partition_rec => io_source_partition_arr(l_indx),
                             io_target_partition_rec => io_target_partition_arr(i),
                             io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                             io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                           );
          io_target_partition_arr(i).matching_indx := io_source_partition_arr(l_indx).indx;
          io_source_partition_arr(l_indx).matching_indx := io_target_partition_arr(i).indx;
        END IF;
        l_result := greatest(l_result, l_part_result);

        -- partiton moved to another position
        IF l_indx < l_match_indx AND NOT cort_exec_pkg.g_params.compare.value_exists('IGNORE_ORDER') THEN
          debug('partition '||io_target_partition_arr(i).partition_name||' exists but moved to another position. Need to recreate');
          l_result := cort_comp_pkg.gc_result_part_exchange;
        END IF;
        l_match_indx := l_indx;
      ELSE
        l_add_partition_arr(i) := i;
      END IF;
    END LOOP;

    FOR i IN REVERSE 1..io_source_partition_arr.COUNT LOOP
      IF io_source_partition_arr(i).matching_indx IS NULL THEN
        IF NOT l_def_part_exists THEN
          debug('Some target partitions are removed. Need to recreate. Some data will be lost');
          l_result := gc_result_part_exchange;
          io_source_partition_arr(i).matching_indx := -1;
        ELSIF io_source_partition_arr(i).is_partition_empty THEN
          l_result := greatest(gc_result_alter, l_result);
          io_source_partition_arr(i).matching_indx := -1;
/*          IF l_source_xref_arr.NEXT(i) IS NOT NULL THEN
            l_before_partition_name := io_source_partition_arr(l_source_xref_arr.NEXT(i)).partition_name;
          ELSE
            l_before_partition_name := NULL;
          END IF;
          debug('Before partition name '||l_before_partition_name);*/
          debug('Drop partition '||l_source_partition_arr(i).partition_name);
          --
          CASE io_source_partition_arr(i).partition_level
          WHEN 'PARTITION' THEN
            l_frwd_clause := get_clause(gc_drop_partition, io_source_partition_arr(i).partition_name, '"', '"');
            l_rlbk_clause := null;
          WHEN 'SUBPARTITION' THEN
            l_frwd_clause := get_clause(gc_drop_subpartition, io_source_partition_arr(i).partition_name, '"', '"');
            l_rlbk_clause := null;
          END CASE;
          add_alter_table_stmt(
            io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
            io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
            in_table_name    => io_source_table_rec.table_name,
            in_owner         => io_source_table_rec.owner,
            in_frwd_clause   => l_frwd_clause,
            in_rlbk_clause   => l_rlbk_clause
          );
        ELSE
          debug('merge partition '||l_source_partition_arr(i).partition_name);
          l_result := gc_result_part_exchange;
          io_source_partition_arr(i).matching_indx := NULL;
        END IF;
      END IF;
    END LOOP;

    IF l_result = cort_comp_pkg.gc_result_part_exchange THEN
      debug('Some target partitions are removed. Need to recreate');
      RETURN gc_result_part_exchange;
    END IF;

    l_match_indx := NULL;
    l_indx := l_add_partition_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      l_result := greatest(gc_result_alter, l_result);

      -- Find next matched partition
      l_match_indx := l_source_xref_arr.NEXT(l_indx);

      IF l_match_indx IS NOT NULL THEN
        l_match_indx := l_source_xref_arr(l_match_indx);

        l_before_partition_name := l_source_partition_arr(l_match_indx).partition_name;
        debug('Inserting partition '||l_target_partition_arr(l_indx).partition_name||' before partition '||l_before_partition_name);
      ELSE
        l_before_partition_name := NULL;
        debug('Adding partition '||l_target_partition_arr(l_indx).partition_name||' at the end');
      END IF;

      CASE io_target_partition_arr(l_indx).partition_level
      WHEN 'PARTITION' THEN
        partition_utils.insert_list_partition_ddl(
          in_partition_arr    => l_source_partition_arr,
          in_high_values      => l_target_partition_arr(l_indx).high_values,
          in_partition_name   => l_target_partition_arr(l_indx).partition_name,
          in_partition_desc   => get_partition_desc_clause(io_target_partition_arr(l_indx), io_target_table_rec.subpartition_arr),
          in_before_partition => l_before_partition_name,
          io_forward_ddl      => io_frwd_alter_stmt_arr,
          io_rollback_ddl     => io_rlbk_alter_stmt_arr
        );
      WHEN 'SUBPARTITION' THEN
        partition_utils.insert_list_subpartition_ddl(
          in_partition_arr     => l_source_partition_arr,
          in_high_values       => l_target_partition_arr(l_indx).high_values,
          in_partition_name    => io_target_partition_arr(l_indx).parent_partition_name,
          in_subpartition_name => l_target_partition_arr(l_indx).partition_name,
          in_subpartition_desc => get_partition_desc_clause(io_target_partition_arr(l_indx), empty_partition_arr),
          in_before_partition  => l_before_partition_name,
          io_forward_ddl       => io_frwd_alter_stmt_arr,
          io_rollback_ddl      => io_rlbk_alter_stmt_arr
        );
      END CASE;

      l_indx := l_add_partition_arr.NEXT(l_indx);
    END LOOP;

    RETURN l_result;
  END comp_list_partitions;

  -- compare range partitions
  FUNCTION comp_range_partitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_comp_result            PLS_INTEGER;
    l_part_result            PLS_INTEGER;
    l_source_partition_arr   partition_utils.gt_partition_arr;
    l_target_partition_arr   partition_utils.gt_partition_arr;
    l_src_indx               PLS_INTEGER;
    l_last_src_indx          PLS_INTEGER;
    l_split_src_indx         PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
  BEGIN
    l_result := gc_result_nochange;
    FOR i IN 1..io_target_partition_arr.COUNT LOOP
      l_target_partition_arr(i).object_type := 'TABLE';
      l_target_partition_arr(i).object_name := io_target_partition_arr(i).table_name;
      l_target_partition_arr(i).object_owner := io_target_partition_arr(i).table_owner;
      l_target_partition_arr(i).partition_name := io_target_partition_arr(i).partition_name;
      l_target_partition_arr(i).parent_part_name := io_target_partition_arr(i).parent_partition_name;
      l_target_partition_arr(i).position := io_target_partition_arr(i).position;
      partition_utils.parse_high_value_str(io_target_partition_arr(i).high_value, l_target_partition_arr(i).high_values);
    END LOOP;

    FOR i IN 1..io_source_partition_arr.COUNT LOOP
      l_source_partition_arr(i).object_type := 'TABLE';
      l_source_partition_arr(i).object_name := io_source_partition_arr(i).table_name;
      l_source_partition_arr(i).object_owner := io_source_partition_arr(i).table_owner;
      l_source_partition_arr(i).partition_name := io_source_partition_arr(i).partition_name;
      l_source_partition_arr(i).parent_part_name := io_source_partition_arr(i).parent_partition_name;
      l_source_partition_arr(i).position := io_source_partition_arr(i).position;
      partition_utils.parse_high_value_str(io_source_partition_arr(i).high_value, l_source_partition_arr(i).high_values);
    END LOOP;


    IF l_target_partition_arr.COUNT < l_source_partition_arr.COUNT THEN
      debug('Less partitions in new table. Need to recreate');
      RETURN gc_result_recreate;
    END IF;

    l_last_src_indx := 0;
    l_src_indx := 1;
    FOR i IN 1..io_target_partition_arr.COUNT LOOP

      IF l_src_indx <= l_source_partition_arr.COUNT THEN
        WHILE l_src_indx <= l_source_partition_arr.COUNT LOOP

          debug('Compare source partition '||to_char(l_src_indx)||') '||l_source_partition_arr(l_src_indx).partition_name||' vs target partition '||to_char(i)||') '||l_target_partition_arr(i).partition_name);

          l_part_result := partition_utils.compare_high_values(
                             in_values1        => l_source_partition_arr(l_src_indx).high_values,
                             in_values2        => l_target_partition_arr(i).high_values,
                             in_partition_type => 'RANGE',
                             in_compare_type   => 'BY HIGH_VALUE'
                           );
          IF l_part_result = partition_utils.compare_equal THEN
            l_comp_result := comp_partition(
                               io_source_table_rec     => io_source_table_rec,
                               io_target_table_rec     => io_target_table_rec,
                               io_source_partition_rec => io_source_partition_arr(l_src_indx),
                               io_target_partition_rec => io_target_partition_arr(i),
                               io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                               io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                             );
            IF l_split_src_indx IS NULL THEN
              io_target_partition_arr(i).matching_indx := io_source_partition_arr(l_src_indx).indx;
              io_source_partition_arr(l_src_indx).matching_indx := io_target_partition_arr(i).indx;
              debug('Source partition '||io_source_partition_arr(l_src_indx).partition_name||' matched to '||io_target_partition_arr(i).partition_name);
            END IF;
            l_split_src_indx := NULL;
            l_result := greatest(l_comp_result, l_result);
            l_last_src_indx := l_src_indx;
            l_src_indx := l_src_indx + 1;
            EXIT;
          ELSIF l_part_result = partition_utils.compare_greater THEN
            -- add new target partition (insert on given position)
            debug('insert/split partition '||l_target_partition_arr(i).partition_name);
            partition_utils.insert_range_partition_ddl(
              in_partition_arr    => l_source_partition_arr,
              in_high_values      => l_target_partition_arr(i).high_values,
              in_partition_name   => l_target_partition_arr(i).partition_name,
              in_partition_desc   => get_partition_desc_clause(io_target_partition_arr(i), empty_partition_arr),
              io_forward_ddl      => io_frwd_alter_stmt_arr,
              io_rollback_ddl     => io_rlbk_alter_stmt_arr
            );
            -- Mark next source partition for recreate (split)
            l_split_src_indx := l_src_indx;
            l_last_src_indx := l_src_indx;
            l_result := greatest(gc_result_alter, l_result);
            EXIT;
          ELSE
            IF l_src_indx = l_source_partition_arr.COUNT THEN
              debug('Last source partition are removed. Need to recreate. Some data will be lost');
              l_result := gc_result_part_exchange;
              io_source_partition_arr(i).matching_indx := -1;
            ELSIF io_source_partition_arr(l_src_indx).is_partition_empty THEN
              l_result := greatest(gc_result_alter, l_result);
              io_source_partition_arr(i).matching_indx := -1;
              debug('Drop partition '||l_source_partition_arr(i).partition_name);
              --
              CASE io_source_partition_arr(i).partition_level
              WHEN 'PARTITION' THEN
                l_frwd_clause := get_clause(gc_drop_partition, io_source_partition_arr(i).partition_name, '"', '"');
                l_rlbk_clause := null;
              WHEN 'SUBPARTITION' THEN
                l_frwd_clause := get_clause(gc_drop_subpartition, io_source_partition_arr(i).partition_name, '"', '"');
                l_rlbk_clause := null;
              END CASE;
              add_alter_table_stmt(
                io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
                io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
                in_table_name    => io_source_table_rec.table_name,
                in_owner         => io_source_table_rec.owner,
                in_frwd_clause   => l_frwd_clause,
                in_rlbk_clause   => l_rlbk_clause
              );
            ELSE
              debug('merge partition '||io_source_partition_arr(l_src_indx).partition_name);
              l_result := gc_result_part_exchange;
              io_source_partition_arr(i).matching_indx := NULL;
            END IF;
            l_last_src_indx := l_src_indx;
            -- go to next source partition
            l_src_indx := l_src_indx + 1;
          END IF;
        END LOOP;
      ELSE
        l_result := greatest(gc_result_alter, l_result);
        -- need to add to the end
        debug('add partition '||l_target_partition_arr(i).partition_name);
        partition_utils.insert_range_partition_ddl(
          in_partition_arr    => l_source_partition_arr,
          in_high_values      => l_target_partition_arr(i).high_values,
          in_partition_name   => l_target_partition_arr(i).partition_name,
          in_partition_desc   => get_partition_desc_clause(io_target_partition_arr(i), empty_partition_arr),
          io_forward_ddl      => io_frwd_alter_stmt_arr,
          io_rollback_ddl     => io_rlbk_alter_stmt_arr
        );
      END IF;

    END LOOP;

    RETURN l_result;
  END comp_range_partitions;

  -- internal function for campare individual partition/subpartition segments 
  FUNCTION int_comp_partition_segments(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr,
    in_partitioning_type    IN VARCHAR2, -- HASH/LIST/RANGE/SYSTEM/REFERENCE
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_partitioning_type      VARCHAR2(30);
    l_frwd_alter_stmt_arr    arrays.gt_clob_arr; -- forward alter statements
    l_rlbk_alter_stmt_arr    arrays.gt_clob_arr;  -- rollback alter statements
  BEGIN
    CASE in_partitioning_type
    WHEN 'HASH' THEN
      l_result := comp_hash_partitions(
                    io_source_table_rec     => io_source_table_rec,
                    io_target_table_rec     => io_target_table_rec,
                    io_source_partition_arr => io_source_partition_arr,
                    io_target_partition_arr => io_target_partition_arr,
                    io_frwd_alter_stmt_arr  => l_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr  => l_rlbk_alter_stmt_arr
                  );
    WHEN 'SYSTEM' THEN
      l_result := comp_system_partitions(
                    io_source_table_rec     => io_source_table_rec,
                    io_target_table_rec     => io_target_table_rec,
                    io_source_partition_arr => io_source_partition_arr,
                    io_target_partition_arr => io_target_partition_arr,
                    io_frwd_alter_stmt_arr  => l_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr  => l_rlbk_alter_stmt_arr
                  );
    WHEN 'REFERENCE' THEN
      l_result := comp_reference_partitions(
                    io_source_table_rec     => io_source_table_rec,
                    io_target_table_rec     => io_target_table_rec,
                    io_source_partition_arr => io_source_partition_arr,
                    io_target_partition_arr => io_target_partition_arr,
                    io_frwd_alter_stmt_arr  => l_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr  => l_rlbk_alter_stmt_arr
                  );
    WHEN 'LIST' THEN
      l_result := comp_list_partitions(
                    io_source_table_rec     => io_source_table_rec,
                    io_target_table_rec     => io_target_table_rec,
                    io_source_partition_arr => io_source_partition_arr,
                    io_target_partition_arr => io_target_partition_arr,
                    io_frwd_alter_stmt_arr  => l_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr  => l_rlbk_alter_stmt_arr
                  );
    WHEN 'RANGE' THEN
      l_result := comp_range_partitions(
                    io_source_table_rec     => io_source_table_rec,
                    io_target_table_rec     => io_target_table_rec,
                    io_source_partition_arr => io_source_partition_arr,
                    io_target_partition_arr => io_target_partition_arr,
                    io_frwd_alter_stmt_arr  => l_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr  => l_rlbk_alter_stmt_arr
                  );
    ELSE
      NULL;
    END CASE;

    IF l_result in (gc_result_alter, gc_result_alter_move) THEN
      FOR i IN 1..l_frwd_alter_stmt_arr.COUNT LOOP
        io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := l_frwd_alter_stmt_arr(i);
      END LOOP;
      FOR i IN 1..l_rlbk_alter_stmt_arr.COUNT LOOP
        io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := l_rlbk_alter_stmt_arr(i);
      END LOOP;
    END IF;

    RETURN l_result;
  END int_comp_partition_segments;

  -- compare partitions
  FUNCTION comp_partitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr, 
    io_target_partition_arr IN OUT NOCOPY cort_exec_pkg.gt_partition_arr, 
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
  BEGIN
    cort_exec_pkg.start_timer;    

    l_result := int_comp_partition_segments(
                  io_source_table_rec     => io_source_table_rec,
                  io_target_table_rec     => io_target_table_rec,
                  io_source_partition_arr => io_source_partition_arr, 
                  io_target_partition_arr => io_target_partition_arr, 
                  in_partitioning_type    => io_source_table_rec.partitioning_type,
                  io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr, 
                  io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                );
    cort_exec_pkg.stop_timer;

    RETURN l_result;
  END comp_partitions;

  -- compares subpartitions for two given partitions
  FUNCTION comp_subpartitions(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_source_partition_rec IN OUT NOCOPY cort_exec_pkg.gt_partition_rec,
    io_target_partition_rec IN OUT NOCOPY cort_exec_pkg.gt_partition_rec,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                  PLS_INTEGER;
    l_source_subpartition_arr cort_exec_pkg.gt_partition_arr;
    l_target_subpartition_arr cort_exec_pkg.gt_partition_arr;
    l_index                   PLS_INTEGER;
  BEGIN
    l_result := gc_result_nochange;
    IF io_source_table_rec.subpartitioning_type = 'NONE' OR
       io_target_table_rec.subpartitioning_type = 'NONE'
    THEN
      RETURN gc_result_nochange;
    END IF;
    l_index := 1;
    FOR i IN io_source_partition_rec.subpartition_from_indx..io_source_partition_rec.subpartition_to_indx LOOP
      l_source_subpartition_arr(l_index) := io_source_table_rec.subpartition_arr(i);
      debug('source subpart name = '||l_source_subpartition_arr(l_index).partition_name);
      l_index := l_index + 1;
    END LOOP;
    l_index := 1;
    FOR i IN io_target_partition_rec.subpartition_from_indx..io_target_partition_rec.subpartition_to_indx LOOP
      l_target_subpartition_arr(l_index) := io_target_table_rec.subpartition_arr(i);
      debug('target subpart name = '||l_target_subpartition_arr(l_index).partition_name);
      l_index := l_index + 1;
    END LOOP;
    l_result := int_comp_partition_segments(
                  io_source_table_rec     => io_source_table_rec,
                  io_target_table_rec     => io_target_table_rec,
                  io_source_partition_arr => l_source_subpartition_arr,
                  io_target_partition_arr => l_target_subpartition_arr,
                  in_partitioning_type    => io_source_table_rec.subpartitioning_type,
                  io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                  io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                );

    l_index := 1;
    FOR i IN io_source_partition_rec.subpartition_from_indx..io_source_partition_rec.subpartition_to_indx LOOP
      io_source_table_rec.subpartition_arr(i) := l_source_subpartition_arr(l_index);
      l_index := l_index + 1;
    END LOOP;
    l_index := 1;
    FOR i IN io_target_partition_rec.subpartition_from_indx..io_target_partition_rec.subpartition_to_indx LOOP
      io_target_table_rec.subpartition_arr(i) := l_target_subpartition_arr(l_index);
      l_index := l_index + 1;
    END LOOP;

    RETURN l_result;
  END comp_subpartitions;

  -- generic function return new name based on rename mode
  FUNCTION get_new_name(
    in_rename_rec  IN cort_exec_pkg.gt_rename_rec,
    in_rename_mode IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_new_name arrays.gt_name;
  BEGIN
    CASE in_rename_mode
    WHEN 'TO_TEMP' THEN
      l_new_name := in_rename_rec.temp_name;
    WHEN 'TO_CORT' THEN
      l_new_name := in_rename_rec.cort_name;
    WHEN 'TO_ORIGINAL' THEN
      l_new_name := in_rename_rec.object_name;
    WHEN 'TO_RENAME' THEN
      l_new_name := in_rename_rec.rename_name;
    END CASE;
   RETURN l_new_name;
  END get_new_name;

  -- create swap table for given table (and partition - optionaly)
  PROCEDURE create_swap_table_sql(
    in_table_rec        IN cort_exec_pkg.gt_table_rec,
    in_swap_table_rec   IN cort_exec_pkg.gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_create_sql     CLOB;
    l_drop_sql       CLOB;
    l_partition_sql  CLOB;
    l_indx           PLS_INTEGER;
    l_columns_list   VARCHAR2(32767);
    l_iot_clause     VARCHAR2(32767);
--    l_part_rec       cort_exec_pkg.gt_partition_rec;
--    l_part_arr       cort_exec_pkg.gt_partition_arr;
--    l_subpart_arr    cort_exec_pkg.gt_partition_arr;
    l_index_rec      cort_exec_pkg.gt_index_rec;
    l_constraint_rec cort_exec_pkg.gt_constraint_rec;
    l_col_prop       VARCHAR2(32767);
    l_col_properties VARCHAR2(32767);
  BEGIN
    FOR i IN 1..in_table_rec.column_arr.COUNT LOOP
      IF in_table_rec.column_arr(i).virtual_column = 'NO' AND
         in_table_rec.column_arr(i).hidden_column = 'NO'
      THEN
        l_columns_list := l_columns_list||CHR(10)||'  '||RTRIM(get_column_clause(in_table_rec, in_table_rec.column_arr(i)))||',';
        l_col_prop := get_column_properties(
                        in_table_rec  => in_table_rec,
                        in_column_rec => in_table_rec.column_arr(i)
                      );
        IF l_col_prop IS NOT NULL THEN
          l_col_properties := l_col_properties||CHR(10)||l_col_prop;
        END IF;
      END IF;
    END LOOP;

    FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
      l_constraint_rec := in_table_rec.constraint_arr(i);
      l_constraint_rec.table_name := in_swap_table_rec.table_name;
      l_constraint_rec.constraint_name := in_swap_table_rec.table_name||'$C'||TO_CHAR(i,'fm0XXX');
      l_constraint_rec.rename_rec.current_name := l_constraint_rec.constraint_name;

      IF in_table_rec.constraint_arr(i).constraint_type IN ('P','U') THEN
        l_indx := in_table_rec.index_indx_arr('"'||l_constraint_rec.index_owner||'"."'||l_constraint_rec.index_name||'"');
        l_index_rec := in_table_rec.index_arr(l_indx);
        l_index_rec.table_name := in_swap_table_rec.table_name;
        l_index_rec.index_name := in_swap_table_rec.table_name||'$I'||TO_CHAR(i,'fm0XXX');
        l_index_rec.rename_rec.current_name := l_index_rec.index_name;

        l_columns_list := l_columns_list||CHR(10)||REGEXP_REPLACE(get_add_constraint_clause(l_constraint_rec, l_index_rec), 'ADD CONSTRAINT', 'CONSTRAINT', 1, 1)||',';
      END IF;
    END LOOP;
    l_columns_list := TRIM(',' FROM l_columns_list);

    IF in_table_rec.iot_name IS NOT NULL
    THEN
      l_iot_clause := 'ORGANIZATION INDEX '||
                      get_clause(gc_pct_threshold, in_table_rec.iot_pct_threshold)||
                      get_clause(gc_key_compression, in_table_rec.iot_key_compression, NULL, ' '||in_table_rec.iot_prefix_length)||
                      get_clause(gc_mapping, in_table_rec.mapping_table);
      IF in_table_rec.iot_include_column IS NOT NULL OR
         in_table_rec.overflow_tablespace IS NOT NULL OR
         in_table_rec.overflow_logging IS NOT NULL
      THEN
        l_iot_clause := l_iot_clause ||' '||
                        gc_overflow ||' '||
                        get_clause(gc_including_column, in_table_rec.iot_include_column)||
                        get_clause(gc_tablespace, in_table_rec.overflow_tablespace, '"', '"')||
                        get_clause(gc_logging, in_table_rec.overflow_logging);
      END IF;
    END IF;

    IF in_swap_table_rec.partitioned = 'YES' AND in_swap_table_rec.subpartitioning_type = 'NONE'
    THEN
      l_partition_sql := 'PARTITION BY '||in_swap_table_rec.partitioning_type||'('||convert_arr_to_str(in_swap_table_rec.part_key_column_arr)||')  ('||CHR(10)||
      get_partitions_sql(in_swap_table_rec.partition_arr, in_swap_table_rec.subpartition_arr)||CHR(10)||
      ')';
    ELSIF in_swap_table_rec.partitioned = 'YES' AND in_swap_table_rec.subpartitioning_type <> 'NONE' THEN
      l_partition_sql := 'PARTITION BY '||in_swap_table_rec.partitioning_type||'('||convert_arr_to_str(in_swap_table_rec.part_key_column_arr)||')'||CHR(10)||
      '  SUBPARTITION BY '||in_swap_table_rec.subpartitioning_type||'('||convert_arr_to_str(in_swap_table_rec.subpart_key_column_arr)||')  ('||CHR(10)||
      get_partitions_sql(in_swap_table_rec.partition_arr, in_swap_table_rec.subpartition_arr)||CHR(10)||
      ')';
    END IF;

    -- create swap table
    l_create_sql := 'CREATE TABLE "'||in_swap_table_rec.owner||'"."'||in_swap_table_rec.table_name||'" ('||l_columns_list||CHR(10)||')'||
                    l_col_properties||CHR(10)||l_iot_clause||l_partition_sql;

    l_drop_sql := 'DROP TABLE "'||in_swap_table_rec.owner||'"."'||in_swap_table_rec.table_name||'"';

    add_stmt(
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr,
      in_frwd_stmt     => l_create_sql,
      in_rlbk_stmt     => l_drop_sql
    );


    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      l_index_rec := in_table_rec.index_arr(i);
      l_index_rec.table_name := in_swap_table_rec.table_name;
      l_index_rec.index_name := in_swap_table_rec.table_name||'$I'||TO_CHAR(i,'fm0XXX');
      l_index_rec.rename_rec.current_name := l_index_rec.index_name;
      IF l_partition_sql IS NULL
      THEN
        l_index_rec := NULL;
      END IF;
      create_index(
        in_index_rec             => l_index_rec,
        in_table_name            => l_index_rec.table_name,
        io_frwd_alter_stmt_arr   => io_frwd_stmt_arr,
        io_rlbk_alter_stmt_arr   => io_rlbk_stmt_arr
      );
      IF l_index_rec.constraint_name IS NOT NULL AND
         in_table_rec.constraint_indx_arr.EXISTS(l_index_rec.constraint_name)
      THEN
        l_indx := in_table_rec.constraint_indx_arr(l_index_rec.constraint_name);
        l_constraint_rec := in_swap_table_rec.constraint_arr(l_indx);
        l_constraint_rec.table_name := in_swap_table_rec.table_name;
        l_constraint_rec.constraint_name := in_swap_table_rec.table_name||'$C'||TO_CHAR(i,'fm0XXX');
        l_constraint_rec.rename_rec.current_name := in_swap_table_rec.table_name||'$C'||TO_CHAR(i,'fm0XXX');
        add_constraint(
          in_constraint_rec        => l_constraint_rec,
          in_index_rec             => l_index_rec,
          io_frwd_alter_stmt_arr   => io_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => io_rlbk_stmt_arr
        );
      END IF;
      l_indx := l_indx + 1;
    END LOOP;

  END create_swap_table_sql;

  -- create clone table for given table
  PROCEDURE create_clone_table_sql(
    in_table_rec       IN cort_exec_pkg.gt_table_rec,
    in_simple_mode     IN BOOLEAN DEFAULT FALSE,
    in_rename_mode     IN VARCHAR2 DEFAULT 'TO_TEMP',
    in_all_partitions  IN BOOLEAN DEFAULT TRUE,
    io_frwd_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_create_sql     CLOB;
    l_drop_sql       CLOB;
    l_partition_sql  CLOB;
    l_columns_list   VARCHAR2(32767);
    l_iot_clause     VARCHAR2(32767);
    l_constraint_rec cort_exec_pkg.gt_constraint_rec;
    l_col_prop       VARCHAR2(32767);
    l_col_properties VARCHAR2(32767);
  BEGIN
    FOR i IN 1..in_table_rec.column_arr.COUNT LOOP
      IF in_table_rec.column_arr(i).hidden_column = 'NO'
      THEN
        l_columns_list := l_columns_list||CHR(10)||'  '||RTRIM(get_column_clause(in_table_rec, in_table_rec.column_arr(i)))||',';
        l_col_prop := get_column_properties(
                        in_table_rec  => in_table_rec,
                        in_column_rec => in_table_rec.column_arr(i)
                      );
        IF l_col_prop IS NOT NULL THEN
          l_col_properties := l_col_properties||CHR(10)||l_col_prop;
        END IF;
      END IF;
    END LOOP;

    IF NOT in_simple_mode THEN
      FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
        l_constraint_rec := in_table_rec.constraint_arr(i);
        l_constraint_rec.table_name := get_new_name(in_table_rec.rename_rec, in_rename_mode);
        l_constraint_rec.constraint_name := get_new_name(in_table_rec.constraint_arr(i).rename_rec, in_rename_mode);
        l_constraint_rec.rename_rec.current_name := get_new_name(in_table_rec.constraint_arr(i).rename_rec, in_rename_mode);

        l_columns_list := l_columns_list||CHR(10)||REGEXP_REPLACE(get_add_constraint_clause(l_constraint_rec, NULL), 'ADD CONSTRAINT', 'CONSTRAINT', 1, 1)||',';
      END LOOP;
    END IF;
    l_columns_list := TRIM(',' FROM l_columns_list);

    IF NOT in_simple_mode AND in_table_rec.iot_name IS NOT NULL
    THEN
      l_iot_clause := 'ORGANIZATION INDEX '||
                      get_clause(gc_pct_threshold, in_table_rec.iot_pct_threshold)||
                      get_clause(gc_key_compression, in_table_rec.iot_key_compression, NULL, ' '||in_table_rec.iot_prefix_length)||
                      get_clause(gc_mapping, in_table_rec.mapping_table);
      IF in_table_rec.iot_include_column IS NOT NULL OR
         in_table_rec.overflow_tablespace IS NOT NULL OR
         in_table_rec.overflow_logging IS NOT NULL
      THEN
        l_iot_clause := l_iot_clause ||' '||
                        gc_overflow ||' '||
                        get_clause(gc_including_column, in_table_rec.iot_include_column)||
                        get_clause(gc_tablespace, in_table_rec.overflow_tablespace, '"', '"')||
                        get_clause(gc_logging, in_table_rec.overflow_logging);
      END IF;
    END IF;

    IF in_table_rec.partitioned = 'YES' AND in_table_rec.subpartitioning_type = 'NONE'
    THEN
      l_partition_sql := 'PARTITION BY '||in_table_rec.partitioning_type||'('||convert_arr_to_str(in_table_rec.part_key_column_arr)||') ';
      IF NOT in_all_partitions THEN
        IF in_table_rec.partitioning_type IN ('RANGE', 'LIST', 'SYSTEM') THEN
          l_partition_sql := l_partition_sql || '('||CHR(10)||get_partition_clause(in_table_rec.partition_arr(1), 'PARTITION') ||
                              get_partition_desc_clause(
                                in_partition_rec    => in_table_rec.partition_arr(1),
                                in_subpartition_arr => in_table_rec.subpartition_arr
                              )||CHR(10)||')';
        END IF;
      ELSE
        l_partition_sql := l_partition_sql || '('||CHR(10)||get_partitions_sql(in_table_rec.partition_arr, in_table_rec.subpartition_arr)||CHR(10)||')';
      END IF;
    ELSIF in_table_rec.partitioned = 'YES' AND in_table_rec.subpartitioning_type <> 'NONE' THEN
      l_partition_sql := 'PARTITION BY '||in_table_rec.partitioning_type||'('||convert_arr_to_str(in_table_rec.part_key_column_arr)||')'||CHR(10)||
      '  SUBPARTITION BY '||in_table_rec.subpartitioning_type||'('||convert_arr_to_str(in_table_rec.subpart_key_column_arr)||')  ';
      IF NOT in_all_partitions THEN
        IF in_table_rec.partitioning_type IN ('RANGE', 'LIST', 'SYSTEM') THEN
          l_partition_sql := l_partition_sql || '('||CHR(10)||get_partition_clause(in_table_rec.partition_arr(1), 'PARTITION') ||
                              get_partition_desc_clause(
                                in_partition_rec    => in_table_rec.partition_arr(1),
                                in_subpartition_arr => in_table_rec.subpartition_arr
                              )||CHR(10)||')';
        END IF;
      ELSE
        l_partition_sql := l_partition_sql || '('||CHR(10)||get_partitions_sql(in_table_rec.partition_arr, in_table_rec.subpartition_arr)||CHR(10)||')';
      END IF;
    END IF;

    l_create_sql := 'CREATE TABLE "'||in_table_rec.owner||'"."'||get_new_name(in_table_rec.rename_rec, in_rename_mode)||'" ('||l_columns_list||CHR(10)||')'||
                    l_col_properties||CHR(10)||l_iot_clause||l_partition_sql;

    l_drop_sql := 'DROP TABLE "'||in_table_rec.owner||'"."'||get_new_name(in_table_rec.rename_rec, in_rename_mode)||'"';

    add_stmt(
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr,
      in_frwd_stmt     => l_create_sql,
      in_rlbk_stmt     => l_drop_sql
    );

  END create_clone_table_sql;

  -- exchanges source table partition (in_partition_name) with target table
  PROCEDURE exchange_partition(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_partition_rec    IN cort_exec_pkg.gt_partition_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_sql   VARCHAR2(32767);
  BEGIN
    l_sql := ' EXCHANGE '||in_partition_rec.partition_level||' '||in_partition_rec.partition_name||' WITH TABLE "'||in_target_table_rec.owner||'"."'||in_target_table_rec.rename_rec.current_name||'" INCLUDING INDEXES WITHOUT VALIDATION';
    add_alter_table_stmt(
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr,
      in_table_name    => in_source_table_rec.rename_rec.current_name,
      in_owner         => in_source_table_rec.owner,
      in_frwd_clause   => l_sql,
      in_rlbk_clause   => l_sql
    );
  END exchange_partition;


  FUNCTION get_grant_privilege_clause(
    in_privilege_rec         IN cort_exec_pkg.gt_privilege_rec
  )
  RETURN VARCHAR2
  AS
    l_clause            VARCHAR2(32767);
    l_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_clause_indx_arr(gc_grant_privilege) := get_clause(gc_grant_privilege, in_privilege_rec.privilege);
    l_clause_indx_arr(gc_privilege_column) := get_clause(gc_privilege_column, in_privilege_rec.column_name, '(', ')');
    l_clause_indx_arr(gc_on_table) := get_clause(gc_on_table, in_privilege_rec.table_schema||'"."'||in_privilege_rec.table_name, '"', '"');
    l_clause_indx_arr(gc_to) := get_clause(gc_to, in_privilege_rec.grantee, '"', '"');
    l_clause_indx_arr(gc_hierarchy_option) := get_clause(gc_hierarchy_option, in_privilege_rec.hierarchy);
    l_clause_indx_arr(gc_grant_option) := get_clause(gc_grant_option, in_privilege_rec.grantable);

    l_clause := get_clause_by_name(l_clause_indx_arr, gc_grant_privilege)||
                get_clause_by_name(l_clause_indx_arr, gc_privilege_column)||
                get_clause_by_name(l_clause_indx_arr, gc_on_table)||
                get_clause_by_name(l_clause_indx_arr, gc_to)||
                get_clause_by_name(l_clause_indx_arr, gc_hierarchy_option)||
                get_clause_by_name(l_clause_indx_arr, gc_grant_option);

    RETURN l_clause;

  END get_grant_privilege_clause;

  FUNCTION get_revoke_privilege_clause(
    in_privilege_rec         IN cort_exec_pkg.gt_privilege_rec
  )
  RETURN VARCHAR2
  AS
    l_clause            VARCHAR2(32767);
    l_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_clause_indx_arr(gc_revoke_privilege) := get_clause(gc_revoke_privilege, in_privilege_rec.privilege);
    l_clause_indx_arr(gc_on_table) := get_clause(gc_on_table, in_privilege_rec.table_schema||'"."'||in_privilege_rec.table_name, '"', '"');
    l_clause_indx_arr(gc_from) := get_clause(gc_from, in_privilege_rec.grantee, '"', '"');

    l_clause := get_clause_by_name(l_clause_indx_arr, gc_revoke_privilege)||
                get_clause_by_name(l_clause_indx_arr, gc_on_table)||
                get_clause_by_name(l_clause_indx_arr, gc_from);

    RETURN l_clause;

  END get_revoke_privilege_clause;

  -- return privileges statements
  PROCEDURE get_privileges_stmt(
    in_privilege_arr         IN cort_exec_pkg.gt_privilege_arr,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause VARCHAR2(32767);
    l_rlbk_clause VARCHAR2(32767);
  BEGIN
    FOR i IN 1..in_privilege_arr.COUNT LOOP
      IF in_privilege_arr(i).grantee <> user THEN
        l_frwd_clause := get_grant_privilege_clause(in_privilege_arr(i));
        l_rlbk_clause := get_revoke_privilege_clause(in_privilege_arr(i));
        io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := TRIM(l_frwd_clause);
        io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := TRIM(l_rlbk_clause);
      END IF;
    END LOOP;
  END get_privileges_stmt;


  -- copies table privileges
  PROCEDURE copy_privileges(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_indx        PLS_INTEGER;
    l_col_name    arrays.gt_name;
  BEGIN
   io_target_table_rec.privilege_arr.DELETE;
    FOR i IN 1..in_source_table_rec.privilege_arr.COUNT LOOP
      l_col_name := in_source_table_rec.privilege_arr(i).column_name;
      IF l_col_name IS NOT NULL THEN
        IF in_source_table_rec.column_indx_arr.EXISTS(l_col_name) THEN
          l_indx := in_source_table_rec.column_indx_arr(l_col_name);
          l_col_name := NVL(in_source_table_rec.column_arr(l_indx).new_column_name,in_source_table_rec.column_arr(l_indx).column_name);
          IF io_target_table_rec.column_indx_arr.EXISTS(l_col_name) THEN
            l_indx := io_target_table_rec.privilege_arr.COUNT+1;
            io_target_table_rec.privilege_arr(l_indx) := in_source_table_rec.privilege_arr(i);
            io_target_table_rec.privilege_arr(l_indx).table_schema := io_target_table_rec.owner;
            io_target_table_rec.privilege_arr(l_indx).table_name := io_target_table_rec.table_name;
            io_target_table_rec.privilege_arr(l_indx).column_name := l_col_name;
          END IF;
        END IF;
      ELSE
        l_indx := io_target_table_rec.privilege_arr.COUNT+1;
        io_target_table_rec.privilege_arr(l_indx) := in_source_table_rec.privilege_arr(i);
        io_target_table_rec.privilege_arr(l_indx).table_schema := io_target_table_rec.owner;
        io_target_table_rec.privilege_arr(l_indx).table_name := io_target_table_rec.table_name;
      END IF;
    END LOOP;

    get_privileges_stmt(
      in_privilege_arr         => io_target_table_rec.privilege_arr,
      io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
      io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
    );

  END copy_privileges;

  FUNCTION get_create_trigger_clause(
    in_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_trigger_rec    IN cort_exec_pkg.gt_trigger_rec
  )
  RETURN CLOB
  AS
    l_clause            CLOB;
    l_clause_indx_arr   arrays.gt_xlstr_indx;
    l_indx              PLS_INTEGER;
  BEGIN
    l_clause_indx_arr(gc_create_trigger) := TRIM(get_clause(gc_create_trigger, in_trigger_rec.owner||'"."'||in_trigger_rec.rename_rec.current_name, '"','"'));
    l_clause_indx_arr(gc_trigger_type) := get_clause(gc_trigger_type, in_trigger_rec.trigger_type);
    l_clause_indx_arr(gc_triggerring_event) := get_clause(gc_triggerring_event, in_trigger_rec.triggering_event);
    l_clause_indx_arr(gc_on_table) := get_clause(gc_on_table, in_table_rec.owner||'"."'||in_table_rec.table_name, '"','"');
    l_clause_indx_arr(gc_referencing_names) := get_clause(gc_referencing_names, in_trigger_rec.referencing_names);
    l_clause_indx_arr(gc_for_each_row) := get_clause(gc_for_each_row, in_trigger_rec.trigger_type);
    IF in_trigger_rec.referenced_trigger_indx IS NOT NULL THEN
      IF in_table_rec.trigger_arr.EXISTS(in_trigger_rec.referenced_trigger_indx) THEN
        l_indx := in_trigger_rec.referenced_trigger_indx;
        l_clause_indx_arr(gc_follows) := get_clause(gc_follows, in_table_rec.trigger_arr(l_indx).owner||'"."'||in_table_rec.trigger_arr(l_indx).rename_rec.current_name, '"', '"');
      END IF;
    END IF;
    l_clause_indx_arr(gc_status) := get_clause(gc_status, in_trigger_rec.status);
    l_clause_indx_arr(gc_when) := get_clause(gc_when, in_trigger_rec.when_clause);

    l_clause := get_clause_by_name(l_clause_indx_arr, gc_create_trigger)||
                get_clause_by_name(l_clause_indx_arr, gc_trigger_type)||
                get_clause_by_name(l_clause_indx_arr, gc_triggerring_event)||
                get_clause_by_name(l_clause_indx_arr, gc_on_table)||
                get_clause_by_name(l_clause_indx_arr, gc_referencing_names)||
                get_clause_by_name(l_clause_indx_arr, gc_for_each_row)||
                get_clause_by_name(l_clause_indx_arr, gc_follows)||
                get_clause_by_name(l_clause_indx_arr, gc_status)||
                get_clause_by_name(l_clause_indx_arr, gc_when)||
                in_trigger_rec.trigger_body;

    RETURN l_clause;
  END get_create_trigger_clause;

  FUNCTION get_drop_trigger_clause(
    in_trigger_rec         IN cort_exec_pkg.gt_trigger_rec
  )
  RETURN CLOB
  AS
    l_clause            CLOB;
  BEGIN
    l_clause := TRIM(get_clause(gc_drop_trigger, in_trigger_rec.owner||'"."'||in_trigger_rec.rename_rec.current_name, '"','"'));
    RETURN l_clause;
  END get_drop_trigger_clause;

  -- updates trigger
  PROCEDURE update_trigger(
    in_table_rec             IN cort_exec_pkg.gt_table_rec,
    in_trigger_rec           IN cort_exec_pkg.gt_trigger_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
  BEGIN
    io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := get_create_trigger_clause(in_table_rec, in_trigger_rec);
    io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := get_drop_trigger_clause(in_trigger_rec);
    io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := NULL;
    io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := NULL;
  END update_trigger;

  -- drops table triggers
  PROCEDURE drop_triggers(
    in_table_rec             IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause CLOB;
    l_rlbk_clause CLOB;
  BEGIN
    FOR i IN 1..in_table_rec.trigger_arr.COUNT LOOP
      l_frwd_clause := get_drop_trigger_clause(in_table_rec.trigger_arr(i));
      l_rlbk_clause := get_create_trigger_clause(in_table_rec, in_table_rec.trigger_arr(i));
      io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := l_frwd_clause;
      io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := l_rlbk_clause;
    END LOOP;
  END drop_triggers;


  -- copies table triggers
  PROCEDURE copy_triggers(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_target_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_clause CLOB;
    l_rlbk_clause CLOB;
  BEGIN
    io_target_table_rec.trigger_arr := in_source_table_rec.trigger_arr;
    io_target_table_rec.trigger_indx_arr := in_source_table_rec.trigger_indx_arr;
    FOR i IN 1..io_target_table_rec.trigger_arr.COUNT LOOP
      io_target_table_rec.trigger_arr(i).table_name := io_target_table_rec.table_name;
      io_target_table_rec.trigger_arr(i).table_owner := io_target_table_rec.owner;

      l_frwd_clause := get_create_trigger_clause(io_target_table_rec, io_target_table_rec.trigger_arr(i));
      l_rlbk_clause := get_drop_trigger_clause(io_target_table_rec.trigger_arr(i));
      io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := l_frwd_clause;
      io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := l_rlbk_clause;
    END LOOP;
  END copy_triggers;

  -- copies table policies
  PROCEDURE copy_policies(
    in_source_table_rec      IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec      IN cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr   IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_policy_function  VARCHAR2(100);
    l_statement_types  VARCHAR2(100);
    l_rel_cols         VARCHAR2(32767);
    l_frwd_clause      CLOB;
    l_rlbk_clause      CLOB;
  BEGIN
    FOR i IN 1..in_source_table_rec.policy_arr.COUNT LOOP
      IF in_source_table_rec.policy_arr(i).package IS NOT NULL THEN
        l_policy_function := '"'||in_source_table_rec.policy_arr(i).package||'"."'||in_source_table_rec.policy_arr(i).function||'"';
      ELSE
        l_policy_function := '"'||in_source_table_rec.policy_arr(i).function||'"';
      END IF;
      l_statement_types := NULL;
      IF in_source_table_rec.policy_arr(i).sel = 'YES' THEN
        l_statement_types := l_statement_types ||'SELECT,';
      END IF;
      IF in_source_table_rec.policy_arr(i).ins = 'YES' THEN
        l_statement_types := l_statement_types ||'INSERT,';
      END IF;
      IF in_source_table_rec.policy_arr(i).upd = 'YES' THEN
        l_statement_types := l_statement_types ||'UPDATE,';
      END IF;
      IF in_source_table_rec.policy_arr(i).del = 'YES' THEN
        l_statement_types := l_statement_types ||'DELETE,';
      END IF;
      IF in_source_table_rec.policy_arr(i).idx = 'YES' THEN
        l_statement_types := l_statement_types ||'INDEX';
      END IF;
      l_statement_types := TRIM(',' FROM l_statement_types);
      FOR j IN 1..in_source_table_rec.policy_arr(i).sec_rel_col_arr.COUNT LOOP
        l_rel_cols := l_rel_cols||in_source_table_rec.policy_arr(i).sec_rel_col_arr(j)||',';
      END LOOP;
      l_rel_cols := TRIM(',' FROM l_rel_cols);

      l_frwd_clause := '
  BEGIN
    DBMS_RLS.ADD_GROUPED_POLICY (
      object_schema         => ''"'||in_target_table_rec.owner||'"'',
      object_name           => ''"'||in_target_table_rec.table_name||'"'',
      policy_group          => ''"'||in_source_table_rec.policy_arr(i).policy_group||'"'',
      policy_name           => ''"'||in_source_table_rec.policy_arr(i).policy_name||'"'',
      function_schema       => ''"'||in_source_table_rec.policy_arr(i).pf_owner||'"'',
      policy_function       => '''||l_policy_function||''',
      statement_types       => '''||l_statement_types||''',
      update_check          => '||CASE WHEN in_source_table_rec.policy_arr(i).chk_option = 'YES' THEN 'TRUE' ELSE 'FALSE' END||',
      enable                => '||CASE WHEN in_source_table_rec.policy_arr(i).enable = 'YES' THEN 'TRUE' ELSE 'FALSE' END||',
      static_policy         => '||CASE WHEN in_source_table_rec.policy_arr(i).static_policy = 'YES' THEN 'TRUE' ELSE 'FALSE' END||',
      policy_type           => '||CASE WHEN in_source_table_rec.policy_arr(i).policy_type IS NOT NULL THEN 'DBMS_RLS.'||in_source_table_rec.policy_arr(i).policy_type ELSE 'NULL' END||',
      long_predicate        => '||CASE WHEN in_source_table_rec.policy_arr(i).long_predicate = 'YES' THEN 'TRUE' ELSE 'FALSE' END||',
      sec_relevant_cols     => '''||l_rel_cols||''',
      sec_relevant_cols_opt => '||CASE WHEN in_source_table_rec.policy_arr(i).column_option = 'ALL_ROWS' THEN 'DBMS_RLS.ALL_ROWS' ELSE 'NULL' END ||'
    );
  END;';

      l_rlbk_clause := '
  BEGIN
    DBMS_RLS.DROP_GROUPED_POLICY (
      object_schema         => '''||in_target_table_rec.owner||''',
      object_name           => '''||in_target_table_rec.table_name||''',
      policy_group          => '''||in_source_table_rec.policy_arr(i).policy_group||''',
      policy_name           => '''||in_source_table_rec.policy_arr(i).policy_name||'''
    );
  END;';

      io_frwd_alter_stmt_arr(io_frwd_alter_stmt_arr.COUNT+1) := l_frwd_clause;
      io_rlbk_alter_stmt_arr(io_rlbk_alter_stmt_arr.COUNT+1) := l_rlbk_clause;
    END LOOP;
  END copy_policies;


  -- returns list of individual partitions clauses
  FUNCTION get_partitions_sql(
    in_partition_arr    IN cort_exec_pkg.gt_partition_arr,
    in_subpartition_arr IN cort_exec_pkg.gt_partition_arr
  )
  RETURN CLOB
  AS
    l_sql      CLOB;
  BEGIN
    FOR i IN 1..in_partition_arr.COUNT LOOP
      l_sql := l_sql || '  '
                     || get_partition_clause(in_partition_arr(i), 'PARTITION') ||
                        get_partition_desc_clause(
                          in_partition_rec    => in_partition_arr(i),
                          in_subpartition_arr => in_subpartition_arr
                        );
      IF i < in_partition_arr.COUNT THEN
        l_sql := l_sql || ',' || CHR(10);
      END IF;
    END LOOP;
    RETURN l_sql;
  END get_partitions_sql;


  -- returns matrix of type convertion
  FUNCTION get_type_convert_matrix
  RETURN gt_type_matrix
  AS
    -- data types
    c_number   CONSTANT VARCHAR2(30) := 'NUMBER';
    c_float    CONSTANT VARCHAR2(30) := 'FLOAT';
    c_bin_db   CONSTANT VARCHAR2(30) := 'BINARY_DOUBLE';
    c_vchar    CONSTANT VARCHAR2(30) := 'VARCHAR2';
    c_char     CONSTANT VARCHAR2(30) := 'CHAR';
    c_nvchar   CONSTANT VARCHAR2(30) := 'NVARCHAR2';
    c_nchar    CONSTANT VARCHAR2(30) := 'NCHAR';
    c_raw      CONSTANT VARCHAR2(30) := 'RAW';
    c_date     CONSTANT VARCHAR2(30) := 'DATE';
    c_time     CONSTANT VARCHAR2(30) := 'TIMESTAMP';
    c_timetz   CONSTANT VARCHAR2(30) := 'TIMESTAMP WITH TIMEZONE';
    c_timeltz  CONSTANT VARCHAR2(30) := 'TIMESTAMP WITH LOCAL TIMEZONE';
    c_int_ym   CONSTANT VARCHAR2(30) := 'INTERVAL YEAR TO MONTH';
    c_int_ds   CONSTANT VARCHAR2(30) := 'INTERVAL DAY TO SECOND';
    c_clob     CONSTANT VARCHAR2(30) := 'CLOB';
    c_nclob    CONSTANT VARCHAR2(30) := 'NCLOB';
    c_blob     CONSTANT VARCHAR2(30) := 'BLOB';
    c_long     CONSTANT VARCHAR2(30) := 'LONG';
    c_longraw  CONSTANT VARCHAR2(30) := 'LONG RAW';
    c_xmltype  CONSTANT VARCHAR2(30) := 'XMLTYPE';
    c_urowid   CONSTANT VARCHAR2(30) := 'UROWID';
    -- convertion matrix
    l_mx       gt_type_matrix;
    -- expressions
    l_e0       VARCHAR2(255) := 'NULL';
    l_e1       VARCHAR2(255) := '<alias>.<source_column>';
    l_e2       VARCHAR2(255) := 'utl_raw.cast_from_number(<alias>.<source_column>)';
    l_e3       VARCHAR2(255) := 'utl_raw.cast_from_binary_double(<alias>.<source_column>)';
    l_e4       VARCHAR2(255) := 'utl_raw.cast_to_raw(<alias>.<source_column>)';
    l_e5       VARCHAR2(255) := 'to_yminterval(<alias>.<source_column>)';
    l_e6       VARCHAR2(255) := 'to_dsinterval(<alias>.<source_column>)';
    l_e7       VARCHAR2(255) := 'XMLTYPE.CREATEXML(<alias>.<source_column>)';
    l_e8       VARCHAR2(255) := 'trim(<alias>.<source_column>)';
    l_e9       VARCHAR2(255) := 'to_yminterval(trim(<alias>.<source_column>))';
    l_e10      VARCHAR2(255) := 'to_dsinterval(trim(<alias>.<source_column>))';
    l_e11      VARCHAR2(255) := 'utl_raw.cast_to_number(<alias>.<source_column>)';
    l_e12      VARCHAR2(255) := 'utl_raw.cast_to_binary_double(<alias>.<source_column>)';
    l_e13      VARCHAR2(255) := 'utl_raw.cast_to_varchar2(<alias>.<source_column>)';
    l_e14      VARCHAR2(255) := 'utl_raw.cast_to_nvarchar2(<alias>.<source_column>)';
    l_e15      VARCHAR2(255) := 'TO_LOB(<alias>.<source_column>)';
    l_e16      VARCHAR2(255) := 'XMLTYPE.CREATEXML(TO_LOB(<alias>.<source_column>))';
    l_e17      VARCHAR2(255) := '<alias>.<source_column>.GetStringVal()';
    l_e18      VARCHAR2(255) := '<alias>.<source_column>.GetClobVal()';
    l_e19      VARCHAR2(255) := 'TO_CHAR(<alias>.<source_column>)';
    l_e20      VARCHAR2(255) := 'to_yminterval(to_char(<alias>.<source_column>))';
    l_e21      VARCHAR2(255) := 'to_dsinterval(to_char(<alias>.<source_column>))';

  BEGIN
    -- source    target
    -- NUMBER                          -- FLOAT                          -- BINARY_DOUBLE                   -- VARCHAR2                        -- CHAR                             -- NVARCHAR2                        -- NCHAR
    l_mx(c_number)(c_float) := l_e1;   l_mx(c_float)(c_number) := l_e1;  l_mx(c_bin_db)(c_number) := l_e1;  l_mx(c_vchar)(c_number) := l_e1;   l_mx(c_char)(c_number) := l_e8;     l_mx(c_nvchar)(c_number) := l_e1;   l_mx(c_nchar)(c_number) := l_e8;
    l_mx(c_number)(c_bin_db) := l_e1;  l_mx(c_float)(c_bin_db) := l_e1;  l_mx(c_bin_db)(c_float) := l_e1;   l_mx(c_vchar)(c_float) := l_e1;    l_mx(c_char)(c_float) := l_e8;      l_mx(c_nvchar)(c_float) := l_e1;    l_mx(c_nchar)(c_float) := l_e8;
    l_mx(c_number)(c_vchar) := l_e1;   l_mx(c_float)(c_vchar) := l_e1;   l_mx(c_bin_db)(c_vchar) := l_e1;   l_mx(c_vchar)(c_bin_db) := l_e1;   l_mx(c_char)(c_bin_db) := l_e8;     l_mx(c_nvchar)(c_bin_db) := l_e1;   l_mx(c_nchar)(c_bin_db) := l_e8;
    l_mx(c_number)(c_char) := l_e1;    l_mx(c_float)(c_char) := l_e1;    l_mx(c_bin_db)(c_char) := l_e1;    l_mx(c_vchar)(c_char) := l_e1;     l_mx(c_char)(c_vchar) := l_e1;      l_mx(c_nvchar)(c_vchar) := l_e1;    l_mx(c_nchar)(c_vchar) := l_e1;
    l_mx(c_number)(c_nvchar) := l_e1;  l_mx(c_float)(c_nvchar) := l_e1;  l_mx(c_bin_db)(c_nvchar) := l_e1;  l_mx(c_vchar)(c_nvchar) := l_e1;   l_mx(c_char)(c_nvchar) := l_e1;     l_mx(c_nvchar)(c_char) := l_e1;     l_mx(c_nchar)(c_char) := l_e1;
    l_mx(c_number)(c_nchar) := l_e1;   l_mx(c_float)(c_nchar) := l_e1;   l_mx(c_bin_db)(c_nchar) := l_e1;   l_mx(c_vchar)(c_nchar) := l_e1;    l_mx(c_char)(c_nchar) := l_e1;      l_mx(c_nvchar)(c_nchar) := l_e1;    l_mx(c_nchar)(c_nvchar) := l_e1;
    l_mx(c_number)(c_raw) := l_e2;     l_mx(c_float)(c_raw) := l_e2;     l_mx(c_bin_db)(c_raw) := l_e3;     l_mx(c_vchar)(c_raw) := l_e4;      l_mx(c_char)(c_raw) := l_e4;        l_mx(c_nvchar)(c_raw) := l_e4;      l_mx(c_nchar)(c_raw) := l_e4;
    l_mx(c_number)(c_date) := l_e0;    l_mx(c_float)(c_date) := l_e0;    l_mx(c_bin_db)(c_date) := l_e0;    l_mx(c_vchar)(c_date) := l_e1;     l_mx(c_char)(c_date) := l_e8;       l_mx(c_nvchar)(c_date) := l_e1;     l_mx(c_nchar)(c_date) := l_e1;
    l_mx(c_number)(c_time) := l_e0;    l_mx(c_float)(c_time) := l_e0;    l_mx(c_bin_db)(c_time) := l_e0;    l_mx(c_vchar)(c_time) := l_e1;     l_mx(c_char)(c_time) := l_e8;       l_mx(c_nvchar)(c_time) := l_e1;     l_mx(c_nchar)(c_time) := l_e8;
    l_mx(c_number)(c_timetz) := l_e0;  l_mx(c_float)(c_timetz) := l_e0;  l_mx(c_bin_db)(c_timetz) := l_e0;  l_mx(c_vchar)(c_timetz) := l_e1;   l_mx(c_char)(c_timetz) := l_e8;     l_mx(c_nvchar)(c_timetz) := l_e1;   l_mx(c_nchar)(c_timetz) := l_e8;
    l_mx(c_number)(c_timeltz) := l_e0; l_mx(c_float)(c_timeltz) := l_e0; l_mx(c_bin_db)(c_timeltz) := l_e0; l_mx(c_vchar)(c_timeltz) := l_e1;  l_mx(c_char)(c_timeltz) := l_e8;    l_mx(c_nvchar)(c_timeltz) := l_e1;  l_mx(c_nchar)(c_timeltz) := l_e8;
    l_mx(c_number)(c_int_ym) := l_e0;  l_mx(c_float)(c_int_ym) := l_e0;  l_mx(c_bin_db)(c_int_ym) := l_e0;  l_mx(c_vchar)(c_int_ym) := l_e5;   l_mx(c_char)(c_int_ym) := l_e9;     l_mx(c_nvchar)(c_int_ym) := l_e5;   l_mx(c_nchar)(c_int_ym) := l_e9;
    l_mx(c_number)(c_int_ds) := l_e0;  l_mx(c_float)(c_int_ds) := l_e0;  l_mx(c_bin_db)(c_int_ds) := l_e0;  l_mx(c_vchar)(c_int_ds) := l_e6;   l_mx(c_char)(c_int_ds) := l_e10;    l_mx(c_nvchar)(c_int_ds) := l_e6;   l_mx(c_nchar)(c_int_ds) := l_e10;
    l_mx(c_number)(c_clob) := l_e1;    l_mx(c_float)(c_clob) := l_e1;    l_mx(c_bin_db)(c_clob) := l_e1;    l_mx(c_vchar)(c_clob) := l_e1;     l_mx(c_char)(c_clob) := l_e1;       l_mx(c_nvchar)(c_clob) := l_e1;     l_mx(c_nchar)(c_clob) := l_e1;
    l_mx(c_number)(c_nclob) := l_e1;   l_mx(c_float)(c_nclob) := l_e1;   l_mx(c_bin_db)(c_nclob) := l_e1;   l_mx(c_vchar)(c_nclob) := l_e1;    l_mx(c_char)(c_nclob) := l_e1;      l_mx(c_nvchar)(c_nclob) := l_e1;    l_mx(c_nchar)(c_nclob) := l_e1;
    l_mx(c_number)(c_blob) := l_e2;    l_mx(c_float)(c_blob) := l_e2;    l_mx(c_bin_db)(c_blob) := l_e3;    l_mx(c_vchar)(c_blob) := l_e4;     l_mx(c_char)(c_blob) := l_e4;       l_mx(c_nvchar)(c_blob) := l_e4;     l_mx(c_nchar)(c_blob) := l_e4;
    l_mx(c_number)(c_long) := l_e1;    l_mx(c_float)(c_long) := l_e1;    l_mx(c_bin_db)(c_long) := l_e1;    l_mx(c_vchar)(c_long) := l_e1;     l_mx(c_char)(c_long) := l_e1;       l_mx(c_nvchar)(c_long) := l_e1;     l_mx(c_nchar)(c_long) := l_e1;
    l_mx(c_number)(c_longraw) := l_e2; l_mx(c_float)(c_longraw) := l_e2; l_mx(c_bin_db)(c_longraw) := l_e3; l_mx(c_vchar)(c_longraw) := l_e4;  l_mx(c_char)(c_longraw) := l_e4;    l_mx(c_nvchar)(c_longraw) := l_e4;  l_mx(c_nchar)(c_longraw) := l_e4;
    l_mx(c_number)(c_xmltype) := l_e0; l_mx(c_float)(c_xmltype) := l_e0; l_mx(c_bin_db)(c_xmltype) := l_e0; l_mx(c_vchar)(c_xmltype) := l_e7;  l_mx(c_char)(c_xmltype) := l_e7;    l_mx(c_nvchar)(c_xmltype) := l_e7;  l_mx(c_nchar)(c_xmltype) := l_e7;
    l_mx(c_number)(c_urowid) := l_e0;  l_mx(c_float)(c_urowid) := l_e0;  l_mx(c_bin_db)(c_urowid) := l_e0;  l_mx(c_vchar)(c_urowid) := l_e1;   l_mx(c_char)(c_urowid) := l_e8;     l_mx(c_nvchar)(c_urowid) := l_e1;   l_mx(c_nchar)(c_urowid) := l_e8;

    -- RAW                             -- DATE                           -- TIMESTAMP                       -- TIMESTAMP WITH TIMEZONE         -- TIMESTAMP WITH LOCAL TIMEZONE    -- INTERVAL YEAR TO MONTH           -- INTERVAL DAY TO SECOND
    l_mx(c_raw)(c_number) := l_e11;    l_mx(c_date)(c_number) := l_e0;   l_mx(c_time)(c_number) := l_e0;    l_mx(c_timetz)(c_number) := l_e0;  l_mx(c_timeltz)(c_number) := l_e0;  l_mx(c_int_ym)(c_number) := l_e0;   l_mx(c_int_ds)(c_number) := l_e0;
    l_mx(c_raw)(c_float) := l_e11;     l_mx(c_date)(c_float) := l_e0;    l_mx(c_time)(c_float) := l_e0;     l_mx(c_timetz)(c_float) := l_e0;   l_mx(c_timeltz)(c_float) := l_e0;   l_mx(c_int_ym)(c_float) := l_e0;    l_mx(c_int_ds)(c_float) := l_e0;
    l_mx(c_raw)(c_bin_db) := l_e12;    l_mx(c_date)(c_bin_db) := l_e0;   l_mx(c_time)(c_bin_db) := l_e0;    l_mx(c_timetz)(c_bin_db) := l_e0;  l_mx(c_timeltz)(c_bin_db) := l_e0;  l_mx(c_int_ym)(c_bin_db) := l_e0;   l_mx(c_int_ds)(c_bin_db) := l_e0;
    l_mx(c_raw)(c_vchar) := l_e13;     l_mx(c_date)(c_vchar) := l_e1;    l_mx(c_time)(c_vchar) := l_e1;     l_mx(c_timetz)(c_vchar) := l_e1;   l_mx(c_timeltz)(c_vchar) := l_e1;   l_mx(c_int_ym)(c_vchar) := l_e1;    l_mx(c_int_ds)(c_vchar) := l_e1;
    l_mx(c_raw)(c_char) := l_e13;      l_mx(c_date)(c_char) := l_e1;     l_mx(c_time)(c_char) := l_e1;      l_mx(c_timetz)(c_char) := l_e1;    l_mx(c_timeltz)(c_char) := l_e1;    l_mx(c_int_ym)(c_char) := l_e1;     l_mx(c_int_ds)(c_char) := l_e1;
    l_mx(c_raw)(c_nvchar) := l_e14;    l_mx(c_date)(c_nvchar) := l_e1;   l_mx(c_time)(c_nvchar) := l_e1;    l_mx(c_timetz)(c_nvchar) := l_e1;  l_mx(c_timeltz)(c_nvchar) := l_e1;  l_mx(c_int_ym)(c_nvchar) := l_e1;   l_mx(c_int_ds)(c_nvchar) := l_e1;
    l_mx(c_raw)(c_nchar) := l_e14;     l_mx(c_date)(c_nchar) := l_e1;    l_mx(c_time)(c_nchar) := l_e1;     l_mx(c_timetz)(c_nchar) := l_e1;   l_mx(c_timeltz)(c_nchar) := l_e1;   l_mx(c_int_ym)(c_nchar) := l_e1;    l_mx(c_int_ds)(c_nchar) := l_e1;
    l_mx(c_raw)(c_date) := l_e0;       l_mx(c_date)(c_raw) := l_e0;      l_mx(c_time)(c_raw) := l_e0;       l_mx(c_timetz)(c_raw) := l_e0;     l_mx(c_timeltz)(c_raw) := l_e0;     l_mx(c_int_ym)(c_raw) := l_e0;      l_mx(c_int_ds)(c_raw) := l_e0;
    l_mx(c_raw)(c_time) := l_e0;       l_mx(c_date)(c_time) := l_e1;     l_mx(c_time)(c_date) := l_e1;      l_mx(c_timetz)(c_date) := l_e1;    l_mx(c_timeltz)(c_date) := l_e1;    l_mx(c_int_ym)(c_date) := l_e0;     l_mx(c_int_ds)(c_date) := l_e0;
    l_mx(c_raw)(c_timetz) := l_e0;     l_mx(c_date)(c_timetz) := l_e1;   l_mx(c_time)(c_timetz) := l_e1;    l_mx(c_timetz)(c_time) := l_e1;    l_mx(c_timeltz)(c_time) := l_e1;    l_mx(c_int_ym)(c_time) := l_e0;     l_mx(c_int_ds)(c_time) := l_e0;
    l_mx(c_raw)(c_timeltz) := l_e0;    l_mx(c_date)(c_timeltz) := l_e1;  l_mx(c_time)(c_timeltz) := l_e1;   l_mx(c_timetz)(c_timeltz) := l_e1; l_mx(c_timeltz)(c_timetz) := l_e1;  l_mx(c_int_ym)(c_timetz) := l_e0;   l_mx(c_int_ds)(c_timetz) := l_e0;
    l_mx(c_raw)(c_int_ym) := l_e0;     l_mx(c_date)(c_int_ym) := l_e0;   l_mx(c_time)(c_int_ym) := l_e0;    l_mx(c_timetz)(c_int_ym) := l_e0;  l_mx(c_timeltz)(c_int_ym) := l_e0;  l_mx(c_int_ym)(c_timeltz) := l_e0;  l_mx(c_int_ds)(c_timeltz) := l_e0;
    l_mx(c_raw)(c_int_ds) := l_e0;     l_mx(c_date)(c_int_ds) := l_e0;   l_mx(c_time)(c_int_ds) := l_e0;    l_mx(c_timetz)(c_int_ds) := l_e0;  l_mx(c_timeltz)(c_int_ds) := l_e0;  l_mx(c_int_ym)(c_int_ds) := l_e0;   l_mx(c_int_ds)(c_int_ym) := l_e0;
    l_mx(c_raw)(c_clob) := l_e13;      l_mx(c_date)(c_clob) := l_e1;     l_mx(c_time)(c_clob) := l_e1;      l_mx(c_timetz)(c_clob) := l_e1;    l_mx(c_timeltz)(c_clob) := l_e1;    l_mx(c_int_ym)(c_clob) := l_e19;    l_mx(c_int_ds)(c_clob) := l_e19;
    l_mx(c_raw)(c_nclob) := l_e14;     l_mx(c_date)(c_nclob) := l_e1;    l_mx(c_time)(c_nclob) := l_e1;     l_mx(c_timetz)(c_nclob) := l_e1;   l_mx(c_timeltz)(c_nclob) := l_e1;   l_mx(c_int_ym)(c_nclob) := l_e19;   l_mx(c_int_ds)(c_nclob) := l_e19;
    l_mx(c_raw)(c_blob) := l_e1;       l_mx(c_date)(c_blob) := l_e0;     l_mx(c_time)(c_blob) := l_e0;      l_mx(c_timetz)(c_blob) := l_e0;    l_mx(c_timeltz)(c_blob) := l_e0;    l_mx(c_int_ym)(c_blob) := l_e0;     l_mx(c_int_ds)(c_blob) := l_e0;
    l_mx(c_raw)(c_long) := l_e1;       l_mx(c_date)(c_long) := l_e1;     l_mx(c_time)(c_long) := l_e1;      l_mx(c_timetz)(c_long) := l_e1;    l_mx(c_timeltz)(c_long) := l_e1;    l_mx(c_int_ym)(c_long) := l_e19;    l_mx(c_int_ds)(c_long) := l_e19;
    l_mx(c_raw)(c_longraw) := l_e1;    l_mx(c_date)(c_longraw) := l_e0;  l_mx(c_time)(c_longraw) := l_e0;   l_mx(c_timetz)(c_longraw) := l_e0; l_mx(c_timeltz)(c_longraw) := l_e0; l_mx(c_int_ym)(c_longraw) := l_e0;  l_mx(c_int_ds)(c_longraw) := l_e0;
    l_mx(c_raw)(c_xmltype) := l_e0;    l_mx(c_date)(c_xmltype) := l_e0;  l_mx(c_time)(c_xmltype) := l_e0;   l_mx(c_timetz)(c_xmltype) := l_e0; l_mx(c_timeltz)(c_xmltype) := l_e0; l_mx(c_int_ym)(c_xmltype) := l_e0;  l_mx(c_int_ds)(c_xmltype) := l_e0;
    l_mx(c_raw)(c_urowid) := l_e0;     l_mx(c_date)(c_urowid) := l_e0;   l_mx(c_time)(c_urowid) := l_e0;    l_mx(c_timetz)(c_urowid) := l_e0;  l_mx(c_timeltz)(c_urowid) := l_e0;  l_mx(c_int_ym)(c_urowid) := l_e0;   l_mx(c_int_ds)(c_urowid) := l_e0;

    -- CLOB                            -- NCLOB                          -- BLOB                            -- LONG                            -- LONG RAW                         -- XMLTYPE                          -- UROWID
    l_mx(c_clob)(c_number) := l_e1;    l_mx(c_nclob)(c_number) := l_e1;  l_mx(c_blob)(c_number) := l_e11;   l_mx(c_long)(c_number) := l_e1;    l_mx(c_longraw)(c_number) := l_e11; l_mx(c_xmltype)(c_number) := l_e0;  l_mx(c_urowid)(c_number) := l_e0;
    l_mx(c_clob)(c_float) := l_e1;     l_mx(c_nclob)(c_float) := l_e1;   l_mx(c_blob)(c_float) := l_e11;    l_mx(c_long)(c_float) := l_e1;     l_mx(c_longraw)(c_float) := l_e11;  l_mx(c_xmltype)(c_float) := l_e0;   l_mx(c_urowid)(c_float) := l_e0;
    l_mx(c_clob)(c_bin_db) := l_e1;    l_mx(c_nclob)(c_bin_db) := l_e1;  l_mx(c_blob)(c_bin_db) := l_e12;   l_mx(c_long)(c_bin_db) := l_e1;    l_mx(c_longraw)(c_bin_db) := l_e12; l_mx(c_xmltype)(c_bin_db) := l_e0;  l_mx(c_urowid)(c_bin_db) := l_e0;
    l_mx(c_clob)(c_vchar) := l_e1;     l_mx(c_nclob)(c_vchar) := l_e1;   l_mx(c_blob)(c_vchar) := l_e13;    l_mx(c_long)(c_vchar) := l_e1;     l_mx(c_longraw)(c_vchar) := l_e13;  l_mx(c_xmltype)(c_vchar) := l_e17;  l_mx(c_urowid)(c_vchar) := l_e1;
    l_mx(c_clob)(c_char) := l_e1;      l_mx(c_nclob)(c_char) := l_e1;    l_mx(c_blob)(c_char) := l_e13;     l_mx(c_long)(c_char) := l_e1;      l_mx(c_longraw)(c_char) := l_e13;   l_mx(c_xmltype)(c_char) := l_e17;   l_mx(c_urowid)(c_char) := l_e1;
    l_mx(c_clob)(c_nvchar) := l_e1;    l_mx(c_nclob)(c_nvchar) := l_e1;  l_mx(c_blob)(c_nvchar) := l_e14;   l_mx(c_long)(c_nvchar) := l_e1;    l_mx(c_longraw)(c_nvchar) := l_e14; l_mx(c_xmltype)(c_nvchar) := l_e17; l_mx(c_urowid)(c_nvchar) := l_e1;
    l_mx(c_clob)(c_nchar) := l_e1;     l_mx(c_nclob)(c_nchar) := l_e1;   l_mx(c_blob)(c_nchar) := l_e14;    l_mx(c_long)(c_nchar) := l_e1;     l_mx(c_longraw)(c_nchar) := l_e14;  l_mx(c_xmltype)(c_nchar) := l_e17;  l_mx(c_urowid)(c_nchar) := l_e1;
    l_mx(c_clob)(c_raw) := l_e4;       l_mx(c_nclob)(c_raw) := l_e4;     l_mx(c_blob)(c_raw) := l_e1;       l_mx(c_long)(c_raw) := l_e4;       l_mx(c_longraw)(c_raw) := l_e1;     l_mx(c_xmltype)(c_raw) := l_e0;     l_mx(c_urowid)(c_raw) := l_e0;
    l_mx(c_clob)(c_date) := l_e1;      l_mx(c_nclob)(c_date) := l_e1;    l_mx(c_blob)(c_date) := l_e0;      l_mx(c_long)(c_date) := l_e1;      l_mx(c_longraw)(c_date) := l_e0;    l_mx(c_xmltype)(c_date) := l_e0;    l_mx(c_urowid)(c_date) := l_e0;
    l_mx(c_clob)(c_time) := l_e1;      l_mx(c_nclob)(c_time) := l_e1;    l_mx(c_blob)(c_time) := l_e0;      l_mx(c_long)(c_time) := l_e1;      l_mx(c_longraw)(c_time) := l_e0;    l_mx(c_xmltype)(c_time) := l_e0;    l_mx(c_urowid)(c_time) := l_e0;
    l_mx(c_clob)(c_timetz) := l_e1;    l_mx(c_nclob)(c_timetz) := l_e1;  l_mx(c_blob)(c_timetz) := l_e0;    l_mx(c_long)(c_timetz) := l_e1;    l_mx(c_longraw)(c_timetz) := l_e0;  l_mx(c_xmltype)(c_timetz) := l_e0;  l_mx(c_urowid)(c_timetz) := l_e0;
    l_mx(c_clob)(c_timeltz) := l_e1;   l_mx(c_nclob)(c_timeltz) := l_e1; l_mx(c_blob)(c_timeltz) := l_e0;   l_mx(c_long)(c_timeltz) := l_e1;   l_mx(c_longraw)(c_timeltz) := l_e0; l_mx(c_xmltype)(c_timeltz) := l_e0; l_mx(c_urowid)(c_timeltz) := l_e0;
    l_mx(c_clob)(c_int_ym) := l_e20;   l_mx(c_nclob)(c_int_ym) := l_e20; l_mx(c_blob)(c_int_ym) := l_e0;    l_mx(c_long)(c_int_ym) := l_e5;    l_mx(c_longraw)(c_int_ym) := l_e0;  l_mx(c_xmltype)(c_int_ym) := l_e0;  l_mx(c_urowid)(c_int_ym) := l_e0;
    l_mx(c_clob)(c_int_ds) := l_e21;   l_mx(c_nclob)(c_int_ds) := l_e21; l_mx(c_blob)(c_int_ds) := l_e0;    l_mx(c_long)(c_int_ds) := l_e6;    l_mx(c_longraw)(c_int_ds) := l_e0;  l_mx(c_xmltype)(c_int_ds) := l_e0;  l_mx(c_urowid)(c_int_ds) := l_e0;
    l_mx(c_clob)(c_nclob) := l_e1;     l_mx(c_nclob)(c_clob) := l_e1;    l_mx(c_blob)(c_clob) := l_e0;      l_mx(c_long)(c_clob) := l_e15;     l_mx(c_longraw)(c_clob) := l_e0;    l_mx(c_xmltype)(c_clob) := l_e18;   l_mx(c_urowid)(c_clob) := l_e1;
    l_mx(c_clob)(c_blob) := l_e0;      l_mx(c_nclob)(c_blob) := l_e0;    l_mx(c_blob)(c_nclob) := l_e0;     l_mx(c_long)(c_nclob) := l_e15;    l_mx(c_longraw)(c_nclob) := l_e0;   l_mx(c_xmltype)(c_nclob) := l_e18;  l_mx(c_urowid)(c_nclob) := l_e1;
    l_mx(c_clob)(c_long) := l_e1;      l_mx(c_nclob)(c_long) := l_e1;    l_mx(c_blob)(c_long) := l_e0;      l_mx(c_long)(c_blob) := l_e0;      l_mx(c_longraw)(c_blob) := l_e1;    l_mx(c_xmltype)(c_blob) := l_e0;    l_mx(c_urowid)(c_blob) := l_e0;
    l_mx(c_clob)(c_longraw) := l_e0;   l_mx(c_nclob)(c_longraw) := l_e0; l_mx(c_blob)(c_longraw) := l_e1;   l_mx(c_long)(c_longraw) := l_e4;   l_mx(c_longraw)(c_long) := l_e0;    l_mx(c_xmltype)(c_long) := l_e18;   l_mx(c_urowid)(c_long) := l_e1;
    l_mx(c_clob)(c_xmltype) := l_e7;   l_mx(c_nclob)(c_xmltype) := l_e7; l_mx(c_blob)(c_xmltype) := l_e0;   l_mx(c_long)(c_xmltype) := l_e16;  l_mx(c_longraw)(c_xmltype) := l_e0; l_mx(c_xmltype)(c_longraw) := l_e0; l_mx(c_urowid)(c_longraw) := l_e0;
    l_mx(c_clob)(c_urowid) := l_e1;    l_mx(c_nclob)(c_urowid) := l_e1;  l_mx(c_blob)(c_urowid) := l_e0;    l_mx(c_long)(c_urowid) := l_e1;    l_mx(c_longraw)(c_urowid) := l_e0;  l_mx(c_xmltype)(c_urowid) := l_e0;  l_mx(c_urowid)(c_xmltype) := l_e0;

    RETURN l_mx;
  END get_type_convert_matrix;

  -- return column expression for copying data
  FUNCTION get_type_convert_expression(
    in_source_column_rec IN cort_exec_pkg.gt_column_rec,
    in_target_column_rec IN cort_exec_pkg.gt_column_rec,
    in_alias_name        IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result          VARCHAR2(32767);
    l_source_type     VARCHAR2(30);
    l_target_type     VARCHAR2(30);
  BEGIN
    l_source_type := in_source_column_rec.data_type;
    l_target_type := in_target_column_rec.data_type;
    IF (in_source_column_rec.data_type_owner IS NULL OR in_source_column_rec.data_type = 'XMLTYPE') AND
       (in_target_column_rec.data_type_owner IS NULL OR in_target_column_rec.data_type = 'XMLTYPE')
    THEN
      debug('Convert data type '||l_source_type||' to '||l_target_type||' for column '||in_source_column_rec.column_name);
      IF in_source_column_rec.data_type <> in_target_column_rec.data_type
      THEN
        IF g_type_matrix.EXISTS(l_source_type) AND
           g_type_matrix(l_source_type).EXISTS(l_target_type)
        THEN
          l_result := g_type_matrix(l_source_type)(l_target_type);
          l_result := REPLACE(l_result, '<source_column>', '"'||in_source_column_rec.column_name||'"');
          l_result := REPLACE(l_result, '<alias>', in_alias_name);
        ELSE
          l_result := 'NULL';
        END IF;
      ELSE
        l_result := '"'||in_source_column_rec.column_name||'"';
      END IF;
    ELSE
      IF in_source_column_rec.data_type = in_target_column_rec.data_type AND
         in_source_column_rec.data_type_owner = in_target_column_rec.data_type_owner
      THEN
        debug('Convert data type '||in_source_column_rec.data_type_owner||'.'||l_source_type||' to '||in_target_column_rec.data_type_owner||'.'||l_target_type||' for column '||in_source_column_rec.column_name);
        l_result := '"'||in_source_column_rec.column_name||'"';
      ELSE
        l_result := 'NULL';
      END IF;
    END IF;
    RETURN l_result;
  END get_type_convert_expression;


  -- Returns list of column name for INSERT statement and list of column values for SELECT statement from source table
  PROCEDURE get_column_values_list(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    out_columns_list    OUT NOCOPY CLOB,
    out_values_list     OUT NOCOPY CLOB
  )
  AS
    l_column_indx   PLS_INTEGER;
    l_column_alias  VARCHAR2(100);
  BEGIN
    FOR i IN 1..in_target_table_rec.column_arr.COUNT LOOP
      IF in_target_table_rec.column_arr(i).virtual_column = 'NO' AND
         in_target_table_rec.column_arr(i).hidden_column = 'NO'
      THEN
        debug('Copying data for nonhidden, nonvirtual column '||in_target_table_rec.column_arr(i).column_name);
        out_columns_list := out_columns_list||'"'||in_target_table_rec.column_arr(i).column_name||'",';
        l_column_alias := ' AS "'||in_target_table_rec.column_arr(i).column_name||'",';
        IF in_source_table_rec.column_indx_arr.EXISTS(in_target_table_rec.column_arr(i).column_name) THEN
          -- existing column
          l_column_indx := in_source_table_rec.column_indx_arr(in_target_table_rec.column_arr(i).column_name);
          -- column type was change or force to change data
          IF in_target_table_rec.column_arr(i).cort_value IS NOT NULL AND
             (in_target_table_rec.column_arr(i).cort_value_force OR comp_data_type(in_source_table_rec.column_arr(l_column_indx), in_target_table_rec.column_arr(i)) = 1)
          THEN
            -- update value
            out_values_list := out_values_list||in_target_table_rec.column_arr(i).cort_value||l_column_alias;
          ELSE
            -- convert data type
            out_values_list := out_values_list||get_type_convert_expression(in_source_table_rec.column_arr(l_column_indx), in_target_table_rec.column_arr(i), cort_exec_pkg.g_params.alias.get_value)||l_column_alias;
          END IF;
        ELSIF in_target_table_rec.column_arr(i).cort_value IS NOT NULL
        THEN
          out_values_list := out_values_list||in_target_table_rec.column_arr(i).cort_value||l_column_alias;
        ELSE
          out_values_list := out_values_list||NVL(in_target_table_rec.column_arr(i).data_default,'NULL')||l_column_alias;
        END IF;
      ELSE
        debug('Not copying data for hidden or virtual column '||in_target_table_rec.column_arr(i).column_name);
      END IF;
    END LOOP;
    out_values_list := TRIM(',' FROM out_values_list);
    out_columns_list := TRIM(',' FROM out_columns_list);
  END get_column_values_list;


  -- Returns rename table DDL
  FUNCTION get_rename_table_ddl(
    in_owner      IN VARCHAR2,
    in_from_name  IN VARCHAR2,
    in_to_name    IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'ALTER TABLE "'||in_owner||'"."'||in_from_name||'" RENAME TO "'||in_to_name||'"';
  END get_rename_table_ddl;

  -- Returns rename index DDL
  FUNCTION get_rename_index_ddl(
    in_owner      IN VARCHAR2,
    in_from_name  IN VARCHAR2,
    in_to_name    IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'ALTER INDEX "'||in_owner||'"."'||in_from_name||'" RENAME TO "'||in_to_name||'"';
  END get_rename_index_ddl;

  -- Returns rename constraint DDL
  FUNCTION get_rename_constraint_ddl(
    in_owner      IN VARCHAR2,
    in_table_name IN VARCHAR2,
    in_from_name  IN VARCHAR2,
    in_to_name    IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'ALTER TABLE "'||in_owner||'"."'||in_table_name||'" RENAME CONSTRAINT "'||in_from_name||'" TO "'||in_to_name||'"';
  END get_rename_constraint_ddl;

  -- Returns rename trigger DDL
  FUNCTION get_rename_trigger_ddl(
    in_owner      IN VARCHAR2,
    in_from_name  IN VARCHAR2,
    in_to_name    IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'ALTER TRIGGER "'||in_owner||'"."'||in_from_name||'" RENAME TO "'||in_to_name||'"';
  END get_rename_trigger_ddl;

  -- Returns rename lob segment DDL
  FUNCTION get_rename_lob_ddl(
    in_owner       IN VARCHAR2,
    in_table_name  IN VARCHAR2,
    in_column_name IN VARCHAR2,
    in_to_name     IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'ALTER TABLE "'||in_owner||'"."'||in_table_name||'" MOVE LOB ("'||in_column_name||'") STORE AS "'||in_to_name||'"';
  END get_rename_lob_ddl;

  -- Returns rename varray lob segment DDL
  FUNCTION get_rename_varray_lob_ddl(
    in_owner       IN VARCHAR2,
    in_table_name  IN VARCHAR2,
    in_column_name IN VARCHAR2,
    in_to_name     IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'ALTER TABLE "'||in_owner||'"."'||in_table_name||'" MOVE VARRAY "'||in_column_name||'" STORE AS LOB "'||in_to_name||'"';
  END get_rename_varray_lob_ddl;

  -- rename object
  PROCEDURE rename_object(
    in_rename_mode   IN VARCHAR2,
    io_rename_rec    IN OUT NOCOPY cort_exec_pkg.gt_rename_rec,
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_new_name arrays.gt_name;
  BEGIN
    l_new_name := get_new_name(io_rename_rec, in_rename_mode);
    IF io_rename_rec.current_name <> l_new_name THEN
      CASE io_rename_rec.object_type
      WHEN 'TABLE' THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := get_rename_table_ddl(io_rename_rec.object_owner, io_rename_rec.current_name, l_new_name);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_rename_table_ddl(io_rename_rec.object_owner, l_new_name, io_rename_rec.current_name);
      WHEN 'INDEX' THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := get_rename_index_ddl(io_rename_rec.object_owner, io_rename_rec.current_name, l_new_name);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_rename_index_ddl(io_rename_rec.object_owner, l_new_name, io_rename_rec.current_name);
      WHEN 'CONSTRAINT' THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := get_rename_constraint_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, io_rename_rec.current_name, l_new_name);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_rename_constraint_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, l_new_name, io_rename_rec.current_name);
      WHEN 'REFERENCE' THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := get_rename_constraint_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, io_rename_rec.current_name, l_new_name);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_rename_constraint_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, l_new_name, io_rename_rec.current_name);
      WHEN 'TRIGGER' THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := get_rename_trigger_ddl(io_rename_rec.object_owner, io_rename_rec.current_name, l_new_name);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_rename_trigger_ddl(io_rename_rec.object_owner, l_new_name, io_rename_rec.current_name);
      WHEN 'LOB' THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := get_rename_lob_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, io_rename_rec.lob_column_name, l_new_name);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_rename_lob_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, io_rename_rec.lob_column_name, io_rename_rec.current_name);
      WHEN 'VARRAY' THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := get_rename_varray_lob_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, io_rename_rec.lob_column_name, l_new_name);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_rename_varray_lob_ddl(io_rename_rec.object_owner, io_rename_rec.parent_object_name, io_rename_rec.lob_column_name, io_rename_rec.current_name);
      END CASE;
      io_rename_rec.current_name := l_new_name;
    END IF;
  END rename_object;

  -- compares two structures of sequences.
  FUNCTION comp_sequences(
    in_source_sequence_rec IN cort_exec_pkg.gt_sequence_rec,
    in_target_sequence_rec IN cort_exec_pkg.gt_sequence_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_result := gc_result_nochange;

    IF in_source_sequence_rec.last_number < in_target_sequence_rec.last_number THEN
      RETURN gc_result_recreate;
    END IF;

    -- min_value
    IF comp_value(in_source_sequence_rec.min_value, in_target_sequence_rec.min_value) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_min_value) := get_clause(gc_min_value, in_target_sequence_rec.min_value);
      l_rlbk_clause_indx_arr(gc_min_value) := get_clause(gc_min_value, in_source_sequence_rec.min_value);
    END IF;

    -- max_value
    IF comp_value(in_source_sequence_rec.max_value, in_target_sequence_rec.max_value) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_max_value) := get_clause(gc_max_value, in_target_sequence_rec.max_value);
      l_rlbk_clause_indx_arr(gc_max_value) := get_clause(gc_max_value, in_source_sequence_rec.max_value);
    END IF;

    -- increment_by
    IF comp_value(in_source_sequence_rec.increment_by, in_target_sequence_rec.increment_by) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_increment_by) := get_clause(gc_increment_by, in_target_sequence_rec.increment_by);
      l_rlbk_clause_indx_arr(gc_increment_by) := get_clause(gc_increment_by, in_source_sequence_rec.increment_by);
    END IF;

    -- cycle_flag
    IF comp_value(in_source_sequence_rec.cycle_flag, in_target_sequence_rec.cycle_flag) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_cycle_flag) := get_clause(gc_cycle_flag, in_target_sequence_rec.cycle_flag);
      l_rlbk_clause_indx_arr(gc_cycle_flag) := get_clause(gc_cycle_flag, in_source_sequence_rec.cycle_flag);
    END IF;

    -- order_flag
    IF comp_value(in_source_sequence_rec.order_flag, in_target_sequence_rec.order_flag) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_order_flag) := get_clause(gc_order_flag, in_target_sequence_rec.order_flag);
      l_rlbk_clause_indx_arr(gc_order_flag) := get_clause(gc_order_flag, in_source_sequence_rec.order_flag);
    END IF;

    -- cache_size
    IF comp_value(in_source_sequence_rec.cache_size, in_target_sequence_rec.cache_size) = 1 THEN
      l_result := gc_result_alter;
      l_frwd_clause_indx_arr(gc_cache_size) := get_clause(gc_cache_size, in_target_sequence_rec.cache_size);
      l_rlbk_clause_indx_arr(gc_cache_size) := get_clause(gc_cache_size, in_source_sequence_rec.cache_size);
    END IF;

    l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_min_value)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_max_value)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_increment_by)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_cycle_flag)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_order_flag)||
                     get_clause_by_name(l_frwd_clause_indx_arr, gc_cache_size);


    l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_min_value)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_max_value)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_increment_by)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_cycle_flag)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_order_flag)||
                     get_clause_by_name(l_rlbk_clause_indx_arr, gc_cache_size);

    IF l_result = gc_result_alter THEN
      add_alter_sequence_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_sequence_name => in_source_sequence_rec.sequence_name,
        in_owner         => in_source_sequence_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    RETURN l_result;

  END comp_sequences;

  -- return sequence create statement
  FUNCTION get_sequence_sql(
    in_sequence_rec in cort_exec_pkg.gt_sequence_rec
  )
  RETURN VARCHAR2
  AS
    l_clause VARCHAR2(32767);
  BEGIN
    l_clause := 'CREATE SEQUENCE "'||in_sequence_rec.owner||'"."'||in_sequence_rec.sequence_name||'" START WITH '||in_sequence_rec.last_number||' '||
                get_clause(gc_min_value, in_sequence_rec.min_value)||
                get_clause(gc_max_value, in_sequence_rec.max_value)||
                get_clause(gc_increment_by, in_sequence_rec.increment_by)||
                get_clause(gc_cycle_flag, in_sequence_rec.cycle_flag)||
                get_clause(gc_order_flag, in_sequence_rec.order_flag)||
                get_clause(gc_cache_size, in_sequence_rec.cache_size);

    RETURN l_clause;
  END get_sequence_sql;

  -- compares type attributes.
  FUNCTION comp_type_attributes(
    in_source_type_rec     IN cort_exec_pkg.gt_type_rec,
    in_target_type_rec     IN cort_exec_pkg.gt_type_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_indx                   PLS_INTEGER;
    l_drop_cnt               PLS_INTEGER;
    l_name                   arrays.gt_name;
  BEGIN
    l_result := gc_result_nochange;

    l_drop_cnt := 0;
    l_indx := 1;
    FOR i IN 1..in_source_type_rec.attribute_arr.COUNT LOOP
      l_name := in_source_type_rec.attribute_arr(i).attr_name;
      debug('checking attribute '||l_name);
      IF in_target_type_rec.attribute_ind_arr.EXISTS(l_name) THEN
        debug('attribute '||l_name||' found in new table');
        IF in_target_type_rec.attribute_ind_arr(l_name) = l_indx THEN
          debug('attribute '||l_name||' found in new table on position '||l_indx);
          IF comp_value(in_source_type_rec.attribute_arr(i).attr_name, in_target_type_rec.attribute_arr(l_indx).attr_name) = 1 OR
             comp_value(in_source_type_rec.attribute_arr(i).attr_type_mod, in_target_type_rec.attribute_arr(l_indx).attr_type_mod) = 1 OR
             comp_value(in_source_type_rec.attribute_arr(i).attr_type_owner, in_target_type_rec.attribute_arr(l_indx).attr_type_owner) = 1 OR
             comp_value(in_source_type_rec.attribute_arr(i).attr_type_name, in_target_type_rec.attribute_arr(l_indx).attr_type_name) = 1 OR
             (in_source_type_rec.attribute_arr(i).length > in_target_type_rec.attribute_arr(l_indx).length) OR
             (in_source_type_rec.attribute_arr(i).precision > in_target_type_rec.attribute_arr(l_indx).precision) OR
             (in_source_type_rec.attribute_arr(i).scale > in_target_type_rec.attribute_arr(l_indx).scale)
          THEN
            debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' has been changed');
            RETURN gc_result_recreate;
          ELSE
            debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' has same data type');
            IF in_source_type_rec.attribute_arr(i).length < in_target_type_rec.attribute_arr(l_indx).length THEN
               debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' - length has been increased');
               IF in_source_type_rec.attribute_arr(i).cluster_key OR
                  in_source_type_rec.attribute_arr(i).func_index_key OR
                  in_source_type_rec.attribute_arr(i).domain_index_key
               THEN
                 debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' is part of the key');
                 RETURN gc_result_recreate;
               ELSE
                 debug('Modifying attribute '||in_source_type_rec.attribute_arr(i).attr_name);
                 l_frwd_clause := 'MODIFY ATTRIBUTE '||in_source_type_rec.attribute_arr(i).attr_name||' '||in_source_type_rec.attribute_arr(i).attr_type_name||'('||in_target_type_rec.attribute_arr(l_indx).length||') CASCADE';
                 l_rlbk_clause := null;
                 add_alter_type_stmt(
                   io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
                   io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
                   in_type_name     => in_source_type_rec.type_name,
                   in_owner         => in_source_type_rec.owner,
                   in_frwd_clause   => l_frwd_clause,
                   in_rlbk_clause   => l_rlbk_clause
                 );
                l_result := gc_result_alter;
               END IF;
            END IF;
            IF (in_source_type_rec.attribute_arr(i).precision < in_target_type_rec.attribute_arr(l_indx).precision) OR
               (in_source_type_rec.attribute_arr(i).scale < in_target_type_rec.attribute_arr(l_indx).scale)
            THEN
               debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' - precision/scale has been increased');
               IF in_source_type_rec.attribute_arr(i).cluster_key OR
                  in_source_type_rec.attribute_arr(i).func_index_key OR
                  in_source_type_rec.attribute_arr(i).domain_index_key
               THEN
                 debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' is part of the key');
                 RETURN gc_result_recreate;
               ELSIF in_source_type_rec.attribute_arr(i).precision IS NULL THEN
                 debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' could not be modified');
                 RETURN gc_result_recreate;
               ELSE
                 debug('Modifying attribute '||in_source_type_rec.attribute_arr(i).attr_name);
                 l_frwd_clause := 'MODIFY ATTRIBUTE '||in_source_type_rec.attribute_arr(i).attr_name||' '||in_source_type_rec.attribute_arr(i).attr_type_name||'('||in_target_type_rec.attribute_arr(l_indx).precision||','||in_target_type_rec.attribute_arr(l_indx).scale||') CASCADE';
                 l_rlbk_clause := null;
                 add_alter_type_stmt(
                   io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
                   io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
                   in_type_name     => in_source_type_rec.type_name,
                   in_owner         => in_source_type_rec.owner,
                   in_frwd_clause   => l_frwd_clause,
                   in_rlbk_clause   => l_rlbk_clause
                 );
                 l_result := gc_result_alter;
               END IF;
            END IF;
          END IF;
          debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' fully matched');
          l_indx := l_indx + 1;
        ELSE
          debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' has been moved from position '||i||' to '||l_indx);
          RETURN gc_result_recreate;
        END IF;
      ELSE
        debug('attribute '||l_name||' not found in new table');
        -- drop
        debug('Dropping attribute '||in_source_type_rec.attribute_arr(i).attr_name);
        IF in_source_type_rec.attribute_arr(i).cluster_key OR
           in_source_type_rec.attribute_arr(i).partition_key OR
           in_source_type_rec.attribute_arr(i).iot_primary_key
        THEN
          debug('Attribute '||in_source_type_rec.attribute_arr(i).attr_name||' is part of the key');
          RETURN gc_result_recreate;
        END IF;
        l_frwd_clause := 'DROP ATTRIBUTE '||in_source_type_rec.attribute_arr(i).attr_name||' CASCADE';
        l_rlbk_clause := null;
        add_alter_type_stmt(
          io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
          in_type_name     => in_source_type_rec.type_name,
          in_owner         => in_source_type_rec.owner,
          in_frwd_clause   => l_frwd_clause,
          in_rlbk_clause   => l_rlbk_clause
        );
        l_result := gc_result_alter;
        l_drop_cnt := l_drop_cnt + 1;
      END IF;
    END LOOP;

    -- check that we do not drop ALL attributes
    IF l_drop_cnt = in_source_type_rec.attribute_arr.COUNT AND l_drop_cnt > 0 THEN
      debug('All ('||l_drop_cnt||') attributes are need to be dropped');
      RETURN gc_result_recreate;
    END IF;

    FOR i IN l_indx..in_target_type_rec.attribute_arr.COUNT LOOP
      -- add
      debug('Adding attribute '||in_target_type_rec.attribute_arr(i).attr_name);
      IF in_target_type_rec.attribute_arr(i).length IS NOT NULL THEN
        l_frwd_clause := 'ADD ATTRIBUTE '||in_target_type_rec.attribute_arr(i).attr_name||' '||in_target_type_rec.attribute_arr(i).attr_type_name||'('||in_target_type_rec.attribute_arr(i).length||') CASCADE';
      ELSIF in_target_type_rec.attribute_arr(i).precision IS NOT NULL THEN
        l_frwd_clause := 'ADD ATTRIBUTE '||in_target_type_rec.attribute_arr(i).attr_name||' '||in_target_type_rec.attribute_arr(i).attr_type_name||'('||in_target_type_rec.attribute_arr(i).precision||','||in_target_type_rec.attribute_arr(i).scale||') CASCADE';
      ELSIF in_target_type_rec.attribute_arr(i).scale IS NOT NULL AND in_target_type_rec.attribute_arr(i).precision IS NULL THEN
        l_frwd_clause := 'ADD ATTRIBUTE '||in_target_type_rec.attribute_arr(i).attr_name||' '||in_target_type_rec.attribute_arr(i).attr_type_name||'('||in_target_type_rec.attribute_arr(i).scale||') CASCADE';
      ELSE
        l_frwd_clause := 'ADD ATTRIBUTE '||in_target_type_rec.attribute_arr(i).attr_name||' '||in_target_type_rec.attribute_arr(i).attr_type_name||' CASCADE';
      END IF;

      l_rlbk_clause := 'DROP ATTRIBUTE '||in_target_type_rec.attribute_arr(i).attr_name||' CASCADE';
      add_alter_type_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_type_name     => in_source_type_rec.type_name,
        in_owner         => in_source_type_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
      l_result := gc_result_alter;
    END LOOP;

    RETURN l_result;
  END comp_type_attributes;

  FUNCTION get_method_clause(
    in_method_rec IN cort_exec_pkg.gt_type_method_rec
  )
  RETURN VARCHAR2
  AS
    l_sql     VARCHAR2(32767);
    l_params  VARCHAR2(32767);
    l_start_n PLS_INTEGER;
  BEGIN
    l_sql := null;
    IF in_method_rec.instantiable = 'NO' THEN
      l_sql := l_sql||'NOT INSTANTIABLE ';
    END IF;
    IF in_method_rec.overriding = 'YES' THEN
      l_sql := l_sql||'OVERRIDING ';
    END IF;

    IF in_method_rec.final = 'YES' THEN
      l_sql := l_sql||'FINAL ';
    ELSIF in_method_rec.final = 'NO' THEN
      l_sql := l_sql||'NOT FINAL ';
    END IF;

    IF in_method_rec.constructor = 'YES' THEN
      l_sql := l_sql||'CONSTRUCTOR FUNCTION "'||in_method_rec.method_name||'"';

      l_params := null;
      FOR i IN 2..in_method_rec.parameter_arr.COUNT LOOP
        l_params := l_params ||'
        '|| in_method_rec.parameter_arr(i).param_name||' '||in_method_rec.parameter_arr(i).param_mode||' '||in_method_rec.parameter_arr(i).param_type_mod||' ';
        IF in_method_rec.parameter_arr(i).param_type_owner IS NOT NULL THEN
          l_params := l_params || '"'||in_method_rec.parameter_arr(i).param_type_owner||'".';
        END IF;
        l_params := l_params || '"'||in_method_rec.parameter_arr(i).param_type_name||'",';
      END LOOP;
      IF l_params IS NOT NULL THEN
        l_params := TRIM(',' FROM l_params);
        l_sql := l_sql||'('||l_params||'
        )';
      END IF;

      l_sql := l_sql||' RETURN SELF AS RESULT';
    ELSE
      IF in_method_rec.method_type <> 'PUBLIC' THEN
        l_sql := l_sql||in_method_rec.method_type||' ';
      END IF;

      IF in_method_rec.static = 'YES' THEN
        l_sql := l_sql||'STATIC ';
        l_start_n := 1;
      ELSE
        l_sql := l_sql||'MEMBER ';
        l_start_n := 2;
      END IF;

      IF in_method_rec.result_rec.type_name IS NULL THEN
        l_sql := l_sql||'PROCEDURE ';
      ELSE
        l_sql := l_sql||'FUNCTION ';
      END IF;

      l_sql := l_sql||'"'||in_method_rec.method_name||'"';

      l_params := null;
      FOR i IN l_start_n..in_method_rec.parameter_arr.COUNT LOOP
        l_params := l_params ||'
        '|| in_method_rec.parameter_arr(i).param_name||' '||in_method_rec.parameter_arr(i).param_mode||' '||in_method_rec.parameter_arr(i).param_type_mod||' ';
        IF in_method_rec.parameter_arr(i).param_type_owner IS NOT NULL THEN
          l_params := l_params || '"'||in_method_rec.parameter_arr(i).param_type_owner||'".';
        END IF;
        l_params := l_params || '"'||in_method_rec.parameter_arr(i).param_type_name||'",';
      END LOOP;
      IF l_params IS NOT NULL THEN
        l_params := TRIM(',' FROM l_params);
        l_sql := l_sql||'('||l_params||'
        )';
      END IF;
      IF in_method_rec.result_rec.type_name IS NOT NULL THEN
        l_sql := l_sql||' RETURN '||in_method_rec.result_rec.param_type_mod||' ';
        IF in_method_rec.result_rec.param_type_owner IS NOT NULL THEN
          l_sql := l_sql|| '"'||in_method_rec.result_rec.param_type_owner||'".';
        END IF;
        l_sql := l_sql|| '"'||in_method_rec.result_rec.param_type_name||'"';
      END IF;
    END IF;

    RETURN l_sql;
  END get_method_clause;

  FUNCTION is_same_method(
    in_source_method_rec IN cort_exec_pkg.gt_type_method_rec,
    in_target_method_rec IN cort_exec_pkg.gt_type_method_rec
  )
  RETURN BOOLEAN
  AS
    l_result  BOOLEAN;
  BEGIN
    l_result := comp_value(in_source_method_rec.method_name,in_target_method_rec.method_name) = 0 AND
                comp_value(in_source_method_rec.method_type, in_target_method_rec.method_type) = 0 AND
                comp_value(in_source_method_rec.final, in_target_method_rec.final) = 0 AND
                comp_value(in_source_method_rec.instantiable, in_target_method_rec.instantiable) = 0 AND
                comp_value(in_source_method_rec.overriding, in_target_method_rec.overriding) = 0 AND
                comp_value(in_source_method_rec.inherited, in_target_method_rec.inherited) = 0 AND
                comp_value(in_source_method_rec.static, in_target_method_rec.static) = 0;

    IF NOT l_result THEN
      debug('Method '||in_source_method_rec.method_name||' has been changed');
      RETURN l_result;
    END IF;

    -- comp results
    l_result := l_result AND
                comp_value(in_source_method_rec.result_rec.param_name,in_target_method_rec.result_rec.param_name) = 0 AND
                comp_value(in_source_method_rec.result_rec.param_no,in_target_method_rec.result_rec.param_no) = 0 AND
                comp_value(in_source_method_rec.result_rec.param_mode,in_target_method_rec.result_rec.param_mode) = 0 AND
                comp_value(in_source_method_rec.result_rec.param_type_mod,in_target_method_rec.result_rec.param_type_mod) = 0 AND
                comp_value(in_source_method_rec.result_rec.param_type_owner,in_target_method_rec.result_rec.param_type_owner) = 0 AND
                comp_value(in_source_method_rec.result_rec.param_type_name,in_target_method_rec.result_rec.param_type_name) = 0 AND
                comp_value(in_source_method_rec.result_rec.character_set_name,in_target_method_rec.result_rec.character_set_name) = 0;

    IF l_result THEN
      -- comp params
      IF in_source_method_rec.parameter_arr.COUNT = in_target_method_rec.parameter_arr.COUNT THEN
        FOR i IN 1..in_source_method_rec.parameter_arr.COUNT LOOP
          IF in_source_method_rec.parameter_arr(i).param_name <> 'SELF' THEN
            l_result := l_result AND
                        comp_value(in_source_method_rec.parameter_arr(i).param_name,in_target_method_rec.parameter_arr(i).param_name) = 0 AND
                        comp_value(in_source_method_rec.parameter_arr(i).param_no,in_target_method_rec.parameter_arr(i).param_no) = 0 AND
                        comp_value(in_source_method_rec.parameter_arr(i).param_mode,in_target_method_rec.parameter_arr(i).param_mode) = 0 AND
                        comp_value(in_source_method_rec.parameter_arr(i).param_type_mod,in_target_method_rec.parameter_arr(i).param_type_mod) = 0 AND
                        comp_value(in_source_method_rec.parameter_arr(i).param_type_owner,in_target_method_rec.parameter_arr(i).param_type_owner) = 0 AND
                        comp_value(in_source_method_rec.parameter_arr(i).param_type_name,in_target_method_rec.parameter_arr(i).param_type_name) = 0 AND
                        comp_value(in_source_method_rec.parameter_arr(i).character_set_name,in_target_method_rec.parameter_arr(i).character_set_name) = 0;
            IF NOT l_result THEN
              debug('Method '||in_source_method_rec.method_name||' has different param '||in_source_method_rec.parameter_arr(i).param_name);
              RETURN l_result;
            END IF;
          END IF;
        END LOOP;
      ELSE
        debug('Method '||in_source_method_rec.method_name||' has different number of params');
        l_result := FALSE;
      END IF;
    ELSE
      debug('Method '||in_source_method_rec.method_name||' return type has been changed');
    END IF;

    RETURN l_result;
  END is_same_method;

  FUNCTION comp_type_methods(
    in_source_type_rec     IN cort_exec_pkg.gt_type_rec,
    in_target_type_rec     IN cort_exec_pkg.gt_type_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result          PLS_INTEGER;
    l_method_name     arrays.gt_name;
    l_drop_sql        CLOB;
    l_add_sql         CLOB;
    l_nochange_arr    arrays.gt_int_arr;
    l_indx_arr        arrays.gt_int_arr;
    l_found           BOOLEAN;
  BEGIN
    l_result := gc_result_nochange;
    FOR i IN 1..in_source_type_rec.method_arr.COUNT LOOP
      l_method_name := in_source_type_rec.method_arr(i).method_name;
      debug('checking method '||l_method_name);
      IF in_target_type_rec.method_ind_arr.EXISTS(l_method_name) THEN
        debug('method '||l_method_name||' exists');
        l_indx_arr := in_target_type_rec.method_ind_arr(l_method_name);
        l_found := FALSE;
        FOR j IN 1..l_indx_arr.COUNT LOOP
          l_found := is_same_method(in_source_type_rec.method_arr(i),in_target_type_rec.method_arr(l_indx_arr(j)));
          IF l_found THEN
            l_nochange_arr(l_indx_arr(j)) := i;
            EXIT;
          END IF;
        END LOOP;
        IF NOT l_found THEN
          debug('definition of method '||l_method_name||' was changed');
          l_drop_sql := l_drop_sql||'
        DROP '||get_method_clause(
                  in_method_rec => in_source_type_rec.method_arr(i)
                )||',';
          l_add_sql := l_add_sql||'
        ADD '||get_method_clause(
                 in_method_rec => in_source_type_rec.method_arr(i)
               )||',';
          l_result := gc_result_alter;
        END IF;
      ELSE
        debug('method '||l_method_name||' does not exists');
        l_drop_sql := l_drop_sql||'
        DROP '||get_method_clause(
                  in_method_rec => in_source_type_rec.method_arr(i)
                )||',';
        l_add_sql := l_add_sql||'
        ADD '||get_method_clause(
                 in_method_rec => in_source_type_rec.method_arr(i)
               )||',';
        l_result := gc_result_alter;
      END IF;
    END LOOP;
    IF l_add_sql IS NOT NULL AND l_drop_sql IS NOT NULL THEN
      l_add_sql := TRIM(',' FROM l_add_sql)||' CASCADE';
      l_drop_sql := TRIM(',' FROM l_drop_sql)||' CASCADE';
    END IF;

    add_alter_type_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_type_name     => in_source_type_rec.type_name,
      in_owner         => in_source_type_rec.owner,
      in_frwd_clause   => l_drop_sql,
      in_rlbk_clause   => l_add_sql
    );

    l_add_sql := NULL;
    l_drop_sql := NULL;

    FOR i IN 1..in_target_type_rec.method_arr.COUNT LOOP
      IF NOT l_nochange_arr.EXISTS(i) THEN
        debug('adding method '||in_target_type_rec.method_arr(i).method_name);
        l_add_sql := l_add_sql||'
        ADD '||get_method_clause(
                 in_method_rec => in_target_type_rec.method_arr(i)
               )||',';
        l_drop_sql := l_drop_sql||'
        DROP '||get_method_clause(
                  in_target_type_rec.method_arr(i)
                )||',';
        l_result := gc_result_alter;
      END IF;
    END LOOP;
    IF l_add_sql IS NOT NULL AND l_drop_sql IS NOT NULL THEN
      l_add_sql := TRIM(',' FROM l_add_sql)||' CASCADE';
      l_drop_sql := TRIM(',' FROM l_drop_sql)||' CASCADE';
    END IF;

    add_alter_type_stmt(
      io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
      in_type_name     => in_source_type_rec.type_name,
      in_owner         => in_source_type_rec.owner,
      in_frwd_clause   => l_add_sql,
      in_rlbk_clause   => l_drop_sql
    );
    RETURN l_result;
  END comp_type_methods;

  -- compares two structures of types.
  FUNCTION comp_types(
    in_source_type_rec     IN cort_exec_pkg.gt_type_rec,
    in_target_type_rec     IN cort_exec_pkg.gt_type_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result                 PLS_INTEGER;
    l_comp_result            PLS_INTEGER;
    l_frwd_clause            CLOB;
    l_rlbk_clause            CLOB;
    l_frwd_clause_indx_arr   arrays.gt_xlstr_indx;
    l_rlbk_clause_indx_arr   arrays.gt_xlstr_indx;
  BEGIN
    l_result := gc_result_nochange;

    IF comp_value(in_source_type_rec.incomplete, in_target_type_rec.incomplete) = 1 OR
       comp_value(in_source_type_rec.supertype_owner, in_target_type_rec.supertype_owner) = 1 OR
       comp_value(in_source_type_rec.supertype_name, in_target_type_rec.supertype_name) = 1
    THEN
      debug('Basic compare = '||gc_result_recreate);
      RETURN gc_result_recreate;
    END IF;

    IF comp_value(in_source_type_rec.final, in_target_type_rec.final) = 1 THEN
      IF in_source_type_rec.subtype_dependency THEN
        l_result := gc_result_create;
      ELSE
        l_result := gc_result_alter;
        l_frwd_clause_indx_arr(gc_final) := get_clause(gc_final, in_target_type_rec.final);
        l_rlbk_clause_indx_arr(gc_final) := get_clause(gc_final, in_source_type_rec.final);
      END IF;
    END IF;

    IF comp_value(in_source_type_rec.instantiable, in_target_type_rec.instantiable) = 1 THEN
      IF in_source_type_rec.table_dependency THEN
        l_result := gc_result_create;
      ELSE
        l_result := gc_result_alter;
        l_frwd_clause_indx_arr(gc_instantiable) := get_clause(gc_instantiable, in_target_type_rec.instantiable);
        l_rlbk_clause_indx_arr(gc_instantiable) := get_clause(gc_instantiable, in_source_type_rec.instantiable);
      END IF;
    END IF;

    IF l_result = gc_result_alter THEN
      l_frwd_clause := get_clause_by_name(l_frwd_clause_indx_arr, gc_final)||
                       get_clause_by_name(l_frwd_clause_indx_arr, gc_instantiable)||
                       ' CASCADE INCLUDING TABLE DATA ';


      l_rlbk_clause := get_clause_by_name(l_rlbk_clause_indx_arr, gc_final)||
                       get_clause_by_name(l_rlbk_clause_indx_arr, gc_instantiable)||
                       ' CASCADE INCLUDING TABLE DATA ';
    END IF;

    IF l_result = gc_result_alter THEN
      add_alter_type_stmt(
        io_frwd_stmt_arr => io_frwd_alter_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_alter_stmt_arr,
        in_type_name     => in_source_type_rec.type_name,
        in_owner         => in_source_type_rec.owner,
        in_frwd_clause   => l_frwd_clause,
        in_rlbk_clause   => l_rlbk_clause
      );
    END IF;

    debug('Compare type params = '||l_result);

    l_comp_result := comp_type_attributes(
                       in_source_type_rec     => in_source_type_rec,
                       in_target_type_rec     => in_target_type_rec,
                       io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                       io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                     );
    l_result := GREATEST(l_result,l_comp_result);
    debug('Compare type attributes = '||l_comp_result);
    IF l_result = gc_result_recreate THEN
      RETURN l_result;
    END IF;

    l_comp_result := comp_type_methods(
                       in_source_type_rec     => in_source_type_rec,
                       in_target_type_rec     => in_target_type_rec,
                       io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                       io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                     );
    l_result := GREATEST(l_result,l_comp_result);
    debug('Compare type methods = '||l_comp_result);

    RETURN l_result;

  END comp_types;

  FUNCTION get_create_view_sql(in_view_rec IN cort_exec_pkg.gt_view_rec)
  RETURN CLOB
  AS
    l_sql          CLOB;
    l_column_names CLOB;
  BEGIN
    l_column_names := convert_arr_to_str(in_view_rec.columns_arr, ',', '"');
    l_sql := 'CREATE OR REPLACE VIEW "'||in_view_rec.owner||'"."'||in_view_rec.view_name||'"('||l_column_names||') AS '||in_view_rec.view_text;
    RETURN l_sql;
  END get_create_view_sql;

-- Init
BEGIN
  g_type_matrix := get_type_convert_matrix;
END cort_comp_pkg;
/