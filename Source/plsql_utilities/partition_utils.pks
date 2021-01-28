CREATE OR REPLACE PACKAGE partition_utils
AUTHID CURRENT_USER
AS

/*
PL/SQL Utilities - Partition Utilities

Copyright (C) 2013 Softcraft Ltd - Rustam Kafarov

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
  Description: find table and index (sub)partition by high value; compare partition high values; return DDL for split, merge, add, drop (sub)partitions.
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added more attributes into gt_partition_rec record
  15.00   | Rustam Kafarov    | Added param by_high_value into find_range_subpartitions
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------
*/

  typecode_maxvalue        CONSTANT PLS_INTEGER := -1;
  typecode_default         CONSTANT PLS_INTEGER := -2;
  typecode_number          CONSTANT PLS_INTEGER := dbms_types.typecode_number;
  typecode_varchar2        CONSTANT PLS_INTEGER := dbms_types.typecode_varchar2;
  typecode_date            CONSTANT PLS_INTEGER := dbms_types.typecode_date;
  typecode_timestamp       CONSTANT PLS_INTEGER := dbms_types.typecode_timestamp;
  typecode_interval_ym     CONSTANT PLS_INTEGER := dbms_types.typecode_interval_ym;
  typecode_interval_ds     CONSTANT PLS_INTEGER := dbms_types.typecode_interval_ds;
  typecode_raw             CONSTANT PLS_INTEGER := dbms_types.typecode_raw;


  compare_greater          CONSTANT PLS_INTEGER := 1;
  compare_equal            CONSTANT PLS_INTEGER := 0;
  compare_less             CONSTANT PLS_INTEGER := -1;

  TYPE gt_high_value_rec IS RECORD(
    typecode            PLS_INTEGER,
    number_value        NUMBER,
    varchar2_value      VARCHAR2(32767),
    date_value          DATE,
    timestamp_value     TIMESTAMP(9),
    interval_ym_value   YMINTERVAL_UNCONSTRAINED,
    interval_ds_value   DSINTERVAL_UNCONSTRAINED,
    raw_value           RAW(32767)
  );

  TYPE gt_high_values IS TABLE OF gt_high_value_rec;

  TYPE gt_partition_cur_rec IS RECORD(
    object_type             VARCHAR2(5),
    object_name             arrays.gt_name,
    object_owner            arrays.gt_name,
    partition_level         VARCHAR2(12),
    partition_name          all_tab_partitions.partition_name%TYPE,         
    parent_name             all_tab_partitions.partition_name%TYPE,
    high_value              all_tab_partitions.high_value%TYPE,
    partition_position      all_tab_partitions.partition_position%TYPE,
    tablespace_name         all_tab_partitions.tablespace_name%TYPE,
    num_rows                all_tab_partitions.num_rows%TYPE,
    blocks                  all_tab_partitions.blocks%TYPE,
    sample_size             all_tab_partitions.sample_size%TYPE,
    last_analyzed           all_tab_partitions.last_analyzed%TYPE,
    buffer_pool             all_tab_partitions.buffer_pool%TYPE,
    global_stats            all_tab_partitions.global_stats%TYPE,
    user_stats              all_tab_partitions.user_stats%TYPE,
    empty_blocks            all_tab_partitions.empty_blocks%TYPE,
    avg_space               all_tab_partitions.avg_space%TYPE,
    chain_cnt               all_tab_partitions.chain_cnt%TYPE,
    avg_row_len             all_tab_partitions.avg_row_len%TYPE,
    distinct_keys           all_ind_partitions.distinct_keys%TYPE,
    blevel                  all_ind_partitions.blevel%TYPE,
    avg_leaf_blocks_per_key all_ind_partitions.avg_leaf_blocks_per_key%TYPE,
    avg_data_blocks_per_key all_ind_partitions.avg_data_blocks_per_key%TYPE,
    clustering_factor       all_ind_partitions.clustering_factor%TYPE,
    compression             all_tab_partitions.compression%TYPE,
    compress_for            all_tab_partitions.compress_for%TYPE,           
    interval                all_tab_partitions.interval%TYPE,               
    segment_created         all_tab_partitions.segment_created%TYPE        
  );
  
  TYPE gt_partition_cur_arr IS TABLE OF gt_partition_cur_rec INDEX BY PLS_INTEGER;

  TYPE gt_partition_ref_cur IS REF CURSOR RETURN gt_partition_cur_rec;     

  TYPE gt_partition_rec IS RECORD(
    array_index             PLS_INTEGER, -- index in array
    object_type             VARCHAR2(5),
    object_name             arrays.gt_name,
    object_owner            arrays.gt_name,
    partition_level         VARCHAR2(12),
    partition_name          arrays.gt_name,
    parent_part_name        arrays.gt_name,
    position                PLS_INTEGER,
    high_values             gt_high_values,
    tablespace              arrays.gt_name,
    compression             VARCHAR2(10),
    num_rows                NUMBER,
    blocks                  NUMBER, -- LEAF_BLOCKS for index
    sample_size             NUMBER,
    last_analyzed           DATE,
    buffer_pool             VARCHAR2(7),
    global_stats            VARCHAR2(3),
    user_stats              VARCHAR2(3),
    interval                VARCHAR2(3),
    segment_created         VARCHAR2(4),
    -- table specific attributes
    compress_for            VARCHAR2(30),
    empty_blocks            NUMBER,
    avg_space               NUMBER, 
    chain_cnt               NUMBER, 
    avg_row_len             NUMBER, 
    -- index specific attributes
    distinct_keys           NUMBER,
    blevel                  NUMBER,
    avg_leaf_blocks_per_key NUMBER,
    avg_data_blocks_per_key NUMBER, 
    clustering_factor       NUMBER 
  );

  TYPE gt_partition_arr IS TABLE OF gt_partition_rec INDEX BY PLS_INTEGER;

  -- partition with list of all underline subpartitions
  TYPE gt_partition_subpartitions_rec IS RECORD(
    partition      gt_partition_rec,
    subpartitions  gt_partition_arr
  ); 
  
  TYPE gt_partition_subpartitions_arr IS TABLE OF gt_partition_subpartitions_rec INDEX BY PLS_INTEGER;
  
  g_use_cache BOOLEAN := TRUE;

  FUNCTION get_maxvalue
  RETURN gt_high_value_rec;

  FUNCTION get_default
  RETURN gt_high_value_rec;

  FUNCTION get_number(in_number_value IN NUMBER)
  RETURN gt_high_value_rec;

  FUNCTION get_varchar2(in_varchar2_value IN VARCHAR2)
  RETURN gt_high_value_rec;

  FUNCTION get_date(in_date_value IN DATE)
  RETURN gt_high_value_rec;

  FUNCTION get_timestamp(in_timestamp_value IN TIMESTAMP)
  RETURN gt_high_value_rec;

  FUNCTION get_interval_ym(in_interval_ym_value IN YMINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec;

  FUNCTION get_interval_ds(in_interval_ds_value IN DSINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec;

  FUNCTION get_raw(in_raw_value IN RAW)
  RETURN gt_high_value_rec;

  -- get_value overloaded wrappers
  FUNCTION get_value(in_number_value IN NUMBER)
  RETURN gt_high_value_rec;

  FUNCTION get_value(in_varchar2_value IN VARCHAR2)
  RETURN gt_high_value_rec;

  FUNCTION get_value(in_date_value IN DATE)
  RETURN gt_high_value_rec;

  FUNCTION get_value(in_timestamp_value IN TIMESTAMP)
  RETURN gt_high_value_rec;

  FUNCTION get_value(in_interval_ym_value IN YMINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec;

  FUNCTION get_value(in_interval_ds_value IN DSINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec;

  FUNCTION compare_high_value_rec(
    in_value1       IN gt_high_value_rec,
    in_value2       IN gt_high_value_rec,
    in_compare_type IN VARCHAR2
  )
  RETURN PLS_INTEGER;

  FUNCTION compare_high_values(
    in_values1        IN gt_high_values,
    in_values2        IN gt_high_values,
    in_partition_type IN VARCHAR2, -- RANGE, LIST
    in_compare_type   IN VARCHAR2 DEFAULT 'BY VALUE' -- BY VALUE/BY HIGH_VALUE
  )
  RETURN PLS_INTEGER;

  PROCEDURE parse_high_value_str(
    in_string  IN VARCHAR2,
    out_values OUT NOCOPY gt_high_values
  );

  FUNCTION convert_to_string(in_high_value IN gt_high_value_rec)
  RETURN VARCHAR2;

  FUNCTION convert_to_string(in_high_values IN gt_high_values)
  RETURN VARCHAR2;

  FUNCTION get_partitions(
    io_cursor IN OUT gt_partition_ref_cur
  )
  RETURN gt_partition_arr;

  FUNCTION get_partitions(
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_name_regexp    IN VARCHAR2 DEFAULT NULL
  )
  RETURN gt_partition_arr;

  FUNCTION get_subpartitions(
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_name_regexp    IN VARCHAR2 DEFAULT NULL
  )
  RETURN gt_partition_arr;

  FUNCTION get_subpartitions(
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_partition_name IN VARCHAR2,
    in_name_regexp    IN VARCHAR2 DEFAULT NULL
  )
  RETURN gt_partition_arr;

  -- convert to 1 dimension arrays into one 2-dimension array
  FUNCTION get_partition_subpartitions(
    in_partitions_arr    IN gt_partition_arr, -- all partitions
    in_subpartitions_arr IN gt_partition_arr  -- all subpartitions
  )
  RETURN gt_partition_subpartitions_arr;
  
  FUNCTION get_partition_subpartitions(
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2
  )
  RETURN gt_partition_subpartitions_arr;

  FUNCTION find_range_partition(
    in_partition_arr  IN gt_partition_arr,
    in_values         IN gt_high_values
  )
  RETURN gt_partition_rec;

  FUNCTION find_range_subpartitions(
    in_subpartition_arr  IN gt_partition_arr,
    in_values            IN gt_high_values,
    in_by_high_value     IN BOOLEAN DEFAULT FALSE
  )
  RETURN gt_partition_arr;

  FUNCTION find_range_part_by_highvalue(
    in_partition_arr  IN gt_partition_arr,
    in_values         IN gt_high_values
  )
  RETURN gt_partition_rec;

  FUNCTION find_list_partition(
    in_partition_arr  IN gt_partition_arr,
    in_value          IN gt_high_value_rec
  )
  RETURN gt_partition_rec;

  FUNCTION find_list_part_by_highvalue(
    in_partition_arr  IN gt_partition_arr,
    in_values         IN gt_high_values
  )
  RETURN gt_partition_rec;

  PROCEDURE insert_range_partition_ddl(
    in_partition_arr  IN gt_partition_arr,
    in_high_values    IN gt_high_values,
    in_partition_name IN VARCHAR2,
    in_partition_desc IN VARCHAR2 DEFAULT NULL,
    io_forward_ddl    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl   IN OUT NOCOPY arrays.gt_clob_arr
  );

  PROCEDURE insert_range_subpartition_ddl(
    in_partition_arr     IN gt_partition_arr,
    in_high_values       IN gt_high_values,
    in_partition_name    IN VARCHAR2,
    in_subpartition_name IN VARCHAR2,
    in_subpartition_desc IN VARCHAR2 DEFAULT NULL,
    io_forward_ddl       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl      IN OUT NOCOPY arrays.gt_clob_arr
  );

  PROCEDURE insert_list_partition_ddl(
    in_partition_arr    IN gt_partition_arr,
    in_high_values      IN gt_high_values,
    in_partition_name   IN VARCHAR2,
    in_partition_desc   IN VARCHAR2 DEFAULT NULL,
    in_before_partition IN VARCHAR2 DEFAULT NULL, -- by default add to end
    io_forward_ddl      IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl     IN OUT NOCOPY arrays.gt_clob_arr
  );

  PROCEDURE insert_list_subpartition_ddl(
    in_partition_arr     IN gt_partition_arr,
    in_high_values       IN gt_high_values,
    in_partition_name    IN VARCHAR2,
    in_subpartition_name IN VARCHAR2,
    in_subpartition_desc IN VARCHAR2 DEFAULT NULL,
    in_before_partition  IN VARCHAR2 DEFAULT NULL, -- by default add to end
    io_forward_ddl       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl      IN OUT NOCOPY arrays.gt_clob_arr
  );

END partition_utils;
/