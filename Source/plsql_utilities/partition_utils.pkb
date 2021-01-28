CREATE OR REPLACE PACKAGE BODY partition_utils
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
  Description: Partition utilities - find table and index (sub)partition by high value; compare partition high values; return DDL for split, merge, add, drop (sub)partitions.
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added more attributes into gt_partition_rec record
  15.00   | Rustam Kafarov    | Added param by_high_value into find_range_subpartitions
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------
*/

  TYPE gt_high_values_cache IS TABLE OF gt_high_values INDEX BY VARCHAR2(32767);
  g_high_values_cache       gt_high_values_cache;


  FUNCTION get_maxvalue
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_maxvalue;
    RETURN l_rec;
  END get_maxvalue;

  FUNCTION get_default
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_default;
    RETURN l_rec;
  END get_default;

  FUNCTION get_number(in_number_value IN NUMBER)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_number;
    l_rec.number_value := in_number_value;
    RETURN l_rec;
  END get_number;

  FUNCTION get_varchar2(in_varchar2_value IN VARCHAR2)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_varchar2;
    l_rec.varchar2_value := in_varchar2_value;
    RETURN l_rec;
  END get_varchar2;

  FUNCTION get_date(in_date_value IN DATE)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_date;
    l_rec.date_value := in_date_value;
    RETURN l_rec;
  END get_date;

  FUNCTION get_timestamp(in_timestamp_value IN TIMESTAMP)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_timestamp;
    l_rec.timestamp_value := in_timestamp_value;
    RETURN l_rec;
  END get_timestamp;

  FUNCTION get_interval_ym(in_interval_ym_value IN YMINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_interval_ym;
    l_rec.interval_ym_value := in_interval_ym_value;
    RETURN l_rec;
  END get_interval_ym;

  FUNCTION get_interval_ds(in_interval_ds_value IN DSINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_interval_ds;
    l_rec.interval_ds_value := in_interval_ds_value;
    RETURN l_rec;
  END get_interval_ds;

  FUNCTION get_raw(in_raw_value IN RAW)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_raw;
    l_rec.raw_value := in_raw_value;
    RETURN l_rec;
  END get_raw;

  -- get_value overloaded wrappers
  FUNCTION get_value(in_number_value IN NUMBER)
  RETURN gt_high_value_rec
  AS
  BEGIN
    RETURN get_number(in_number_value);
  END get_value;

  FUNCTION get_value(in_varchar2_value IN VARCHAR2)
  RETURN gt_high_value_rec
  AS
  BEGIN
    RETURN get_varchar2(in_varchar2_value);
  END get_value;

  FUNCTION get_value(in_date_value IN DATE)
  RETURN gt_high_value_rec
  AS
  BEGIN
    RETURN get_date(in_date_value);
  END get_value;

  FUNCTION get_value(in_timestamp_value IN TIMESTAMP)
  RETURN gt_high_value_rec
  AS
  BEGIN
    RETURN get_timestamp(in_timestamp_value);
  END get_value;

  FUNCTION get_value(in_interval_ym_value IN YMINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec
  AS
  BEGIN
    RETURN get_interval_ym(in_interval_ym_value);
  END get_value;

  FUNCTION get_value(in_interval_ds_value IN DSINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec
  AS
  BEGIN
    RETURN get_interval_ds(in_interval_ds_value);
  END get_value;

  FUNCTION get_typecode(in_datatype IN VARCHAR2)
  RETURN PLS_INTEGER
  AS
    l_typecode PLS_INTEGER;
  BEGIN
    CASE
    WHEN in_datatype IN ('NUMBER','FLOAT') THEN
      l_typecode := typecode_number;
    WHEN in_datatype IN ('VARCHAR2','VARCHAR','CHAR','NVARCHAR2','NCHAR') THEN
      l_typecode := typecode_varchar2;
    WHEN in_datatype = 'DATE' THEN
      l_typecode := typecode_date;
    WHEN in_datatype LIKE 'TIMESTAMP(_)' THEN
      l_typecode := typecode_timestamp;
    WHEN in_datatype LIKE 'INTERVAL YEAR(_) TO MONTH' THEN
      l_typecode := typecode_interval_ym;
    WHEN in_datatype LIKE 'INTERVAL DAY(_) TO SECOND(_)' THEN
      l_typecode := typecode_interval_ds;
    WHEN in_datatype = 'RAW' THEN
      l_typecode := typecode_raw;
    ELSE
      l_typecode := NULL;
    END CASE;
    RETURN l_typecode;
  END get_typecode;

  FUNCTION get_typecode(in_dbms_typecode IN PLS_INTEGER)
  RETURN PLS_INTEGER
  AS
    l_typecode PLS_INTEGER;
  BEGIN
    CASE
    WHEN in_dbms_typecode = dbms_types.typecode_number THEN
      l_typecode := typecode_number;
    WHEN in_dbms_typecode IN (dbms_types.typecode_varchar2,
                              dbms_types.typecode_varchar,
                              dbms_types.typecode_char,
                              dbms_types.typecode_nvarchar2,
                              dbms_types.typecode_nchar) THEN
      l_typecode := typecode_varchar2;
    WHEN in_dbms_typecode = dbms_types.typecode_date THEN
      l_typecode := typecode_date;
    WHEN in_dbms_typecode in (180, dbms_types.typecode_timestamp) THEN
      l_typecode := typecode_timestamp;
    WHEN in_dbms_typecode in (182, dbms_types.typecode_interval_ym) THEN
      l_typecode := typecode_interval_ym;
    WHEN in_dbms_typecode in (183, dbms_types.typecode_interval_ds) THEN
      l_typecode := typecode_interval_ds;
    WHEN in_dbms_typecode in (23, dbms_types.typecode_raw) THEN
      l_typecode := typecode_raw;
    ELSE
      l_typecode := NULL;
    END CASE;
    RETURN l_typecode;
  END get_typecode;

  FUNCTION compare_high_value_rec(
    in_value1       IN gt_high_value_rec,
    in_value2       IN gt_high_value_rec,
    in_compare_type IN VARCHAR2
  )
  RETURN PLS_INTEGER
  AS
    l_result PLS_INTEGER;
  BEGIN
    IF in_value1.typecode = in_value2.typecode THEN
      CASE in_value1.typecode
      WHEN typecode_maxvalue THEN
        l_result := compare_equal;
      WHEN typecode_default THEN
        l_result := compare_equal;
      WHEN typecode_number THEN
        IF (in_value1.number_value = in_value2.number_value) OR
           (in_value1.number_value IS NULL AND in_value2.number_value IS NULL) THEN
          l_result := compare_equal;
        ELSIF (in_value1.number_value < in_value2.number_value) OR
              (in_value1.number_value IS NOT NULL AND in_value2.number_value IS NULL) THEN
          l_result := compare_less;
        ELSIF (in_value1.number_value > in_value2.number_value) OR
              (in_value1.number_value IS NULL AND in_value2.number_value IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_varchar2 THEN
        IF (in_value1.varchar2_value = in_value2.varchar2_value) OR
           (in_value1.varchar2_value IS NULL AND in_value2.varchar2_value IS NULL) THEN
          l_result := compare_equal;
        ELSIF (in_value1.varchar2_value < in_value2.varchar2_value) OR
              (in_value1.varchar2_value IS NOT NULL AND in_value2.varchar2_value IS NULL) THEN
          l_result := compare_less;
        ELSIF (in_value1.varchar2_value > in_value2.varchar2_value) OR
              (in_value1.varchar2_value IS NULL AND in_value2.varchar2_value IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_date THEN
        IF (in_value1.date_value = in_value2.date_value) OR
           (in_value1.date_value IS NULL AND in_value2.date_value IS NULL) THEN
          l_result := compare_equal;
        ELSIF (in_value1.date_value < in_value2.date_value) OR
              (in_value1.date_value IS NOT NULL AND in_value2.date_value IS NULL) THEN
          l_result := compare_less;
        ELSIF (in_value1.date_value > in_value2.date_value) OR
              (in_value1.date_value IS NULL AND in_value2.date_value IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_timestamp THEN
        IF (in_value1.timestamp_value = in_value2.timestamp_value) OR
           (in_value1.timestamp_value IS NULL AND in_value2.timestamp_value IS NULL) THEN
          l_result := compare_equal;
        ELSIF (in_value1.timestamp_value < in_value2.timestamp_value) OR
              (in_value1.timestamp_value IS NOT NULL AND in_value2.timestamp_value IS NULL) THEN
          l_result := compare_less;
        ELSIF (in_value1.timestamp_value > in_value2.timestamp_value) OR
              (in_value1.timestamp_value IS NULL AND in_value2.timestamp_value IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_interval_ym THEN
        IF (in_value1.interval_ym_value = in_value2.interval_ym_value) OR
           (in_value1.interval_ym_value IS NULL AND in_value2.interval_ym_value IS NULL) THEN
          l_result := compare_equal;
        ELSIF (in_value1.interval_ym_value < in_value2.interval_ym_value) OR
              (in_value1.interval_ym_value IS NOT NULL AND in_value2.interval_ym_value IS NULL) THEN
          l_result := compare_less;
        ELSIF (in_value1.interval_ym_value > in_value2.interval_ym_value) OR
              (in_value1.interval_ym_value IS NULL AND in_value2.interval_ym_value IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_interval_ds THEN
        IF (in_value1.interval_ds_value = in_value2.interval_ds_value) OR
           (in_value1.interval_ds_value IS NULL AND in_value2.interval_ds_value IS NULL) THEN
          l_result := compare_equal;
        ELSIF (in_value1.interval_ds_value < in_value2.interval_ds_value) OR
              (in_value1.interval_ds_value IS NOT NULL AND in_value2.interval_ds_value IS NULL) THEN
          l_result := compare_less;
        ELSIF (in_value1.interval_ds_value > in_value2.interval_ds_value) OR
              (in_value1.interval_ds_value IS NULL AND in_value2.interval_ds_value IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_raw THEN
        IF (in_value1.raw_value = in_value2.raw_value) OR
           (in_value1.raw_value IS NULL AND in_value2.raw_value IS NULL) THEN
          l_result := compare_equal;
        ELSIF (in_value1.raw_value < in_value2.raw_value) OR
              (in_value1.raw_value IS NOT NULL AND in_value2.raw_value IS NULL) THEN
          l_result := compare_less;
        ELSIF (in_value1.raw_value > in_value2.raw_value) OR
              (in_value1.raw_value IS NULL AND in_value2.raw_value IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      ELSE
        raise_application_error(-20000, 'Unsupported typecode: '||in_value1.typecode);
      END CASE;
    ELSE
      IF in_value1.typecode = typecode_maxvalue THEN
        l_result := compare_greater;
      ELSIF in_value2.typecode = typecode_maxvalue THEN
        l_result := compare_less;
      ELSIF in_value1.typecode = typecode_default THEN
        l_result := compare_greater;
      ELSIF in_value2.typecode = typecode_default THEN
        IF in_compare_type = 'BY VALUE' THEN
          l_result := compare_equal;
        ELSE
          l_result := compare_less;
        END IF;
      ELSE
        raise_application_error(-20000, 'Incosistent data types');
      END IF;
    END IF;
    RETURN l_result;
  END compare_high_value_rec;

  FUNCTION compare_high_values(
    in_values1        IN gt_high_values,
    in_values2        IN gt_high_values,
    in_partition_type IN VARCHAR2, -- RANGE, LIST
    in_compare_type   IN VARCHAR2 DEFAULT 'BY VALUE' -- BY VALUE/BY HIGH_VALUE
  )
  RETURN PLS_INTEGER
  AS
    l_result       PLS_INTEGER;
    l_equal        BOOLEAN;
    l_value        gt_high_value_rec;
  BEGIN
    CASE in_partition_type
    WHEN 'LIST'  THEN
      IF in_compare_type = 'BY HIGH_VALUE' THEN
        IF in_values1.COUNT < in_values2.COUNT THEN
          RETURN compare_less;
        ELSIF in_values1.COUNT > in_values2.COUNT THEN
          RETURN compare_greater;
        END IF;
      END IF;
    WHEN 'RANGE' THEN
      IF in_values1.COUNT < in_values2.COUNT THEN
        RETURN compare_less;
      ELSIF in_values1.COUNT > in_values2.COUNT THEN
        RETURN compare_greater;
      END IF;
    ELSE
      RETURN NULL;
    END CASE;
    FOR i IN 1..in_values1.COUNT LOOP
      CASE in_partition_type
      WHEN 'LIST' THEN
        l_equal := FALSE;
        FOR j IN 1..in_values2.COUNT LOOP
          IF compare_high_value_rec(in_values1(i),in_values2(j),in_compare_type) = compare_equal THEN
            l_equal := TRUE;
            EXIT;
          END IF;
        END LOOP;
        IF l_equal THEN
          l_result := compare_equal;
        ELSE
          l_result := compare_less;
          EXIT;
        END IF;
      WHEN 'RANGE' THEN
        l_result := compare_high_value_rec(in_values1(i),in_values2(i),in_compare_type);
        EXIT WHEN l_result <> compare_equal;
      END CASE;
    END LOOP;
    RETURN l_result;
  END compare_high_values;

  PROCEDURE parse_high_value_str(
    in_string  IN VARCHAR2,
    out_values OUT NOCOPY gt_high_values
  )
  AS
    l_sql               VARCHAR2(32767);
    l_cur               NUMBER;
    l_cnt               INTEGER;
    l_row_cnt           NUMBER;
    l_columns           dbms_sql.desc_tab2;
  BEGIN
    IF g_use_cache AND g_high_values_cache.EXISTS(in_string) THEN
      out_values := g_high_values_cache(in_string);
      RETURN; 
    END IF;
         
    out_values := gt_high_values();
    IF UPPER(in_string) = 'DEFAULT' THEN
      out_values.EXTEND(1);
      out_values(1) := get_default;
    ELSIF UPPER(in_string) = 'MAXVALUE' THEN
      out_values.EXTEND(1);
      out_values(1) := get_maxvalue;
    ELSE
      l_sql := 'SELECT '||in_string||' FROM (select NULL as MAXVALUE FROM dual)';
      l_cur := dbms_sql.open_cursor;
      BEGIN
        dbms_sql.parse(l_cur, l_sql, dbms_sql.native);
        dbms_sql.describe_columns2(l_cur, l_cnt, l_columns);
        out_values.EXTEND(l_cnt);
        FOR i IN 1..l_cnt LOOP
          out_values(i).typecode := get_typecode(in_dbms_typecode => l_columns(i).col_type);
          CASE out_values(i).typecode
          WHEN typecode_number THEN
            dbms_sql.define_column(l_cur, i, out_values(i).number_value);
          WHEN typecode_varchar2 THEN
            dbms_sql.define_column(l_cur, i, out_values(i).varchar2_value, l_columns(i).col_max_len);
          WHEN typecode_date THEN
            dbms_sql.define_column(l_cur, i, out_values(i).date_value);
          WHEN typecode_timestamp THEN
            dbms_sql.define_column(l_cur, i, out_values(i).timestamp_value);
          WHEN typecode_interval_ym THEN
            dbms_sql.define_column(l_cur, i, out_values(i).interval_ym_value);
          WHEN typecode_interval_ds THEN
            dbms_sql.define_column(l_cur, i, out_values(i).interval_ds_value);
          WHEN typecode_raw THEN
            dbms_sql.define_column(l_cur, i, out_values(i).raw_value, l_columns(i).col_max_len);
          ELSE
            raise_application_error(-20000, 'Invalid partition data type. TYPECODE = '||l_columns(i).col_type);
          END CASE;
        END LOOP;
        l_row_cnt := dbms_sql.execute(l_cur);
        l_row_cnt := dbms_sql.fetch_rows(l_cur);
        FOR i IN 1..l_cnt LOOP
          CASE out_values(i).typecode
          WHEN typecode_number THEN
            dbms_sql.column_value(l_cur, i, out_values(i).number_value);
          WHEN typecode_varchar2 THEN
            dbms_sql.column_value(l_cur, i, out_values(i).varchar2_value);
          WHEN typecode_date THEN
            dbms_sql.column_value(l_cur, i, out_values(i).date_value);
          WHEN typecode_timestamp THEN
            dbms_sql.column_value(l_cur, i, out_values(i).timestamp_value);
          WHEN typecode_interval_ym THEN
            dbms_sql.column_value(l_cur, i, out_values(i).interval_ym_value);
          WHEN typecode_interval_ds THEN
            dbms_sql.column_value(l_cur, i, out_values(i).interval_ds_value);
          WHEN typecode_raw THEN
            dbms_sql.column_value(l_cur, i, out_values(i).raw_value);
          ELSE
            raise_application_error(-20000, 'Invalid partition data type. TYPECODE = '||l_columns(i).col_type);
          END CASE;
          IF l_columns(i).col_name = 'MAXVALUE' AND
             out_values(i).typecode = typecode_varchar2 AND
             out_values(i).varchar2_value IS NULL
          THEN
            out_values(i) := get_maxvalue;
          END IF;
        END LOOP;
      EXCEPTION
        WHEN OTHERS THEN
          dbms_sql.close_cursor(l_cur);
          RAISE;
      END;
      dbms_sql.close_cursor(l_cur);
    END IF;                         
    IF g_use_cache THEN
      g_high_values_cache(in_string) := out_values;
    ELSE
      g_high_values_cache.DELETE;
    END IF;     
  END parse_high_value_str;

  FUNCTION convert_to_string(in_high_value IN gt_high_value_rec)
  RETURN VARCHAR2
  AS
    l_str           VARCHAR2(32767);
    l_date_format   VARCHAR2(100) := 'SYYYY-MM-DD HH24:MI:SS';
    l_date_calendar VARCHAR2(100) := 'NLS_CALENDAR=GREGORIAN';
    l_msec_format   VARCHAR2(100) := '.FF9';

    FUNCTION quote(in_value IN VARCHAR2)
    RETURN VARCHAR2
    AS
      l_quote CHAR(1) := CHR(39);
    BEGIN
      RETURN l_quote||in_value||l_quote;
    END quote;

  BEGIN
    CASE in_high_value.typecode
    WHEN typecode_maxvalue THEN
      l_str := 'MAXVALUE';
    WHEN typecode_default THEN
      l_str := 'DEFAULT';
    WHEN typecode_number THEN
      l_str := TO_CHAR(in_high_value.number_value);
    WHEN typecode_varchar2 THEN
      l_str := quote(REPLACE(in_high_value.varchar2_value,'''',''''''));
    WHEN typecode_date THEN
      l_str := 'TO_DATE('||quote(TO_CHAR(in_high_value.date_value,l_date_format,l_date_calendar))||','||quote(l_date_format)||','||quote(l_date_calendar)||')';
    WHEN typecode_timestamp THEN
      l_str := 'TIMESTAMP'||quote(TO_CHAR(in_high_value.timestamp_value,l_date_format||l_msec_format,l_date_calendar));
    WHEN typecode_interval_ym THEN
      l_str := 'INTERVAL'||quote(TO_CHAR(in_high_value.interval_ym_value))||' YEAR(9) TO MONTH';
    WHEN typecode_interval_ds THEN
      l_str := 'INTERVAL'||quote(TO_CHAR(in_high_value.interval_ds_value))||' DAY(9) TO SECOND(9)';
    WHEN typecode_raw THEN
      l_str := quote(in_high_value.raw_value);
    END CASE;
    RETURN l_str;
  END convert_to_string;

  FUNCTION convert_to_string(in_high_values IN gt_high_values)
  RETURN VARCHAR2
  AS
    l_str           VARCHAR2(32767);
  BEGIN
    IF in_high_values IS NOT NULL THEN
      FOR i IN 1..in_high_values.COUNT LOOP
        l_str := l_str || convert_to_string(in_high_values(i));
        IF i < in_high_values.COUNT THEN
          l_str := l_str || ', ';
        END IF;
      END LOOP;
    END IF;
    RETURN l_str;
  END convert_to_string;

  FUNCTION get_object_type(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_object_type VARCHAR2(5);
  BEGIN
    BEGIN
      SELECT object_type
        INTO l_object_type 
        FROM all_objects
       WHERE owner = in_object_owner
         AND object_name = in_object_name
         AND object_type IN ('TABLE','INDEX');
    EXCEPTION          
      WHEN NO_DATA_FOUND THEN
        l_object_type := NULL;
    END;             
    RETURN l_object_type;
  END get_object_type;     
  
  FUNCTION get_partition_rec(
    in_partition_cur_rec IN gt_partition_cur_rec
  )
  RETURN  gt_partition_rec
  AS
    l_result gt_partition_rec; 
  BEGIN
    l_result.object_type := in_partition_cur_rec.object_type;
    l_result.object_name := in_partition_cur_rec.object_name;
    l_result.object_owner := in_partition_cur_rec.object_owner;
    l_result.partition_level := in_partition_cur_rec.partition_level;
    l_result.partition_name := in_partition_cur_rec.partition_name;
    l_result.parent_part_name := in_partition_cur_rec.parent_name;
    l_result.position := in_partition_cur_rec.partition_position;
    l_result.tablespace := in_partition_cur_rec.tablespace_name;
    l_result.num_rows := in_partition_cur_rec.num_rows;
    l_result.blocks := in_partition_cur_rec.blocks;
    l_result.sample_size := in_partition_cur_rec.sample_size;
    l_result.last_analyzed := in_partition_cur_rec.last_analyzed;
    l_result.buffer_pool := in_partition_cur_rec.buffer_pool;
    l_result.global_stats := in_partition_cur_rec.global_stats;
    l_result.user_stats := in_partition_cur_rec.user_stats;
    l_result.empty_blocks := in_partition_cur_rec.empty_blocks;
    l_result.avg_space := in_partition_cur_rec.avg_space;
    l_result.chain_cnt := in_partition_cur_rec.chain_cnt;
    l_result.avg_row_len := in_partition_cur_rec.avg_row_len;
    l_result.distinct_keys := in_partition_cur_rec.distinct_keys;
    l_result.blevel := in_partition_cur_rec.blevel;
    l_result.avg_leaf_blocks_per_key := in_partition_cur_rec.avg_leaf_blocks_per_key;
    l_result.avg_data_blocks_per_key := in_partition_cur_rec.avg_data_blocks_per_key;
    l_result.clustering_factor := in_partition_cur_rec.clustering_factor;
    l_result.compression := in_partition_cur_rec.compression;
    l_result.compress_for := in_partition_cur_rec.compress_for;
    l_result.interval := in_partition_cur_rec.interval;
    l_result.segment_created := in_partition_cur_rec.segment_created;
 
    parse_high_value_str(in_partition_cur_rec.high_value, l_result.high_values);
    RETURN l_result;
  END get_partition_rec;
  
  PROCEDURE assign_to_partition_arr(
    in_partition_cur_arr IN gt_partition_cur_arr,
    out_partition_arr    IN OUT NOCOPY gt_partition_arr
  )
  AS
  BEGIN
    FOR i IN 1..in_partition_cur_arr.COUNT LOOP
      out_partition_arr(i) := get_partition_rec(in_partition_cur_arr(i));
      out_partition_arr(i).array_index := i; 
    END LOOP;
  END assign_to_partition_arr;

  FUNCTION get_partitions(
    io_cursor IN OUT gt_partition_ref_cur
  )
  RETURN gt_partition_arr
  AS
    l_partition_arr               gt_partition_arr;   
    l_partition_cur_arr           gt_partition_cur_arr;   
  BEGIN
    -- cursor should be open
    FETCH io_cursor           
     BULK COLLECT 
     INTO l_partition_cur_arr;
    CLOSE io_cursor;
    
    assign_to_partition_arr(
      in_partition_cur_arr => l_partition_cur_arr,
      out_partition_arr    => l_partition_arr
    );

    RETURN l_partition_arr;
  END get_partitions;

  
  FUNCTION int_get_partitions(
    in_object_name     IN VARCHAR2,
    in_object_owner    IN VARCHAR2,
    in_partition_level IN VARCHAR2, -- partition/subpartition
    in_name_regexp     IN VARCHAR2 DEFAULT NULL,
    in_partition_name  IN VARCHAR2 DEFAULT NULL -- not null for subpartitions
  )
  RETURN gt_partition_arr
  AS
    l_ref_cursor                  gt_partition_ref_cur; 
    l_object_type                 VARCHAR2(5);
    l_partition_arr               gt_partition_arr;   
  BEGIN
    l_object_type := get_object_type(in_object_name, in_object_owner);

    IF l_object_type IS NULL THEN
      RETURN l_partition_arr;  
    END IF;
    
    CASE in_partition_level
    WHEN 'PARTITION' THEN
      CASE l_object_type
      WHEN 'TABLE' THEN
        OPEN l_ref_cursor FOR
        SELECT l_object_type, in_object_name, in_object_owner, in_partition_level,
               partition_name, NULL AS parent_name, high_value, partition_position, tablespace_name,  
               num_rows, blocks, sample_size, last_analyzed, buffer_pool, global_stats, user_stats,
               -- table specific
               empty_blocks, avg_space, chain_cnt, avg_row_len,
               -- index specific  
               NULL AS distinct_keys, NULL AS blevel, NULL AS avg_leaf_blocks_per_key, NULL AS avg_data_blocks_per_key, NULL AS clustering_factor,
               $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
               compression, compress_for, interval, segment_created
               $ELSE
               NULL AS compression, NULL AS compress_for, NULL AS interval, NULL AS segment_created
               $END  
          FROM all_tab_partitions
         WHERE table_name = in_object_name
           AND table_owner = in_object_owner
           AND (in_name_regexp IS NULL OR REGEXP_LIKE(partition_name, in_name_regexp))  
         ORDER BY partition_position;
      WHEN 'INDEX' THEN
        OPEN l_ref_cursor FOR
        SELECT l_object_type, in_object_name, in_object_owner, in_partition_level,
               partition_name, NULL AS parent_name, high_value, partition_position, tablespace_name,  
               num_rows, leaf_blocks, sample_size, last_analyzed, buffer_pool, global_stats, user_stats,
               -- table specific
               NULL AS empty_blocks, NULL AS avg_space, NULL AS chain_cnt, NULL AS avg_row_len,
               -- index specific  
               distinct_keys, blevel, avg_leaf_blocks_per_key, avg_data_blocks_per_key, clustering_factor,
               compression, NULL AS compress_for,  
               $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
               interval, segment_created
               $ELSE
               NULL AS interval, NULL AS segment_created
               $END  
          FROM all_ind_partitions
         WHERE index_name = in_object_name
           AND index_owner = in_object_owner
           AND (in_name_regexp IS NULL OR REGEXP_LIKE(partition_name, in_name_regexp))  
         ORDER BY partition_position;
      END CASE;
    WHEN 'SUBPARTITION' THEN
      CASE l_object_type
      WHEN 'TABLE' THEN
        IF in_partition_name IS NOT NULL THEN
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, in_partition_level,
                 subpartition_name, partition_name, high_value, subpartition_position, tablespace_name,   
                 num_rows, blocks, sample_size, last_analyzed, buffer_pool, global_stats, user_stats,
                 -- table specific
                 empty_blocks, avg_space, chain_cnt, avg_row_len,
                 -- index specific  
                 NULL AS distinct_keys, NULL AS blevel, NULL AS avg_leaf_blocks_per_key, NULL AS avg_data_blocks_per_key, NULL AS clustering_factor,
                 $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
                 compression, compress_for, interval, segment_created
                 $ELSE
                 NULL AS compression, NULL AS compress_for, NULL AS interval, NULL AS segment_created
                 $END  
            FROM all_tab_subpartitions
           WHERE table_name = in_object_name
             AND table_owner = in_object_owner
             AND partition_name = in_partition_name
             AND (in_name_regexp IS NULL OR REGEXP_LIKE(subpartition_name, in_name_regexp))  
           ORDER BY subpartition_position;
        ELSE
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, in_partition_level,
                 sp.subpartition_name, sp.partition_name, sp.high_value, sp.subpartition_position, sp.tablespace_name,  
                 sp.num_rows, sp.blocks, sp.sample_size, sp.last_analyzed, sp.buffer_pool, sp.global_stats, sp.user_stats, 
                 -- table specific
                 sp.empty_blocks, sp.avg_space, sp.chain_cnt, sp.avg_row_len, 
                 -- index specific  
                 NULL AS distinct_keys, NULL AS blevel, NULL AS avg_leaf_blocks_per_key, NULL AS avg_data_blocks_per_key, NULL AS clustering_factor,
                 $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
                 sp.compression, sp.compress_for, sp.interval, sp.segment_created
                 $ELSE
                 NULL as compression, NULL as compress_for, NULL as interval, NULL as segment_created
                 $END  
            FROM all_tab_subpartitions sp
           INNER JOIN all_tab_partitions p
              ON p.table_name = sp.table_name
             AND p.table_owner = sp.table_owner
             AND p.partition_name = sp.partition_name
           WHERE sp.table_name = in_object_name
             AND sp.table_owner = in_object_owner
             AND (in_name_regexp IS NULL OR REGEXP_LIKE(subpartition_name, in_name_regexp))  
           ORDER BY p.partition_position, sp.subpartition_position;
        END IF;
      WHEN 'INDEX' THEN
        IF in_partition_name IS NOT NULL THEN
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, in_partition_level,
                 partition_name, subpartition_name, high_value, subpartition_position, tablespace_name, 
                 num_rows, leaf_blocks, sample_size, last_analyzed, buffer_pool, global_stats, user_stats,
                 -- table specific
                 NULL AS empty_blocks, NULL AS avg_space, NULL AS chain_cnt, NULL AS avg_row_len,
                 -- index specific  
                 distinct_keys, blevel, avg_leaf_blocks_per_key, avg_data_blocks_per_key, clustering_factor,
                 compression, NULL AS compress_for,  
                 $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
                 interval, segment_created
                 $ELSE
                 NULL AS interval, NULL AS segment_created
                 $END  
            FROM all_ind_subpartitions
           WHERE index_name = in_object_name
             AND index_owner = in_object_owner
             AND partition_name = in_partition_name
             AND (in_name_regexp IS NULL OR REGEXP_LIKE(subpartition_name, in_name_regexp))  
           ORDER BY subpartition_position;
        ELSE
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, in_partition_level,
                 sp.subpartition_name, sp.partition_name, sp.high_value, sp.subpartition_position, sp.tablespace_name,  
                 sp.num_rows, sp.leaf_blocks, sp.sample_size, sp.last_analyzed, sp.buffer_pool, sp.global_stats, sp.user_stats,
                 -- table specific
                 NULL AS empty_blocks, NULL AS avg_space, NULL AS chain_cnt, NULL AS avg_row_len,
                 -- index specific  
                 sp.distinct_keys, sp.blevel, sp.avg_leaf_blocks_per_key, sp.avg_data_blocks_per_key, sp.clustering_factor,
                 sp.compression, NULL AS compress_for,  
                 $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
                 sp.interval, sp.segment_created
                 $ELSE
                 NULL AS interval, NULL AS segment_created
                 $END  
            FROM all_ind_subpartitions sp
           INNER JOIN all_ind_partitions p
              ON p.index_name = sp.index_name
             AND p.index_owner = sp.index_owner
             AND p.partition_name = sp.partition_name
           WHERE sp.index_name = in_object_name
             AND sp.index_owner = in_object_owner
             AND (in_name_regexp IS NULL OR REGEXP_LIKE(subpartition_name, in_name_regexp))  
           ORDER BY p.partition_position, sp.subpartition_position;
        END IF;      
      END CASE;
    END CASE;   
    
    RETURN get_partitions(l_ref_cursor);
    
  END int_get_partitions;    
  
  FUNCTION get_partitions(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_name_regexp  IN VARCHAR2 DEFAULT NULL
  )
  RETURN gt_partition_arr
  AS       
  BEGIN
    RETURN int_get_partitions(in_object_name, in_object_owner, 'PARTITION', in_name_regexp);
  END get_partitions;

  FUNCTION get_subpartitions(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_name_regexp  IN VARCHAR2 DEFAULT NULL
  )
  RETURN gt_partition_arr
  AS
  BEGIN
    RETURN int_get_partitions(in_object_name, in_object_owner, 'SUBPARTITION', in_name_regexp);
  END get_subpartitions;

  FUNCTION get_subpartitions(
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_partition_name IN VARCHAR2,
    in_name_regexp    IN VARCHAR2 DEFAULT NULL
  )
  RETURN gt_partition_arr
  AS
    l_empty_arr gt_partition_arr;
  BEGIN
    IF in_partition_name IS NOT NULL THEN
      RETURN int_get_partitions(in_object_name, in_object_owner, 'SUBPARTITION', in_name_regexp, in_partition_name);
    ELSE
      RETURN l_empty_arr;
    END IF;
  END get_subpartitions;

  -- convert to 1 dimension arrays into one 2-dimension array
  FUNCTION get_partition_subpartitions(
    in_partitions_arr    IN gt_partition_arr, -- all partitions
    in_subpartitions_arr IN gt_partition_arr  -- all subpartitions
  )
  RETURN gt_partition_subpartitions_arr
  AS
    l_result gt_partition_subpartitions_arr;
    l_last   PLS_INTEGER;   
    l_indx   PLS_INTEGER;
  BEGIN      
    l_last := 1;
    FOR i IN 1..in_partitions_arr.COUNT LOOP
      l_result(i).partition := in_partitions_arr(i);
      -- loop for subpartitions
      l_indx := 1;
      FOR j IN l_last..in_subpartitions_arr.COUNT LOOP
        IF in_subpartitions_arr(j).parent_part_name = in_partitions_arr(i).partition_name THEN
          l_result(i).subpartitions(l_indx) := in_subpartitions_arr(j);
          l_indx := l_indx + 1;
        ELSE
          l_last := j;
          EXIT;
        END IF;  
      END LOOP;
    END LOOP;
    RETURN l_result;
  END get_partition_subpartitions;
  
  FUNCTION get_partition_subpartitions(
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2
  )
  RETURN gt_partition_subpartitions_arr
  AS       
    l_partitions    gt_partition_arr;
    l_subpartitions gt_partition_arr;
  BEGIN
    l_partitions := int_get_partitions(in_object_name, in_object_owner, 'PARTITION');
    l_subpartitions := int_get_partitions(in_object_name, in_object_owner, 'SUBPARTITION');
    RETURN get_partition_subpartitions(
             in_partitions_arr    => l_partitions,
             in_subpartitions_arr => l_subpartitions
           );
  END get_partition_subpartitions;

  FUNCTION get_partition_by_name(
    in_partition_arr  IN gt_partition_arr,
    in_partition_name IN VARCHAR2
  )
  RETURN gt_partition_rec
  AS
  BEGIN
    FOR i IN 1..in_partition_arr.COUNT LOOP
      IF in_partition_arr(i).partition_name = in_partition_name THEN
        RETURN in_partition_arr(i);
      END IF;
    END LOOP;
    RETURN NULL;
  END get_partition_by_name;

  FUNCTION int_find_partition(
    in_partition_arr  IN gt_partition_arr,
    in_values         IN gt_high_values,
    in_partition_type IN VARCHAR2, -- LIST, RANGE
    in_search_mode    IN VARCHAR2
  )
  RETURN gt_partition_rec
  AS
    l_comp_result  PLS_INTEGER;
  BEGIN
    FOR i IN 1..in_partition_arr.COUNT LOOP
      l_comp_result := compare_high_values(
                         in_values1        => in_values,
                         in_values2        => in_partition_arr(i).high_values,
                         in_partition_type => in_partition_type
                       );

      CASE in_partition_type
      WHEN 'RANGE' THEN
        IF (l_comp_result = compare_less  AND in_search_mode = 'BY VALUE') OR
           (l_comp_result = compare_equal AND in_search_mode = 'BY HIGHVALUE') THEN
          RETURN in_partition_arr(i);
        END IF;
      WHEN 'LIST' THEN
        IF (l_comp_result = compare_equal AND in_search_mode = 'BY VALUE') OR
           (l_comp_result = compare_equal AND in_search_mode = 'BY HIGHVALUE' AND
            in_values.EXISTS(1) AND in_partition_arr(i).high_values.EXISTS(1) AND
            in_values(1).typecode = in_partition_arr(i).high_values(1).typecode) THEN
          RETURN in_partition_arr(i);
        END IF;
      END CASE;
    END LOOP;
    RETURN NULL;
  END int_find_partition;

  FUNCTION find_range_partition(
    in_partition_arr  IN gt_partition_arr,
    in_values         IN gt_high_values
  )
  RETURN gt_partition_rec
  AS
  BEGIN
    IF in_values IS NULL THEN
      RETURN NULL;
    END IF;
    RETURN int_find_partition(
             in_partition_arr  => in_partition_arr,
             in_values         => in_values,
             in_partition_type => 'RANGE',
             in_search_mode    => 'BY VALUE'
           );
  END find_range_partition;

  FUNCTION find_range_subpartitions(
    in_subpartition_arr  IN gt_partition_arr,
    in_values            IN gt_high_values,
    in_by_high_value     IN BOOLEAN DEFAULT FALSE
  )
  RETURN gt_partition_arr
  AS  
    l_result           gt_partition_arr;                  
    l_subpartition_arr gt_partition_arr;
    l_partition_name   arrays.gt_name;
    l_part_rec         gt_partition_rec; 
    l_last_index       PLS_INTEGER;
  BEGIN         
    l_last_index := 1;
    LOOP    
      l_subpartition_arr.DELETE;
      -- check if original array has any elements 
      IF l_last_index IS NOT NULL AND in_subpartition_arr.EXISTS(l_last_index) THEN  
        -- get parent partition name of the first available subpartition
        l_partition_name := in_subpartition_arr(l_last_index).parent_part_name;
        -- looping all subpartitions
        FOR i IN l_last_index..in_subpartition_arr.LAST loop
          -- reset last index to NULL so when we reach the end of main array next iteration will interrupt main loop
          l_last_index := NULL;
          -- and finding all subpartitions with the same parent partition anme
          IF l_partition_name = in_subpartition_arr(i).parent_part_name THEN
            -- add them into dedicated to parent partition array of subpartitions
            l_subpartition_arr(l_subpartition_arr.COUNT+1) := in_subpartition_arr(i);
          ELSE   
            -- when we find first subpartition belonging to another partition we interrupt FOR LOOP 
            l_last_index := i;
            EXIT;
          END IF;   
        END LOOP;
        
        -- find subpartition in prepared dedicated array 
        l_part_rec :=    
          int_find_partition(
            in_partition_arr  => l_subpartition_arr,
            in_values         => in_values,
            in_partition_type => 'RANGE',
            in_search_mode    => CASE WHEN in_by_high_value THEN 'BY HIGHVALUE' ELSE 'BY VALUE' END
          );
        
        -- if supartition found then add it to result array      
        IF l_part_rec.partition_name IS NOT NULL THEN
          l_result(l_result.COUNT + 1) := l_part_rec;
        END IF;  
        
      ELSE
        -- no subpartitions to process
        -- exit the loop
        EXIT;
      END IF;
    END LOOP;     
                
    RETURN l_result;
  END find_range_subpartitions;

  FUNCTION find_range_part_by_highvalue(
    in_partition_arr  IN gt_partition_arr,
    in_values         IN gt_high_values
  )
  RETURN gt_partition_rec
  AS
  BEGIN
    IF in_values IS NULL THEN
      RETURN NULL;
    END IF;
    RETURN int_find_partition(
             in_partition_arr  => in_partition_arr,
             in_values         => in_values,
             in_partition_type => 'RANGE',
             in_search_mode    => 'BY HIGHVALUE'
           );
  END find_range_part_by_highvalue;

  FUNCTION find_list_partition(
    in_partition_arr  IN gt_partition_arr,
    in_value          IN gt_high_value_rec
  )
  RETURN gt_partition_rec
  AS
  BEGIN
    RETURN int_find_partition(
             in_partition_arr  => in_partition_arr,
             in_values         => gt_high_values(in_value),
             in_partition_type => 'LIST',
             in_search_mode    => 'BY VALUE'
           );
  END find_list_partition;

  FUNCTION find_list_part_by_highvalue(
    in_partition_arr  IN gt_partition_arr,
    in_values         IN gt_high_values
  )
  RETURN gt_partition_rec
  AS
  BEGIN
    RETURN int_find_partition(
             in_partition_arr  => in_partition_arr,
             in_values         => in_values,
             in_partition_type => 'LIST',
             in_search_mode    => 'BY HIGHVALUE'
           );
  END find_list_part_by_highvalue;


  FUNCTION get_range_split_clause(
    in_split_values         IN gt_high_values,
    in_split_partition_name IN VARCHAR2,
    in_new_partition_name   IN VARCHAR2 DEFAULT NULL,
    in_partition_level      IN VARCHAR2 DEFAULT 'PARTITION',
    in_partition_desc       IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_part_name VARCHAR2(32);
  BEGIN
    IF in_new_partition_name IS NOT NULL THEN
      l_part_name := '"'||in_new_partition_name||'"';
    END IF;
    RETURN 'SPLIT '||in_partition_level||' "'||in_split_partition_name||'" AT ('||
            convert_to_string(in_split_values)||') INTO ('||in_partition_level||' '||l_part_name||' '||
            in_partition_desc||', '||in_partition_level||' "'||in_split_partition_name||'")';
  END get_range_split_clause;

  FUNCTION get_list_split_clause(
    in_split_values         IN gt_high_values,
    in_split_partition_name IN VARCHAR2,
    in_new_partition_name   IN VARCHAR2 DEFAULT NULL,
    in_partition_level      IN VARCHAR2 DEFAULT 'PARTITION',
    in_partition_desc       IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_part_name VARCHAR2(32);
  BEGIN
    IF in_new_partition_name IS NOT NULL THEN
      l_part_name := '"'||in_new_partition_name||'"';
    END IF;
    RETURN 'SPLIT '||in_partition_level||' "'||in_split_partition_name||'" VALUES ('||
            convert_to_string(in_split_values)||') INTO ('||in_partition_level||' '||l_part_name||' '||
            in_partition_desc||', '||in_partition_level||' "'||in_split_partition_name||'")';
  END get_list_split_clause;

  FUNCTION get_add_range_clause(
    in_values           IN gt_high_values,
    in_partition_name   IN VARCHAR2 DEFAULT NULL,
    in_parent_part_name IN VARCHAR2 DEFAULT NULL,
    in_partition_level  IN VARCHAR2 DEFAULT 'PARTITION',
    in_partition_desc   IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_part_name VARCHAR2(32);
    l_ddl       VARCHAR2(32767);
  BEGIN
    IF in_partition_level = 'SUBPARTITION' THEN
      l_ddl := 'MODIFY PARTITION "'||in_parent_part_name||'" ';
    END IF;
    IF in_partition_name IS NOT NULL THEN
      l_part_name := '"'||in_partition_name||'"';
    END IF;
    l_ddl := l_ddl||'ADD '||in_partition_level||' '||l_part_name||' VALUES LESS THAN ('||
           convert_to_string(in_values)||') '||in_partition_desc;
    RETURN l_ddl;
  END get_add_range_clause;

  FUNCTION get_add_list_clause(
    in_values           IN gt_high_values,
    in_partition_name   IN VARCHAR2 DEFAULT NULL,
    in_parent_part_name IN VARCHAR2 DEFAULT NULL,
    in_partition_level  IN VARCHAR2 DEFAULT 'PARTITION',
    in_partition_desc   IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_part_name VARCHAR2(32);
    l_ddl       VARCHAR2(32767);
  BEGIN
    IF in_partition_level = 'SUBPARTITION' THEN
      l_ddl := 'MODIFY PARTITION "'||in_parent_part_name||'" ';
    END IF;
    IF in_partition_name IS NOT NULL THEN
      l_part_name := '"'||in_partition_name||'"';
    END IF;
    l_ddl := l_ddl||'ADD '||in_partition_level||' '||l_part_name||' VALUES ('||
             convert_to_string(in_values)||') '||in_partition_desc;
    RETURN l_ddl;
  END get_add_list_clause;

  FUNCTION get_add_values_clause(
    in_values            IN gt_high_values,
    in_partition_name    IN VARCHAR2,
    in_partition_level   IN VARCHAR2 DEFAULT 'PARTITION'
  )
  RETURN VARCHAR2
  AS
    l_ddl VARCHAR2(32767);
  BEGIN
    RETURN 'MODIFY '||in_partition_level||' "'||in_partition_name||'" ADD VALUES ('||
           convert_to_string(in_values)||')';
  END get_add_values_clause;

  FUNCTION get_drop_values_clause(
    in_values           IN gt_high_values,
    in_partition_name   IN VARCHAR2,
    in_partition_level  IN VARCHAR2 DEFAULT 'PARTITION'
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'MODIFY '||in_partition_level||' "'||in_partition_name||'" DROP VALUES ('||
           convert_to_string(in_values)||')';
  END get_drop_values_clause;

  FUNCTION get_drop_clause(
    in_partition_name   IN VARCHAR2,
    in_partition_level  IN VARCHAR2 DEFAULT 'PARTITION'
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN 'DROP '||in_partition_level||' "'||in_partition_name||'"';
  END get_drop_clause;

  PROCEDURE int_insert_range_partition_ddl(
    in_partition_arr     IN gt_partition_arr,
    in_high_values       IN gt_high_values,
    in_partition_name    IN VARCHAR2,
    in_partition_level   IN VARCHAR2,
    in_parent_partition  IN VARCHAR2,
    in_partition_desc    IN VARCHAR2 DEFAULT NULL,
    io_forward_ddl       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl      IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_part_rec            gt_partition_rec;
    l_ddl                 VARCHAR2(32767);
    l_alter_clause        VARCHAR2(1000);
  BEGIN
    IF in_partition_arr.COUNT = 0 THEN
      RETURN;
    END IF;
    IF in_partition_arr(1).object_type <> 'TABLE' THEN
      RAISE_APPLICATION_ERROR(-20000,'Unsupported opeation for object type '||in_partition_arr(1).object_type);
    END IF;

    l_alter_clause := 'ALTER TABLE "'||in_partition_arr(1).object_owner||'"."'||in_partition_arr(1).object_name||'" ';

    l_part_rec := find_range_part_by_highvalue(
                    in_partition_arr => in_partition_arr,
                    in_values        => in_high_values
                  );
    IF l_part_rec.partition_name IS NULL THEN
      l_part_rec := find_range_partition(
                      in_partition_arr => in_partition_arr,
                      in_values        => in_high_values
                    );
      IF l_part_rec.partition_name IS NOT NULL THEN
        -- split found partition
        l_ddl := l_alter_clause||
                 get_range_split_clause(
                   in_split_values         => in_high_values,
                   in_split_partition_name => l_part_rec.partition_name,
                   in_new_partition_name   => in_partition_name,
                   in_partition_level      => in_partition_level,
                   in_partition_desc       => in_partition_desc
                 );
      ELSE
        -- add partition
        l_ddl := l_alter_clause||
                 get_add_range_clause(
                   in_values               => in_high_values,
                   in_partition_name       => in_partition_name,
                   in_parent_part_name     => in_parent_partition,
                   in_partition_level      => in_partition_level,
                   in_partition_desc       => in_partition_desc
                 );
      END IF;
      io_forward_ddl(io_forward_ddl.COUNT+1) := l_ddl;
      l_ddl := l_alter_clause||
               get_drop_clause(
                 in_partition_name   => in_partition_name,
                 in_partition_level  => in_partition_level
               );
      io_rollback_ddl(io_rollback_ddl.COUNT+1) := l_ddl;
    END IF;
  END int_insert_range_partition_ddl;

  PROCEDURE int_insert_list_partition_ddl(
    in_partition_arr     IN gt_partition_arr,
    in_high_values       IN gt_high_values,
    in_partition_name    IN VARCHAR2,
    in_parent_partition  IN VARCHAR2,
    in_partition_level   IN VARCHAR2,
    in_partition_desc    IN VARCHAR2 DEFAULT NULL,
    in_before_partition  IN VARCHAR2 DEFAULT NULL,
    io_forward_ddl       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl      IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_part_rec            gt_partition_rec;
    l_found               BOOLEAN;
    l_found_part_name     arrays.gt_name;
    l_add_values          gt_high_values;
    l_default_partition   gt_partition_rec;
    l_before_partition    arrays.gt_name;
    l_ddl                 VARCHAR2(32767);
    l_alter_clause        VARCHAR2(1000);
    l_default             BOOLEAN;
  BEGIN
    IF in_partition_arr.COUNT = 0 THEN
      RETURN;
    END IF;
    IF in_partition_arr(1).object_type <> 'TABLE' THEN
      RAISE_APPLICATION_ERROR(-20000,'Unsupported opeation for object type '||in_partition_arr(1).object_type);
    END IF;
    
    l_alter_clause := 'ALTER TABLE "'||in_partition_arr(1).object_owner||'"."'||in_partition_arr(1).object_name||'" ';

    l_add_values := gt_high_values();
    l_found_part_name := NULL;
    l_found := FALSE;
    FOR i IN 1..in_high_values.COUNT LOOP
      l_part_rec := find_list_part_by_highvalue(
                      in_partition_arr => in_partition_arr,
                      in_values        => gt_high_values(in_high_values(i))
                    );
      IF l_part_rec.partition_name IS NOT NULL THEN
        IF l_found_part_name <> l_part_rec.partition_name THEN
          l_found := FALSE;
        ELSE
          l_found_part_name := l_part_rec.partition_name;
          l_found := TRUE;
        END IF;
      ELSE
        l_add_values.EXTEND(1);
        l_add_values(l_add_values.COUNT) := in_high_values(i);
      END IF;
    END LOOP;

    IF NOT l_found AND l_found_part_name IS NULL THEN
      l_default_partition := find_list_part_by_highvalue(
                               in_partition_arr => in_partition_arr,
                               in_values        => gt_high_values(get_default)
                             );
      IF in_before_partition IS NULL THEN
        l_before_partition := l_default_partition.partition_name;
        l_default := TRUE;
      ELSE
        l_before_partition := in_before_partition;
        l_default := nvl(in_before_partition = l_default_partition.partition_name, FALSE);
      END IF;

      IF l_before_partition IS NULL THEN
        -- add to end partition
        l_ddl := l_alter_clause||
                 get_add_list_clause(
                   in_values               => in_high_values,
                   in_partition_name       => in_partition_name,
                   in_parent_part_name     => in_parent_partition,
                   in_partition_level      => in_partition_level,
                   in_partition_desc       => in_partition_desc
                 );
        io_forward_ddl(io_forward_ddl.COUNT+1) := l_ddl;
        l_ddl := l_alter_clause||
                 get_drop_clause(
                   in_partition_name   => in_partition_name,
                   in_partition_level  => in_partition_level
                 );
        io_rollback_ddl(io_rollback_ddl.COUNT+1) := l_ddl;
      ELSE
        IF NOT l_default THEN
          l_ddl := l_alter_clause||
                   get_add_values_clause(
                     in_values               => l_add_values,
                     in_partition_name       => in_before_partition,
                     in_partition_level      => in_partition_level
                   );
          io_forward_ddl(io_forward_ddl.COUNT+1) := l_ddl;
          io_rollback_ddl(io_rollback_ddl.COUNT+1) := NULL;
        END IF;
        l_ddl := l_alter_clause||
                 get_list_split_clause(
                   in_split_values         => l_add_values,
                   in_split_partition_name => in_before_partition,
                   in_new_partition_name   => in_partition_name,
                   in_partition_level      => in_partition_level,
                   in_partition_desc       => in_partition_desc
                 );
        io_forward_ddl(io_forward_ddl.COUNT+1) := l_ddl;

        l_ddl := l_alter_clause||
                 get_drop_clause(
                   in_partition_name   => in_partition_name,
                   in_partition_level  => in_partition_level
                 );
        io_rollback_ddl(io_rollback_ddl.COUNT+1) := l_ddl;
      END IF;
    ELSIF l_found AND l_found_part_name IS NOT NULL THEN
      l_ddl := l_alter_clause||
               get_add_values_clause(
                 in_values               => l_add_values,
                 in_partition_name       => l_found_part_name,
                 in_partition_level      => in_partition_level
               );
      io_forward_ddl(io_forward_ddl.COUNT+1) := l_ddl;
      l_ddl := l_alter_clause||
               get_drop_values_clause(
                 in_values               => l_add_values,
                 in_partition_name       => l_found_part_name,
                 in_partition_level      => in_partition_level
               );
      io_rollback_ddl(io_rollback_ddl.COUNT+1) := l_ddl;
    END IF;
  END int_insert_list_partition_ddl;

  PROCEDURE insert_range_partition_ddl(
    in_partition_arr  IN gt_partition_arr,
    in_high_values    IN gt_high_values,
    in_partition_name IN VARCHAR2,
    in_partition_desc IN VARCHAR2 DEFAULT NULL,
    io_forward_ddl    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl   IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    int_insert_range_partition_ddl(
      in_partition_arr    => in_partition_arr,
      in_high_values      => in_high_values,
      in_partition_name   => in_partition_name,
      in_partition_level  => 'PARTITION',
      in_parent_partition => NULL,
      in_partition_desc   => in_partition_desc,
      io_forward_ddl      => io_forward_ddl,
      io_rollback_ddl     => io_rollback_ddl
    );
  END insert_range_partition_ddl;

  PROCEDURE insert_range_subpartition_ddl(
    in_partition_arr     IN gt_partition_arr,
    in_high_values       IN gt_high_values,
    in_partition_name    IN VARCHAR2,
    in_subpartition_name IN VARCHAR2,
    in_subpartition_desc IN VARCHAR2 DEFAULT NULL,
    io_forward_ddl       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl      IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    int_insert_range_partition_ddl(
      in_partition_arr    => in_partition_arr,
      in_high_values      => in_high_values,
      in_partition_name   => in_subpartition_name,
      in_partition_level  => 'SUBPARTITION',
      in_parent_partition => in_partition_name,
      in_partition_desc   => in_subpartition_desc,
      io_forward_ddl      => io_forward_ddl,
      io_rollback_ddl     => io_rollback_ddl
    );
  END insert_range_subpartition_ddl;


  PROCEDURE insert_list_partition_ddl(
    in_partition_arr    IN gt_partition_arr,
    in_high_values      IN gt_high_values,
    in_partition_name   IN VARCHAR2,
    in_partition_desc   IN VARCHAR2 DEFAULT NULL,
    in_before_partition IN VARCHAR2 DEFAULT NULL, -- by default add to end
    io_forward_ddl      IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl     IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    int_insert_list_partition_ddl(
      in_partition_arr    => in_partition_arr,
      in_high_values      => in_high_values,
      in_partition_name   => in_partition_name,
      in_partition_level  => 'PARTITION',
      in_partition_desc   => in_partition_desc,
      in_parent_partition => NULL,
      in_before_partition => in_before_partition,
      io_forward_ddl      => io_forward_ddl,
      io_rollback_ddl     => io_rollback_ddl
    );
  END insert_list_partition_ddl;

  PROCEDURE insert_list_subpartition_ddl(
    in_partition_arr     IN gt_partition_arr,
    in_high_values       IN gt_high_values,
    in_partition_name    IN VARCHAR2,
    in_subpartition_name IN VARCHAR2,
    in_subpartition_desc IN VARCHAR2 DEFAULT NULL,
    in_before_partition  IN VARCHAR2 DEFAULT NULL, -- by default add to end
    io_forward_ddl       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rollback_ddl      IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    int_insert_list_partition_ddl(
      in_partition_arr    => in_partition_arr,
      in_high_values      => in_high_values,
      in_partition_name   => in_subpartition_name,
      in_partition_level  => 'SUBPARTITION',
      in_partition_desc   => in_subpartition_desc,
      in_parent_partition => in_partition_name,
      in_before_partition => in_before_partition,
      io_forward_ddl      => io_forward_ddl,
      io_rollback_ddl     => io_rollback_ddl
    );
  END insert_list_subpartition_ddl;


END partition_utils;
/