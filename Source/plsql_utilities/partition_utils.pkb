CREATE OR REPLACE PACKAGE BODY partition_utils
AS

/*
PL/SQL Utilities - Partition Utilities

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


  FUNCTION maxvalue
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_maxvalue;
    RETURN l_rec;
  END maxvalue;

  FUNCTION default_value
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_default;
    RETURN l_rec;
  END default_value;

  FUNCTION number_value(in_number_value IN NUMBER)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_number;
    l_rec.value := AnyData.ConvertNumber(in_number_value);
    RETURN l_rec;
  END number_value;

  FUNCTION varchar2_value(in_varchar2_value IN VARCHAR2)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_varchar2;
    l_rec.value := AnyData.ConvertVarchar2(in_varchar2_value);
    RETURN l_rec;
  END varchar2_value;

  FUNCTION date_value(in_date_value IN DATE)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_date;
    l_rec.value := AnyData.ConvertDate(in_date_value);
    RETURN l_rec;
  END date_value;

  FUNCTION timestamp_value(in_timestamp_value IN TIMESTAMP)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_timestamp;
    l_rec.value := AnyData.ConvertTimestamp(in_timestamp_value);
    RETURN l_rec;
  END timestamp_value;

  FUNCTION interval_ym_value(in_interval_ym_value IN YMINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_interval_ym;
    l_rec.value := AnyData.ConvertIntervalYM(in_interval_ym_value);
    RETURN l_rec;
  END interval_ym_value;

  FUNCTION interval_ds_value(in_interval_ds_value IN DSINTERVAL_UNCONSTRAINED)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_interval_ds;
    l_rec.value := AnyData.ConvertIntervalDS(in_interval_ds_value);
    RETURN l_rec;
  END interval_ds_value;

  FUNCTION tuple_value(in_tuple_value IN gt_high_values)
  RETURN gt_high_value_rec
  AS
    l_rec gt_high_value_rec;
  BEGIN
    l_rec.typecode := typecode_tuple;
    l_rec.value := AnyData.ConvertCollection(in_tuple_value);
    RETURN l_rec;
  END tuple_value;


  FUNCTION get_number_value(in_value IN gt_high_value_rec) RETURN NUMBER
  AS
  BEGIN
    IF in_value.typecode = typecode_number THEN
      RETURN in_value.value.AccessNumber;
    ELSE
      RETURN NULL;
    END IF;
  END get_number_value;

  FUNCTION get_varchar2_value(in_value IN gt_high_value_rec) RETURN VARCHAR2
  AS
  BEGIN
    IF in_value.typecode = typecode_varchar2 THEN
      RETURN in_value.value.AccessVarchar2;
    ELSE
      RETURN NULL;
    END IF;
  END get_varchar2_value;

  FUNCTION get_date_value(in_value IN gt_high_value_rec) RETURN DATE
  AS
  BEGIN
    IF in_value.typecode = typecode_date THEN
      RETURN in_value.value.AccessDate;
    ELSE
      RETURN NULL;
    END IF;
  END get_date_value;

  FUNCTION get_timestamp_value(in_value IN gt_high_value_rec) RETURN TIMESTAMP
  AS
  BEGIN
    IF in_value.typecode = typecode_timestamp THEN
      RETURN in_value.value.AccessTimestamp;
    ELSE
      RETURN NULL;
    END IF;
  END get_timestamp_value;

  FUNCTION get_interval_ym_value(in_value IN gt_high_value_rec) RETURN YMINTERVAL_UNCONSTRAINED
  AS
  BEGIN
    IF in_value.typecode = typecode_interval_ym THEN
      RETURN in_value.value.AccessIntervalYM;
    ELSE
      RETURN NULL;
    END IF;
  END get_interval_ym_value;

  FUNCTION get_interval_ds_value(in_value IN gt_high_value_rec) RETURN DSINTERVAL_UNCONSTRAINED
  AS
  BEGIN
    IF in_value.typecode = typecode_interval_ds THEN
      RETURN in_value.value.AccessIntervalDS;
    ELSE
      RETURN NULL;
    END IF;
  END get_interval_ds_value;

  FUNCTION get_tuple_value(in_value IN gt_high_value_rec) RETURN gt_high_values
  AS
    l_result gt_high_values;
    l_num    NUMBER;
  BEGIN
    IF in_value.typecode = typecode_tuple THEN
      l_num := in_value.value.GetCollection(l_result);
    END IF;
    RETURN l_result;
  END get_tuple_value;

  FUNCTION get_typecode(in_datatype IN VARCHAR2)
  RETURN PLS_INTEGER
  AS
    l_typecode PLS_INTEGER;
  BEGIN
    CASE
    WHEN in_datatype IN ('NUMBER','FLOAT') THEN
      l_typecode := typecode_number;
    WHEN in_datatype IN ( 'VARCHAR2', 'VARCHAR', 'CHAR', 'NVARCHAR2', 'NCHAR', 'RAW') THEN
      l_typecode := typecode_varchar2;
    WHEN in_datatype = 'DATE' THEN
      l_typecode := typecode_date;
    WHEN in_datatype LIKE 'TIMESTAMP(_)' THEN
      l_typecode := typecode_timestamp;
    WHEN in_datatype LIKE 'INTERVAL YEAR(_) TO MONTH' THEN
      l_typecode := typecode_interval_ym;
    WHEN in_datatype LIKE 'INTERVAL DAY(_) TO SECOND(_)' THEN
      l_typecode := typecode_interval_ds;
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
                              dbms_types.typecode_nchar,
                              dbms_sql.raw_type, dbms_types.typecode_raw
                              ) THEN
      l_typecode := typecode_varchar2;
    WHEN in_dbms_typecode = dbms_types.typecode_date THEN
      l_typecode := typecode_date;
    WHEN in_dbms_typecode in (dbms_sql.timestamp_type, dbms_types.typecode_timestamp) THEN
      l_typecode := typecode_timestamp;
    WHEN in_dbms_typecode in (dbms_sql.interval_year_to_month_type, dbms_types.typecode_interval_ym) THEN
      l_typecode := typecode_interval_ym;
    WHEN in_dbms_typecode in (dbms_sql.interval_day_to_second_type, dbms_types.typecode_interval_ds) THEN
      l_typecode := typecode_interval_ds;
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
    l_result        PLS_INTEGER;
    l_val1          gt_high_values;
    l_val2          gt_high_values;
  BEGIN
    IF in_value1.typecode = in_value2.typecode THEN
      CASE in_value1.typecode
      WHEN typecode_maxvalue THEN
        l_result := compare_equal;
      WHEN typecode_default THEN
        l_result := compare_equal;
      WHEN typecode_number THEN
        IF (get_number_value(in_value1) = get_number_value(in_value2)) OR
           (get_number_value(in_value1) IS NULL AND get_number_value(in_value2) IS NULL) THEN
          l_result := compare_equal;
        ELSIF (get_number_value(in_value1) < get_number_value(in_value2)) OR
              (get_number_value(in_value1) IS NOT NULL AND get_number_value(in_value2) IS NULL) THEN
          l_result := compare_less;
        ELSIF (get_number_value(in_value1) > get_number_value(in_value2)) OR
              (get_number_value(in_value1) IS NULL AND get_number_value(in_value2) IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_varchar2 THEN
        IF (get_varchar2_value(in_value1) = get_varchar2_value(in_value2)) OR
           (get_varchar2_value(in_value1) IS NULL AND get_varchar2_value(in_value2) IS NULL) THEN
          l_result := compare_equal;
        ELSIF (get_varchar2_value(in_value1) < get_varchar2_value(in_value2)) OR
              (get_varchar2_value(in_value1) IS NOT NULL AND get_varchar2_value(in_value2) IS NULL) THEN
          l_result := compare_less;
        ELSIF (get_varchar2_value(in_value1) > get_varchar2_value(in_value2)) OR
              (get_varchar2_value(in_value1) IS NULL AND get_varchar2_value(in_value2) IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_date THEN
        IF (get_date_value(in_value1) = get_date_value(in_value2)) OR
           (get_date_value(in_value1) IS NULL AND get_date_value(in_value2) IS NULL) THEN
          l_result := compare_equal;
        ELSIF (get_date_value(in_value1) < get_date_value(in_value2)) OR
              (get_date_value(in_value1) IS NOT NULL AND get_date_value(in_value2) IS NULL) THEN
          l_result := compare_less;
        ELSIF (get_date_value(in_value1) > get_date_value(in_value2)) OR
              (get_date_value(in_value1) IS NULL AND get_date_value(in_value2) IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_timestamp THEN
        IF (get_timestamp_value(in_value1) = get_timestamp_value(in_value2)) OR
           (get_timestamp_value(in_value1) IS NULL AND get_timestamp_value(in_value2) IS NULL) THEN
          l_result := compare_equal;
        ELSIF (get_timestamp_value(in_value1) < get_timestamp_value(in_value2)) OR
              (get_timestamp_value(in_value1) IS NOT NULL AND get_timestamp_value(in_value2) IS NULL) THEN
          l_result := compare_less;
        ELSIF (get_timestamp_value(in_value1) > get_timestamp_value(in_value2)) OR
              (get_timestamp_value(in_value1) IS NULL AND get_timestamp_value(in_value2) IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_interval_ym THEN
        IF (get_interval_ym_value(in_value1) = get_interval_ym_value(in_value2)) OR
           (get_interval_ym_value(in_value1) IS NULL AND get_interval_ym_value(in_value2) IS NULL) THEN
          l_result := compare_equal;
        ELSIF (get_interval_ym_value(in_value1) < get_interval_ym_value(in_value2)) OR
              (get_interval_ym_value(in_value1) IS NOT NULL AND get_interval_ym_value(in_value2) IS NULL) THEN
          l_result := compare_less;
        ELSIF (get_interval_ym_value(in_value1) > get_interval_ym_value(in_value2)) OR
              (get_interval_ym_value(in_value1) IS NULL AND get_interval_ym_value(in_value2) IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_interval_ds THEN
        IF (get_interval_ds_value(in_value1) = get_interval_ds_value(in_value2)) OR
           (get_interval_ds_value(in_value1) IS NULL AND get_interval_ds_value(in_value2) IS NULL) THEN
          l_result := compare_equal;
        ELSIF (get_interval_ds_value(in_value1) < get_interval_ds_value(in_value2)) OR
              (get_interval_ds_value(in_value1) IS NOT NULL AND get_interval_ds_value(in_value2) IS NULL) THEN
          l_result := compare_less;
        ELSIF (get_interval_ds_value(in_value1) > get_interval_ds_value(in_value2)) OR
              (get_interval_ds_value(in_value1) IS NULL AND get_interval_ds_value(in_value2) IS NOT NULL) THEN
          l_result := compare_greater;
        END IF;
      WHEN typecode_tuple THEN
        l_val1 := get_tuple_value(in_value1); 
        l_val2 := get_tuple_value(in_value2);
        l_result := compare_less;
        IF l_val1.COUNT = l_val2.COUNT THEN
          FOR i IN 1..l_val1.COUNT LOOP
            IF compare_high_value_rec(
                 in_value1       => l_val1(i),
                 in_value2       => l_val2(i),
                 in_compare_type => in_compare_type
               ) = compare_equal 
            THEN
              l_result := compare_equal;
            ELSE
              l_result := compare_less;
              EXIT;
            END IF;
          END LOOP;             
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
  
  PROCEDURE parse_values(
    in_sql         IN VARCHAR2,
    out_values     OUT NOCOPY gt_high_values
  )
  AS
    l_cur                 NUMBER;
    l_cnt                 INTEGER;
    l_row_cnt             NUMBER;
    l_columns             dbms_sql.desc_tab2;
    l_number_value        NUMBER;
    l_varchar2_value      VARCHAR2(32767);
    l_date_value          DATE;
    l_timestamp_value     TIMESTAMP(9);
    l_interval_ym_value   YMINTERVAL_UNCONSTRAINED;
    l_interval_ds_value   DSINTERVAL_UNCONSTRAINED;
  BEGIN
    l_cur := dbms_sql.open_cursor;
    BEGIN
      dbms_sql.parse(l_cur, in_sql, dbms_sql.native);
      dbms_sql.describe_columns2(l_cur, l_cnt, l_columns);
      out_values.EXTEND(l_cnt);
      FOR i IN 1..l_cnt LOOP
        CASE get_typecode(in_dbms_typecode => l_columns(i).col_type)
        WHEN typecode_number THEN
          dbms_sql.define_column(l_cur, i, l_number_value);
        WHEN typecode_varchar2 THEN
          dbms_sql.define_column(l_cur, i, l_varchar2_value, 32767);
        WHEN typecode_date THEN
          dbms_sql.define_column(l_cur, i, l_date_value);
        WHEN typecode_timestamp THEN
          dbms_sql.define_column(l_cur, i, l_timestamp_value);
        WHEN typecode_interval_ym THEN
          dbms_sql.define_column(l_cur, i, l_interval_ym_value);
        WHEN typecode_interval_ds THEN
          dbms_sql.define_column(l_cur, i, l_interval_ds_value);
        ELSE
          raise_application_error(-20000, 'Invalid partition key column '||i||' data type. col_type = '||l_columns(i).col_type);
        END CASE;
      END LOOP;
      l_row_cnt := dbms_sql.execute(l_cur);
      l_row_cnt := dbms_sql.fetch_rows(l_cur);
      FOR i IN 1..l_cnt LOOP
        IF l_columns(i).col_name = 'MAXVALUE' THEN
          out_values(i) := maxvalue;
        ELSE
          CASE get_typecode(in_dbms_typecode => l_columns(i).col_type)
          WHEN typecode_number THEN
            dbms_sql.column_value(l_cur, i, l_number_value);
            out_values(i) := number_value(l_number_value); 
          WHEN typecode_varchar2 THEN
            dbms_sql.column_value(l_cur, i, l_varchar2_value);
            out_values(i) := varchar2_value(l_varchar2_value);
          WHEN typecode_date THEN
            dbms_sql.column_value(l_cur, i, l_date_value);
            out_values(i) := date_value(l_date_value);
          WHEN typecode_timestamp THEN
            dbms_sql.column_value(l_cur, i, l_timestamp_value);
            out_values(i) := timestamp_value(l_timestamp_value);
          WHEN typecode_interval_ym THEN
            dbms_sql.column_value(l_cur, i, l_interval_ym_value);
            out_values(i) := interval_ym_value(l_interval_ym_value);
          WHEN typecode_interval_ds THEN
            dbms_sql.column_value(l_cur, i, l_interval_ds_value);
            out_values(i) := interval_ds_value(l_interval_ds_value);
          END CASE;
        END IF;  
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        dbms_sql.close_cursor(l_cur);
        RAISE;  
    END;
    dbms_sql.close_cursor(l_cur);
  END parse_values;

  $IF dbms_db_version.version > 12 or (dbms_db_version.version = 12 and dbms_db_version.release = 2) $THEN
  PROCEDURE parse_multi_col_list_values(
    in_string      IN VARCHAR2,
    out_values     OUT NOCOPY gt_high_values
  )
  AS
    l_parse_string              VARCHAR2(32767);
    l_tuple_string              VARCHAR2(32767);
    l_key_col                   VARCHAR2(32767);
    l_search_pos                PLS_INTEGER;
    l_key                       VARCHAR2(32767);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_expr_start_pattern        VARCHAR2(100);
    l_expr_end_pattern          VARCHAR2(100);
    l_open_bracket_cnt          PLS_INTEGER;
    l_open_bracket_pos          PLS_INTEGER;
    l_cur                       NUMBER;
    l_cnt                       INTEGER;
    l_columns                   dbms_sql.desc_tab2;
    l_sql                       clob;
  BEGIN
    l_search_pos := 1;
    l_open_bracket_cnt := 0;
    l_expr_start_pattern := q'{((N|n)?')|((N|n)?(Q|q)'\S)|(\()|(\))}';

    LOOP
      l_key_start_pos := REGEXP_INSTR(in_string, l_expr_start_pattern, l_search_pos, 1, 0);
      l_key_end_pos   := REGEXP_INSTR(in_string, l_expr_start_pattern, l_search_pos, 1, 1);
      EXIT WHEN l_key_start_pos = 0 OR l_key_start_pos IS NULL
             OR l_key_end_pos = 0 OR l_key_end_pos IS NULL;
        
      l_parse_string := l_parse_string || SUBSTR(in_string, l_search_pos, l_key_start_pos-l_search_pos);

      l_key := SUBSTR(in_string, l_key_start_pos, l_key_end_pos-l_key_start_pos);
      CASE
      WHEN l_key = 'N''' OR l_key = 'n''' OR l_key = '''' THEN
        l_expr_end_pattern := '''';
        l_search_pos := REGEXP_INSTR(in_string, l_expr_end_pattern, l_key_end_pos, 1, 0) + 1;
      WHEN REGEXP_LIKE(l_key, q'{(N|n)?(Q|q)'\S}') THEN
        CASE SUBSTR(l_key, -1)
          WHEN '{' THEN l_expr_end_pattern := '}''';
          WHEN '[' THEN l_expr_end_pattern := ']''';
          WHEN '(' THEN l_expr_end_pattern := ')''';
          WHEN '<' THEN l_expr_end_pattern := '>''';
          ELSE l_expr_end_pattern := SUBSTR(l_key, -1)||'''';
        END CASE;
        l_search_pos := INSTR(in_string, l_expr_end_pattern, l_key_end_pos) + 2;
      WHEN l_key = '(' THEN 
        l_search_pos := l_key_end_pos;
        IF l_open_bracket_cnt = 0 THEN 
          l_parse_string := l_parse_string ||'t_rec';
          l_open_bracket_pos := l_key_end_pos;
        END IF;
        l_open_bracket_cnt := l_open_bracket_cnt + 1;
      WHEN l_key = ')' THEN
        l_search_pos := l_key_end_pos;
        l_open_bracket_cnt := l_open_bracket_cnt - 1;
        IF l_open_bracket_cnt = 0 THEN 
          IF l_tuple_string IS NULL THEN
            l_tuple_string := 'SELECT '||SUBSTR(in_string, l_open_bracket_pos, l_key_start_pos-l_open_bracket_pos)||' FROM DUAL';
          END IF; 
        END IF;
      END CASE;
      l_parse_string := l_parse_string || SUBSTR(in_string, l_key_start_pos, l_search_pos-l_key_start_pos);
    END LOOP;

    l_parse_string := l_parse_string || SUBSTR(in_string, l_search_pos);
    
   
    l_cur := dbms_sql.open_cursor;
    BEGIN
      dbms_sql.parse(l_cur, l_tuple_string, dbms_sql.native);
      dbms_sql.describe_columns2(l_cur, l_cnt, l_columns);
    EXCEPTION
      WHEN OTHERS THEN 
        dbms_sql.close_cursor(l_cur);
        RAISE;  
    END;
    dbms_sql.close_cursor(l_cur);
    
    FOR i IN 1..l_cnt LOOP
      l_key_col := l_key_col||'
      COL_'||to_char(i)||'   '||
      CASE get_typecode(in_dbms_typecode => l_columns(i).col_type)
      WHEN typecode_number THEN 'NUMBER'
      WHEN typecode_varchar2 THEN 'VARCHAR2(32767)'
      WHEN typecode_date THEN 'DATE'
      WHEN typecode_timestamp THEN 'TIMESTAMP(9)'
      WHEN typecode_interval_ym THEN 'YMINTERVAL_UNCONSTRAINED'
      WHEN typecode_interval_ds THEN 'DSINTERVAL_UNCONSTRAINED'
      END||
      CASE WHEN i < l_cnt THEN ',' END;
    END LOOP;
      
    l_sql := '
    declare
      type t_rec is record('||l_key_col||'
      );
      type t_arr is table of t_rec;
      rec          t_rec;
      arr          t_arr;
      h            partition_utils.gt_high_values;
      tuple        partition_utils.gt_high_values;
    begin
      h := partition_utils.gt_high_values();
      tuple := partition_utils.gt_high_values();
      arr := t_arr('||l_parse_string||');
      h.extend(arr.count);
      tuple.extend('||l_cnt||');
      for i in 1..arr.count loop';        
    FOR i IN 1..l_cnt LOOP
      l_sql := l_sql||'
        tuple('||i||') := '|| 
        CASE get_typecode(in_dbms_typecode => l_columns(i).col_type)
        WHEN typecode_number THEN 'partition_utils.number_value' 
        WHEN typecode_varchar2 THEN 'partition_utils.varchar2_value'
        WHEN typecode_date THEN 'partition_utils.date_value'
        WHEN typecode_timestamp THEN 'partition_utils.timestamp_value'
        WHEN typecode_interval_ym THEN 'partition_utils.interval_ym_value'
        WHEN typecode_interval_ds THEN 'partition_utils.interval_ds_value'
        END||'(arr(i).COL_'||to_char(i)||');';
    END LOOP;
      l_sql := l_sql||'
        h(i) := partition_utils.tuple_value(tuple);
      end loop;
      :out_values := h;
    end;';

    
    EXECUTE IMMEDIATE l_sql USING OUT out_values;

  END parse_multi_col_list_values;
  $END

  PROCEDURE parse_high_value_str(
    in_string      IN VARCHAR2,
    out_values     OUT NOCOPY gt_high_values
  )
  AS
    l_cur                 NUMBER;
    l_cnt                 INTEGER;
    l_row_cnt             NUMBER;
    l_columns             dbms_sql.desc_tab2;
    l_number_value        NUMBER;
    l_varchar2_value      VARCHAR2(32767);
    l_date_value          DATE;
    l_timestamp_value     TIMESTAMP(9);
    l_interval_ym_value   YMINTERVAL_UNCONSTRAINED;
    l_interval_ds_value   DSINTERVAL_UNCONSTRAINED;
  BEGIN
    IF g_use_cache AND g_high_values_cache.EXISTS(in_string) THEN
      out_values := g_high_values_cache(in_string);
      RETURN; 
    END IF;
    
    IF in_string IS NULL THEN
      RETURN; 
    END IF;
    
--    dbms_output.put_line('in_string = '||in_string);
    
    out_values := gt_high_values();
    IF UPPER(in_string) = 'DEFAULT' THEN
      out_values.EXTEND(1);
      out_values(1) := default_value;
    ELSIF UPPER(in_string) = 'MAXVALUE' THEN
      out_values.EXTEND(1);
      out_values(1) := maxvalue;
    $IF dbms_db_version.version > 12 or (dbms_db_version.version = 12 and dbms_db_version.release = 2) $THEN
    ELSIF TRIM(in_string) LIKE '(%)' THEN
      parse_multi_col_list_values(in_string, out_values);
    $END  
    ELSE
      l_cur := dbms_sql.open_cursor;
      BEGIN
        dbms_sql.parse(l_cur, 'SELECT '||in_string||' FROM (SELECT NULL AS MAXVALUE from dual)', dbms_sql.native);
        dbms_sql.describe_columns2(l_cur, l_cnt, l_columns);
        out_values.EXTEND(l_cnt);
        FOR i IN 1..l_cnt LOOP
          CASE get_typecode(in_dbms_typecode => l_columns(i).col_type)
          WHEN typecode_number THEN
            dbms_sql.define_column(l_cur, i, l_number_value);
          WHEN typecode_varchar2 THEN
            dbms_sql.define_column(l_cur, i, l_varchar2_value, 32767);
          WHEN typecode_date THEN
            dbms_sql.define_column(l_cur, i, l_date_value);
          WHEN typecode_timestamp THEN
            dbms_sql.define_column(l_cur, i, l_timestamp_value);
          WHEN typecode_interval_ym THEN
            dbms_sql.define_column(l_cur, i, l_interval_ym_value);
          WHEN typecode_interval_ds THEN
            dbms_sql.define_column(l_cur, i, l_interval_ds_value);
          ELSE
            raise_application_error(-20000, 'Invalid partition key column '||i||' data type. col_type = '||l_columns(i).col_type);
          END CASE;
        END LOOP;
        l_row_cnt := dbms_sql.execute(l_cur);
        l_row_cnt := dbms_sql.fetch_rows(l_cur);
        FOR i IN 1..l_cnt LOOP
          IF l_columns(i).col_name = 'MAXVALUE' THEN
            out_values(i) := maxvalue;
          ELSE
            CASE get_typecode(in_dbms_typecode => l_columns(i).col_type)
            WHEN typecode_number THEN
              dbms_sql.column_value(l_cur, i, l_number_value);
              out_values(i) := number_value(l_number_value); 
            WHEN typecode_varchar2 THEN
              dbms_sql.column_value(l_cur, i, l_varchar2_value);
              out_values(i) := varchar2_value(l_varchar2_value);
            WHEN typecode_date THEN
              dbms_sql.column_value(l_cur, i, l_date_value);
              out_values(i) := date_value(l_date_value);
            WHEN typecode_timestamp THEN
              dbms_sql.column_value(l_cur, i, l_timestamp_value);
              out_values(i) := timestamp_value(l_timestamp_value);
            WHEN typecode_interval_ym THEN
              dbms_sql.column_value(l_cur, i, l_interval_ym_value);
              out_values(i) := interval_ym_value(l_interval_ym_value);
            WHEN typecode_interval_ds THEN
              dbms_sql.column_value(l_cur, i, l_interval_ds_value);
              out_values(i) := interval_ds_value(l_interval_ds_value);
            END CASE;
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

  FUNCTION as_literal(in_high_value IN gt_high_value_rec)
  RETURN VARCHAR2
  AS
    l_str           VARCHAR2(32767);
    l_date_format   VARCHAR2(100) := 'SYYYY-MM-DD HH24:MI:SS';
    l_date_calendar VARCHAR2(100) := 'NLS_CALENDAR=GREGORIAN';
    l_msec_format   VARCHAR2(100) := '.FF9';
    l_quote         CHAR(1) := CHR(39);
    tuple            gt_high_values;
  BEGIN
    CASE in_high_value.typecode
    WHEN typecode_maxvalue THEN
      l_str := 'MAXVALUE';
    WHEN typecode_default THEN
      l_str := 'DEFAULT';
    WHEN typecode_number THEN
      l_str := TO_CHAR(get_number_value(in_high_value));
    WHEN typecode_varchar2 THEN
      l_str := l_quote||REPLACE(get_varchar2_value(in_high_value),l_quote,l_quote||l_quote)||l_quote;
    WHEN typecode_date THEN
      l_str := 'TO_DATE('||l_quote||TO_CHAR(get_date_value(in_high_value),l_date_format,l_date_calendar)||l_quote||','||l_quote||l_date_format||l_quote||','||l_quote||l_date_calendar||l_quote||')';
    WHEN typecode_timestamp THEN
      l_str := 'TIMESTAMP'||l_quote||TO_CHAR(get_timestamp_value(in_high_value),l_date_format||l_msec_format,l_date_calendar)||l_quote;
    WHEN typecode_interval_ym THEN
      l_str := 'INTERVAL'||l_quote||TO_CHAR(get_interval_ym_value(in_high_value))||l_quote||' YEAR(9) TO MONTH';
    WHEN typecode_interval_ds THEN
      l_str := 'INTERVAL'||l_quote||TO_CHAR(get_interval_ds_value(in_high_value))||l_quote||' DAY(9) TO SECOND(9)';
    WHEN typecode_tuple THEN
      tuple := get_tuple_value(in_high_value);
      l_str := '('||as_literal(tuple)||')';
    ELSE
      NULL;
    END CASE;
    RETURN l_str;
  END as_literal;

  FUNCTION as_literal(in_high_values IN gt_high_values)
  RETURN VARCHAR2
  AS
    l_str           VARCHAR2(32767);
  BEGIN
    IF in_high_values IS NOT NULL THEN
      FOR i IN 1..in_high_values.COUNT LOOP
        l_str := l_str || as_literal(in_high_values(i))||CASE WHEN i < in_high_values.COUNT THEN ', ' END;
      END LOOP;
    END IF;
    RETURN l_str;
  END as_literal;

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

  
  PROCEDURE parse_partition_arr(
    io_partition_arr IN OUT NOCOPY gt_partition_arr
  )
  AS
    l_def_filter         CLOB;
    l_filter             CLOB;
    l_low_bound          VARCHAR2(32767);
    l_high_bound         VARCHAR2(32767);
    l_equal_filter       VARCHAR2(32767);
    l_maxval_filter      VARCHAR2(32767);
    l_key_column_str     VARCHAR2(32767);
    l_key_col_arr        gt_key_columns;
  BEGIN
    IF io_partition_arr.COUNT > 0 THEN
      CASE io_partition_arr(1).object_type
      WHEN 'TABLE' THEN
        SELECT tc.column_name, tc.data_type, tc.data_length
          BULK COLLECT 
          INTO l_key_col_arr
          FROM all_part_key_columns k
         INNER JOIN all_tab_columns tc
            ON tc.table_name = k.name
           AND tc.owner = k.owner
           AND tc.column_name = k.column_name
         WHERE k.object_type = 'TABLE'
           AND k.owner = io_partition_arr(1).object_owner
           AND k.name = io_partition_arr(1).object_name
          ORDER by k.column_position;

      WHEN 'INDEX' THEN
        SELECT tc.column_name, tc.data_type, tc.data_length
          BULK COLLECT 
          INTO l_key_col_arr
          FROM all_part_key_columns k
         INNER JOIN all_indexes i
            ON i.index_name = k.name
           AND i.owner = k.owner  
         INNER JOIN all_tab_columns tc
            ON tc.table_name = i.table_name
           AND tc.owner = i.table_owner
           AND tc.column_name = k.column_name
         WHERE k.object_type = 'INDEX'
           AND k.owner = io_partition_arr(1).object_owner
           AND k.name = io_partition_arr(1).object_name
          ORDER by k.column_position; 
      END CASE;     
    END IF;

    IF l_key_col_arr IS NOT NULL THEN 
      FOR i IN 1..l_key_col_arr.COUNT LOOP
        l_key_column_str := l_key_column_str||'"'||l_key_col_arr(i).column_name||'",';
      END LOOP;
      l_key_column_str := '('||TRIM(',' FROM l_key_column_str)||')';
    END IF;
    
    FOR i IN 1..io_partition_arr.COUNT LOOP
      io_partition_arr(i).array_index := i;
      io_partition_arr(i).key_columns := l_key_col_arr;
      parse_high_value_str(io_partition_arr(i).high_value, io_partition_arr(i).high_values);
      CASE io_partition_arr(i).partitioning_type 
        WHEN 'LIST' THEN
          IF io_partition_arr(i).high_values(1).typecode = typecode_default THEN
            IF l_def_filter IS NOT NULL THEN
              io_partition_arr(i).filter_clause := 'CASE WHEN '||l_key_column_str ||' IN ('||l_def_filter||') THEN 0 ELSE 1 END = 1';
            ELSE  
              io_partition_arr(i).filter_clause := '1=1';
            END IF;  
          ELSE
            l_filter := as_literal(io_partition_arr(i).high_values);
            io_partition_arr(i).filter_clause := l_key_column_str||' IN ('||l_filter||')'; 
            IF l_def_filter IS NULL THEN
              l_def_filter := l_filter; 
            ELSE
              l_def_filter := l_def_filter|| ',' ||l_filter;
            END IF;
          END IF;
        WHEN 'RANGE' THEN
          l_filter := NULL;
          l_low_bound := NULL;
          IF i=1 THEN 
            l_low_bound := '(1=1)';
          ELSE
            -- low bound
            l_equal_filter := NULL; 
            FOR j IN 1..io_partition_arr(i-1).high_values.COUNT LOOP
              IF io_partition_arr(i-1).high_values(j).typecode <> typecode_maxvalue THEN
                l_low_bound := l_low_bound||'('||l_equal_filter||'"'||io_partition_arr(i-1).key_columns(j).column_name||'" >'||CASE WHEN j = io_partition_arr(i-1).high_values.COUNT THEN '=' END||' '||as_literal(io_partition_arr(i-1).high_values(j))||')';
              ELSE
                l_low_bound := l_low_bound||'('||l_equal_filter||'1=0)';
                EXIT;
              END IF;  
              l_equal_filter := l_equal_filter||'"'||io_partition_arr(i-1).key_columns(j).column_name||'" = '||as_literal(io_partition_arr(i-1).high_values(j))||' AND ';
              IF j < io_partition_arr(i-1).high_values.COUNT THEN
                l_low_bound := l_low_bound||' OR ';  
              END IF;
            END LOOP;
          END IF;
          -- high bound
          l_equal_filter := NULL; 
          l_high_bound := NULL;          
          l_maxval_filter := NULL;
          FOR j IN 1..io_partition_arr(i).high_values.COUNT LOOP
            IF io_partition_arr(i).high_values(j).typecode <> typecode_maxvalue THEN
              l_high_bound := l_high_bound||'('||l_equal_filter||'"'||io_partition_arr(i).key_columns(j).column_name||'" < '||as_literal(io_partition_arr(i).high_values(j))||')';
            ELSE
              l_high_bound := l_high_bound||'('||l_equal_filter||'1=1)';
              l_maxval_filter := '('||l_equal_filter||'"'||io_partition_arr(i).key_columns(j).column_name||'" is null)';
              EXIT;
            END IF;  
            l_equal_filter := l_equal_filter||'"'||io_partition_arr(i).key_columns(j).column_name||'" = '||as_literal(io_partition_arr(i).high_values(j))||' AND ';
            IF j < io_partition_arr(i).high_values.COUNT THEN
              l_high_bound := l_high_bound||' OR ';  
            END IF;
          END LOOP;
          l_filter := '(('||l_low_bound||') AND ('||l_high_bound||'))';
          IF l_maxval_filter IS NOT NULL THEN
            l_filter := '('||l_filter||' OR '||l_maxval_filter||')';
          END IF;
        ELSE NULL;
      END CASE;
      io_partition_arr(i).filter_clause := l_filter;
    END LOOP;
  END parse_partition_arr;

  FUNCTION get_partitions(
    io_cursor IN OUT gt_partition_ref_cur
  )
  RETURN gt_partition_arr
  AS
    l_partition_cur_arr           gt_partition_cur_arr;
    l_partition_arr               gt_partition_arr;   
  BEGIN
    -- cursor should be open
    FETCH io_cursor           
     BULK COLLECT 
     INTO l_partition_cur_arr;
    CLOSE io_cursor;
    
    FOR i IN 1..l_partition_cur_arr.COUNT LOOP
      l_partition_arr(i).object_type := l_partition_cur_arr(i).object_type;
      l_partition_arr(i).object_name := l_partition_cur_arr(i).object_name;
      l_partition_arr(i).object_owner := l_partition_cur_arr(i).object_owner;
      l_partition_arr(i).partitioning_type := l_partition_cur_arr(i).partitioning_type;
      l_partition_arr(i).partition_level := l_partition_cur_arr(i).partition_level;
      l_partition_arr(i).partition_name := l_partition_cur_arr(i).partition_name;         
      l_partition_arr(i).parent_name := l_partition_cur_arr(i).parent_name;
      l_partition_arr(i).partition_position := l_partition_cur_arr(i).partition_position;
      l_partition_arr(i).high_value := l_partition_cur_arr(i).high_value;
    END LOOP;
    
    parse_partition_arr(
      io_partition_arr => l_partition_arr
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
        SELECT l_object_type, in_object_name, in_object_owner, pt.partitioning_type, in_partition_level,
               p.partition_name, NULL AS parent_part_name, p.partition_position, p.high_value
          FROM all_tab_partitions p
         INNER JOIN all_part_tables pt
            ON pt.table_name = p.table_name  
           AND pt.owner = p.table_owner
         WHERE p.table_name = in_object_name
           AND p.table_owner = in_object_owner
           AND (in_name_regexp IS NULL OR REGEXP_LIKE(p.partition_name, in_name_regexp))  
         ORDER BY p.partition_position;
      WHEN 'INDEX' THEN
        OPEN l_ref_cursor FOR
        SELECT l_object_type, in_object_name, in_object_owner, pi.partitioning_type, in_partition_level,
               p.partition_name, NULL AS parent_name, p.partition_position, p.high_value
          FROM all_ind_partitions p
         INNER JOIN all_part_indexes pi
            ON pi.index_name = p.index_name
           AND pi.owner = p.index_owner 
         WHERE p.index_name = in_object_name
           AND p.index_owner = in_object_owner
           AND (in_name_regexp IS NULL OR REGEXP_LIKE(p.partition_name, in_name_regexp))  
         ORDER BY partition_position;
      END CASE;
    WHEN 'SUBPARTITION' THEN
      CASE l_object_type
      WHEN 'TABLE' THEN
        IF in_partition_name IS NOT NULL THEN
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, pt.subpartitioning_type, in_partition_level,
                 p.subpartition_name, p.partition_name, p.subpartition_position, p.high_value
            FROM all_tab_subpartitions p
           INNER JOIN all_part_tables pt
              ON pt.table_name = p.table_name  
             AND pt.owner = p.table_owner
           WHERE p.table_name = in_object_name
             AND p.table_owner = in_object_owner
             AND p.partition_name = in_partition_name
             AND (in_name_regexp IS NULL OR REGEXP_LIKE(p.subpartition_name, in_name_regexp))  
           ORDER BY subpartition_position;
        ELSE
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, pt.subpartitioning_type, in_partition_level,
                 sp.subpartition_name, sp.partition_name, sp.subpartition_position, sp.high_value
            FROM all_tab_subpartitions sp
           INNER JOIN all_tab_partitions p
              ON p.table_name = sp.table_name
             AND p.table_owner = sp.table_owner
             AND p.partition_name = sp.partition_name
           INNER JOIN all_part_tables pt
              ON pt.table_name = p.table_name  
             AND pt.owner = p.table_owner
           WHERE sp.table_name = in_object_name
             AND sp.table_owner = in_object_owner
             AND (in_name_regexp IS NULL OR REGEXP_LIKE(subpartition_name, in_name_regexp))  
           ORDER BY p.partition_position, sp.subpartition_position;
        END IF;
      WHEN 'INDEX' THEN
        IF in_partition_name IS NOT NULL THEN
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, pi.subpartitioning_type, in_partition_level,
                 p.subpartition_name, p.partition_name, p.subpartition_position, p.high_value
            FROM all_ind_subpartitions p
           INNER JOIN all_part_indexes pi
              ON pi.index_name = p.index_name
             AND pi.owner = p.index_owner 
           WHERE p.index_name = in_object_name
             AND p.index_owner = in_object_owner
             AND p.partition_name = in_partition_name
             AND (in_name_regexp IS NULL OR REGEXP_LIKE(p.subpartition_name, in_name_regexp))  
           ORDER BY subpartition_position;
        ELSE
          OPEN l_ref_cursor FOR
          SELECT l_object_type, in_object_name, in_object_owner, pi.subpartitioning_type, in_partition_level,
                 sp.subpartition_name, sp.partition_name, sp.subpartition_position, sp.high_value
            FROM all_ind_subpartitions sp
           INNER JOIN all_ind_partitions p
              ON p.index_name = sp.index_name
             AND p.index_owner = sp.index_owner
             AND p.partition_name = sp.partition_name
           INNER JOIN all_part_indexes pi
              ON pi.index_name = p.index_name
             AND pi.owner = p.index_owner 
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
        IF in_subpartitions_arr(j).parent_name = in_partitions_arr(i).partition_name THEN
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
        l_partition_name := in_subpartition_arr(l_last_index).parent_name;
        -- looping all subpartitions
        FOR i IN l_last_index..in_subpartition_arr.LAST loop
          -- reset last index to NULL so when we reach the end of main array next iteration will interrupt main loop
          l_last_index := NULL;
          -- and finding all subpartitions with the same parent partition anme
          IF l_partition_name = in_subpartition_arr(i).parent_name THEN
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
            as_literal(in_split_values)||') INTO ('||in_partition_level||' '||l_part_name||' '||
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
            as_literal(in_split_values)||') INTO ('||in_partition_level||' '||l_part_name||' '||
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
           as_literal(in_values)||') '||in_partition_desc;
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
             as_literal(in_values)||') '||in_partition_desc;
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
           as_literal(in_values)||')';
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
           as_literal(in_values)||')';
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
                               in_values        => gt_high_values(default_value)
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