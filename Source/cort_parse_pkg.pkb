CREATE OR REPLACE PACKAGE BODY cort_parse_pkg
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
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_replaced_names_indx          arrays.gt_name_indx;  -- list of object names indexed by replaced names 

  PROCEDURE debug(
    in_text      IN CLOB,
    in_details   IN CLOB DEFAULT NULL
  )
  AS
  BEGIN
    cort_exec_pkg.debug(in_text, in_details);
  END debug;

 
  /*  Masks all reg exp key symbols */
  FUNCTION get_regexp_const(
    in_value          IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    TYPE t_regexp_keys  IS TABLE OF VARCHAR2(1);
    TYPE t_regexp_masks IS TABLE OF VARCHAR2(4);
    l_regexp_keys  t_regexp_keys  := t_regexp_keys(  '\',  '[',  ']',  '*',  '?',  '.',  '+',  '*',  '-',  '^',  '{',  '}',  '|',  '$',  '(',  ')');
    l_regexp_masks t_regexp_masks := t_regexp_masks('\\', '\[', '\]', '\*', '\?', '\.', '\+', '\*', '\-', '\^', '\{', '\}', '\|', '\$', '\(', '\)');
    l_value   VARCHAR2(32767);
  BEGIN
    l_value := in_value;
    FOR I IN 1..l_regexp_keys.COUNT LOOP
      l_value := REPLACE(l_value, l_regexp_keys(i), l_regexp_masks(i));
    END LOOP;
    RETURN l_value;
  END get_regexp_const;

  /* Return TRUE is given name is simple SQL name and doesnt require double quotes */
  FUNCTION is_simple_name(in_name IN VARCHAR2)
  RETURN BOOLEAN
  AS
  BEGIN
    $IF (dbms_db_version.version = 12 AND dbms_db_version.release >=2) or (dbms_db_version.version > 12) $THEN 
      RETURN REGEXP_LIKE(in_name, '^[A-Z][A-Z0-9_$#]{0,127}$');
    $ELSE  
      RETURN REGEXP_LIKE(in_name, '^[A-Z][A-Z0-9_$#]{0,29}$');
    $END
  END is_simple_name;

  FUNCTION get_parse_only_sql(
    in_name  IN VARCHAR2,
    in_delim IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_regexp VARCHAR2(200);
  BEGIN
    IF is_simple_name(in_name) THEN
      l_regexp := '('||get_regexp_const(in_name)||')'||in_delim;
    ELSE
      l_regexp := '("'||get_regexp_const(in_name)||'")';
    END IF;
    RETURN l_regexp;
  END get_parse_only_sql;

  FUNCTION get_owner_name_regexp(
    in_name  IN VARCHAR2,
    in_owner IN VARCHAR2,
    in_delim IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2 
  AS
    l_regexp VARCHAR2(200);
  BEGIN
    IF is_simple_name(in_owner) THEN
      l_regexp := get_regexp_const(in_owner);
    ELSE
      l_regexp := '"'||get_regexp_const(in_owner)||'"';
    END IF;
    RETURN '('||l_regexp||'\s*\.\s*)?'||get_parse_only_sql(in_name, in_delim);
  END get_owner_name_regexp;

  /* Returns regular expression to find `column of given type */
  FUNCTION get_column_regexp(
    in_column_rec  IN  cort_exec_pkg.gt_column_rec 
  )
  RETURN VARCHAR2
  AS
    TYPE t_type_exprs  IS TABLE OF VARCHAR2(4000) INDEX BY VARCHAR2(30);
    l_type_expr_arr       t_type_exprs;
    l_regexp              VARCHAR2(1000);
    l_data_type_regexp    VARCHAR2(1000);
  BEGIN
    l_type_expr_arr('NUMBER') := '((NUMBER)|(INTEGER)|(INT)|(SMALLINT)|(DECIMAL)|(NUMERIC)|(DEC))';
    l_type_expr_arr('FLOAT') := '((FLOAT)|(REAL)|(DOUBLE\s+PRECISION))';
    l_type_expr_arr('BINARY_DOUBLE') := '((BINARY_DOUBLE)|(BINARY_FLOAT))';
    l_type_expr_arr('VARCHAR2') := '((VARCHAR2)|(VARCHAR)|(CHARACTER\s+VARYING)|(CHAR\s+VARYING))';
    l_type_expr_arr('CHAR') := '((CHAR)|(CHARACTER))';
    l_type_expr_arr('NVARCHAR2') := '((NVARCHAR2)|(NATIONAL\s+CHARACTER\s+VARYING)|(NATIONAL\s+CHAR\s+VARYING)|(NCHAR\s+VARYING))';
    l_type_expr_arr('NCHAR') := '((NCHAR)|(NATIONAL\s+CHARACTER)|(NATIONAL\s+CHAR))';
    l_type_expr_arr('TIMESTAMP') := '(TIMESTAMP(\([0-9]\))?)';
    l_type_expr_arr('TIMESTAMP WITH TIMEZONE') := '(TIMESTAMP\s*(\(\s*[0-9]\s*\))?\s*WITH\s+TIME\s+ZONE)';
    l_type_expr_arr('TIMESTAMP WITH LOCAL TIMEZONE') := '(TIMESTAMP\s*(\(\s*[0-9]\s*\))?\s+WITH\s+LOCAL\s+TIME\s+ZONE)';
    l_type_expr_arr('INTERVAL YEAR TO MONTH') := '(INTERVAL\s+YEAR\s*(\(\s*[0-9]\s*\))?\s+TO\s+MONTH)';
    l_type_expr_arr('INTERVAL DAY TO SECOND') := '(INTERVAL\s+DAY\s*(\(\s*[0-9]\s*\))?\s+TO\s+SECOND(\(\s*[0-9]\s*\))?)';
    l_type_expr_arr('RAW') := '(RAW\s*\(\s*[0-9]+\s*\))';

    l_regexp := get_parse_only_sql(in_column_rec.column_name, '\s+');


    IF in_column_rec.data_type_mod IS NOT NULL THEN
      l_data_type_regexp := l_data_type_regexp||get_regexp_const(in_column_rec.data_type_mod)||'\s+';
    END IF;
    
    IF in_column_rec.data_type_owner IS NOT NULL THEN
      l_data_type_regexp := l_data_type_regexp||get_owner_name_regexp(in_column_rec.data_type, in_column_rec.data_type_owner);
    ELSE
      IF l_type_expr_arr.EXISTS(in_column_rec.data_type) THEN
        l_data_type_regexp := l_data_type_regexp||l_type_expr_arr(in_column_rec.data_type);
      ELSE
        l_data_type_regexp := l_data_type_regexp||get_regexp_const(in_column_rec.data_type);
      END IF;
    END IF;

    IF in_column_rec.virtual_column = 'YES' THEN
      l_data_type_regexp := '('||l_data_type_regexp||')?';
--    l_regexp := l_regexp||'(GENERATED\s+ALWAYS\s+)?AS)\W';
    END IF;
    l_regexp := '\W('||l_regexp || l_data_type_regexp||')\W';
    
    RETURN l_regexp;
  END get_column_regexp;

  -- Returns position of close bracket - ) - ignoring all nested pairs of brackets ( ) and quoted SQL names
  FUNCTION get_closed_bracket(
    in_sql              IN CLOB,
    in_search_position  IN PLS_INTEGER -- Position AFTER open bracket
  )
  RETURN PLS_INTEGER
  AS
    l_search_pos         PLS_INTEGER;
    l_open_bracket_cnt   PLS_INTEGER;
    l_close_bracket_cnt  PLS_INTEGER;
    l_key_pos            PLS_INTEGER;
    l_key                VARCHAR2(1);
  BEGIN
    l_search_pos := in_search_position;
    l_open_bracket_cnt := 1;
    l_close_bracket_cnt := 0;
    LOOP
      l_key_pos := REGEXP_INSTR(in_sql, '\(|\)|"', l_search_pos, 1, 0);
      EXIT WHEN l_key_pos = 0 OR l_key_pos IS NULL
             OR l_open_bracket_cnt > 4000 OR l_close_bracket_cnt > 4000;
      l_key := SUBSTR(in_sql, l_key_pos, 1);
      CASE l_key
        WHEN '(' THEN
          l_open_bracket_cnt := l_open_bracket_cnt + 1;
        WHEN ')' THEN
          l_close_bracket_cnt := l_close_bracket_cnt + 1;
        WHEN '"' THEN
          l_key_pos := REGEXP_INSTR(in_sql, '"', l_search_pos, 1, 0);
      END CASE;
      IF l_open_bracket_cnt = l_close_bracket_cnt THEN
        RETURN l_key_pos;
      END IF;
      l_search_pos := l_key_pos + 1;
    END LOOP;
    cort_exec_pkg.raise_error('Parsing error - unable to find closing bracket. Search position - '||in_search_position);
  END get_closed_bracket;


  FUNCTION skip_whitespace(
    in_sql        IN CLOB,
    in_search_pos IN PLS_INTEGER
  )
  RETURN PLS_INTEGER
  AS
    l_key_start_pos PLS_INTEGER;
  BEGIN
    l_key_start_pos := REGEXP_INSTR(in_sql, '\S', in_search_pos, 1, 0);
    IF l_key_start_pos > 0 THEN
      RETURN l_key_start_pos;
    ELSE 
      RETURN in_search_pos;
    END IF;  
  END skip_whitespace;
  
  -- read and return SQL name following in_search_pos. If valid name is not found then return NULL
  FUNCTION read_next_name(
    in_sql        IN CLOB,
    io_search_pos IN OUT PLS_INTEGER
  )
  RETURN VARCHAR2
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       arrays.gt_name;
  BEGIN
    l_search_pos := io_search_pos;
    l_search_pos := skip_whitespace(in_sql, l_search_pos);
    l_regexp := '\S';
    l_key := REGEXP_SUBSTR(in_sql, l_regexp, l_search_pos, 1);
    CASE 
    WHEN l_key = '"' THEN
      l_regexp := '"';
      l_key_start_pos := REGEXP_INSTR(in_sql, l_regexp, l_search_pos, 1, 1);
      l_search_pos := l_key_start_pos;
      l_key_end_pos := REGEXP_INSTR(in_sql, l_regexp, l_search_pos, 1, 0);
      l_key := SUBSTR(in_sql, l_key_start_pos, l_key_end_pos - l_key_start_pos);
      io_search_pos := l_key_end_pos + 1;
    WHEN l_key >= 'A' AND l_key <= 'Z' THEN
      l_regexp := '[A-Z][A-Z0-9_$#]{0,29}';
      l_key_start_pos := REGEXP_INSTR(in_sql, l_regexp, l_search_pos, 1, 0);
      l_key_end_pos := REGEXP_INSTR(in_sql, l_regexp, l_search_pos, 1, 1);
      l_key := SUBSTR(in_sql, l_key_start_pos, l_key_end_pos - l_key_start_pos);
      io_search_pos := l_key_end_pos;
    ELSE
      l_key := NULL;  
    END CASE;
    RETURN l_key;
  END read_next_name;
  
  
  PROCEDURE initial_object_parse(
    io_sql_rec       IN OUT NOCOPY gt_sql_rec, 
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
    l_regexp                    VARCHAR(1000);
    l_name_regexp               VARCHAR(1000);
    l_schema_name               VARCHAR2(1000);
  BEGIN
    l_search_pos := 1;
    l_search_pos := skip_whitespace(io_sql_rec.normalized_sql, l_search_pos);
    
    io_sql_rec.object_type := in_object_type;
    io_sql_rec.object_name := in_object_name;
    io_sql_rec.object_owner := in_object_owner;

    CASE in_object_type 
    WHEN 'TABLE' THEN
      l_regexp := '(CREATE)\s+((GLOBAL\s+TEMPORARY\s+)?TABLE)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner, '\W'); 
    WHEN 'INDEX' THEN
      l_regexp := '(CREATE)\s+((UNIQUE\s+|BITMAP\s+)?INDEX)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner, '\W'); 
    WHEN 'SEQUENCE' THEN
      l_regexp := '(CREATE)\s+(SEQUENCE)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner, '(\W|$)'); 
    WHEN 'TYPE' THEN
      l_regexp := '(CREATE)\s+((OR\s+REPLACE\s+)?TYPE)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner, '(\W|$)'); 
    END CASE; 
    
    debug('Create regexp = '||l_regexp); 
    debug('Object name regexp = '||l_name_regexp);

    l_key_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 0); -- find object definition
    l_key_end_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 2); -- find end of table definition
    IF l_key_start_pos = l_search_pos THEN
      io_sql_rec.cort_param_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 1); -- end of keyword CREATE
      io_sql_rec.cort_param_end_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 0, NULL, 2); -- start of keyword following "CREATE"
      io_sql_rec.name_start_pos := l_key_end_pos;
    ELSE
      cort_exec_pkg.raise_error( 'Unable to parse CREATE statement');
    END IF;
    
    l_search_pos := io_sql_rec.name_start_pos;
    -- find object name 
    l_search_pos := skip_whitespace(io_sql_rec.normalized_sql, l_search_pos);
    l_key_end_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_name_regexp, l_search_pos, 1, 1, NULL, 2);
    IF l_key_end_pos > 0 THEN
      io_sql_rec.definition_start_pos := l_key_end_pos;
      l_schema_name := REGEXP_SUBSTR(io_sql_rec.normalized_sql, l_name_regexp, l_search_pos, 1, NULL, 1);
      l_schema_name := REGEXP_REPLACE(l_schema_name, '^\s+', NULL);
      l_schema_name := REGEXP_REPLACE(l_schema_name, '\s*\.\s*$', NULL);
      io_sql_rec.schema_name := l_schema_name; 
      debug('schema_name = '||io_sql_rec.schema_name);
    ELSE
      debug('Object name not found');
      cort_exec_pkg.raise_error( 'Object name not found');
    END IF; 
    
  END initial_object_parse;
  
  -- Finds cort_value params and assigns them to the nearest column
  PROCEDURE parse_column_cort_values(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    io_table_rec  IN OUT NOCOPY cort_exec_pkg.gt_table_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(20);
    l_text                      VARCHAR2(32767);
    l_value                     VARCHAR2(32767);
    l_indx                      PLS_INTEGER;
    l_last_column_index         PLS_INTEGER;
    l_column_index              PLS_INTEGER;
    
    FUNCTION get_column_at(
      in_position      IN  PLS_INTEGER, 
      out_column_index OUT PLS_INTEGER
    )
    RETURN BOOLEAN  
    AS
    BEGIN
      FOR i IN l_last_column_index..io_table_rec.column_arr.COUNT LOOP
        IF in_position BETWEEN io_table_rec.column_arr(i).sql_end_position 
                           AND io_table_rec.column_arr(i).sql_next_start_position
        THEN
          l_last_column_index := i;
          out_column_index := i; 
          RETURN TRUE;
        END IF;                    
      END LOOP;
      l_last_column_index := io_table_rec.column_arr.COUNT + 1;
      out_column_index := -1;
      RETURN FALSE;
    END get_column_at;
    
  BEGIN
    --#[release]= 
    -- or
    --#[release]==
    -- where [release] is current release value wrapped with square brackets []. This clause is optional
    
    l_regexp := cort_params_pkg.gc_prefix||'(\['||
                get_regexp_const(cort_pkg.get_current_release)||'\])?('||
                cort_params_pkg.gc_force_value_prefix||'|'||
                cort_params_pkg.gc_value_prefix||')';

  
    l_last_column_index := 1;
    l_indx := io_sql_rec.lexical_units_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF io_sql_rec.lexical_units_arr(l_indx).start_position BETWEEN io_sql_rec.columns_start_pos AND io_sql_rec.columns_end_pos AND
         io_sql_rec.lexical_units_arr(l_indx).unit_type IN ('LINE COMMENT', 'COMMENT') 
      THEN
        CASE io_sql_rec.lexical_units_arr(l_indx).unit_type 
        WHEN 'LINE COMMENT' THEN
          l_text := SUBSTR(io_sql_rec.lexical_units_arr(l_indx).text, 3);
        WHEN 'COMMENT' THEN
          l_text := SUBSTR(io_sql_rec.lexical_units_arr(l_indx).text, 3, LENGTH(io_sql_rec.lexical_units_arr(l_indx).text)-4);
        END CASE; 
        l_key_start_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 0, 'i');
        l_key_end_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 1, 'i');
        IF l_key_start_pos = 1 THEN
          l_key := REGEXP_SUBSTR(l_text, l_regexp, 1, 1, 'i', 2);
          l_value := SUBSTR(l_text, l_key_end_pos);
          IF TRIM(l_value) IS NOT NULL THEN
            CASE l_key
            WHEN cort_params_pkg.gc_force_value_prefix THEN
              IF get_column_at(l_indx, l_column_index) THEN
                debug('Parsing: Column at position '||l_indx||' - '||io_table_rec.column_arr(l_column_index).column_name||'. Cort force value = '||l_value);
                io_table_rec.column_arr(l_column_index).cort_value := l_value;
                io_table_rec.column_arr(l_column_index).cort_value_force := TRUE;
              ELSE
                debug('Parsing: Column at position '||l_indx||' not found');
              END IF;
            WHEN cort_params_pkg.gc_value_prefix THEN
              IF get_column_at(l_indx, l_column_index) THEN
                debug('Parsing: Column at position '||l_indx||' - '||io_table_rec.column_arr(l_column_index).column_name||'. Cort value = '||l_value);
                io_table_rec.column_arr(l_column_index).cort_value := l_value;
                io_table_rec.column_arr(l_column_index).cort_value_force := FALSE;
              ELSE
                debug('Parsing: Column at position '||l_indx||' not found');
              END IF;
            ELSE 
              debug('Unknown cort key = '||l_key);
            END CASE;
          END IF;
        END IF;            
      END IF;
      EXIT WHEN l_indx > io_sql_rec.columns_end_pos;
      l_indx := io_sql_rec.lexical_units_arr.NEXT(l_indx);
    END LOOP;

  END parse_column_cort_values;

  PROCEDURE int_replace_names(
    in_sql_rec IN gt_sql_rec,
    out_sql    OUT NOCOPY CLOB 
  )
  AS
    l_indx         PLS_INTEGER;
    l_replace_rec  gt_replace_rec;
  BEGIN
    -- loop all names start from the end
    out_sql := in_sql_rec.original_sql;
    l_indx := in_sql_rec.replaced_names_arr.LAST;
    WHILE l_indx IS NOT NULL LOOP
      l_replace_rec := in_sql_rec.replaced_names_arr(l_indx);
      debug('l_replace_rec.object_name = '||l_replace_rec.object_name);
      debug('l_replace_rec.new_name = '||l_replace_rec.new_name);
      IF in_sql_rec.lexical_units_arr.EXISTS(l_replace_rec.start_pos-1) AND 
         in_sql_rec.lexical_units_arr(l_replace_rec.start_pos-1).unit_type = 'QUOTED NAME'
      THEN
        l_replace_rec.start_pos := l_replace_rec.start_pos - 1;
        l_replace_rec.end_pos := l_replace_rec.end_pos + 1;
      END IF;          
      out_sql := SUBSTR(out_sql, 1, l_replace_rec.start_pos - 1)||'"'||l_replace_rec.new_name||'"'||SUBSTR(out_sql, l_replace_rec.end_pos);
      l_indx := in_sql_rec.replaced_names_arr.PRIOR(l_indx);
    END LOOP;
    out_sql := SUBSTR(out_sql, 1, in_sql_rec.cort_param_start_pos - 1)||' '||SUBSTR(out_sql, in_sql_rec.cort_param_end_pos);
  END int_replace_names;

  -- find all entries for given name 
  FUNCTION find_substitutable_name(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_new_name     IN VARCHAR2,
    in_pattern      IN VARCHAR2,
    in_search_pos   IN PLS_INTEGER DEFAULT 1,
    in_subexpr      IN PLS_INTEGER DEFAULT NULL
  )
  RETURN PLS_INTEGER
  AS
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, in_pattern, in_search_pos, 1, 0, NULL, in_subexpr);
    IF l_start_pos > 0 THEN 
      debug('found substitutable name @'||l_start_pos||'. Pattern = '||in_pattern);
      l_replace_rec.object_type := in_object_type;
      l_replace_rec.object_name := in_object_name;
      l_replace_rec.start_pos := l_start_pos;
      l_replace_rec.end_pos   := REGEXP_INSTR(io_sql_rec.normalized_sql, in_pattern, in_search_pos, 1, 1, NULL, in_subexpr);
      l_replace_rec.new_name  := in_new_name;
      io_sql_rec.replaced_names_arr(l_start_pos) := l_replace_rec;
      g_replaced_names_indx(in_object_type||':"'||in_new_name||'"') := in_object_name;
      RETURN l_replace_rec.end_pos;
    ELSE
      RETURN 0;
    END IF;
  END find_substitutable_name;

  -- find all entries for given table name 
  PROCEDURE find_table_name(
    io_sql_rec     IN OUT NOCOPY gt_sql_rec,
    in_table_name  IN VARCHAR2,
    in_table_owner IN VARCHAR2,
    in_temp_name   IN VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_save_pos    PLS_INTEGER;
  BEGIN
    -- find table declaration
    l_search_pos := 1;    
    l_pattern := '\WTABLE\s+'||get_owner_name_regexp(in_table_name, in_table_owner)||'(\s|\()';
    LOOP
      EXIT WHEN l_search_pos = 0;              
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'TABLE',
                        in_object_name  => in_table_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;                
    -- find self references   
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WREFERENCES\s+'||get_owner_name_regexp(in_table_name, in_table_owner)||'(\s|\()';
    LOOP
      EXIT WHEN l_search_pos = 0;              
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'TABLE',
                        in_object_name  => in_table_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;                
    -- find create index statement
    l_pattern := '\WON\s+'||get_owner_name_regexp(in_table_name, in_table_owner)||'(\s|\()';
    l_search_pos := io_sql_rec.columns_start_pos;
    LOOP
      EXIT WHEN l_search_pos = 0;              
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'TABLE',
                        in_object_name  => in_table_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;
  END find_table_name;

  -- find all entries for given constraint name 
  PROCEDURE find_constraint_name(
    io_sql_rec         IN OUT NOCOPY gt_sql_rec,
    in_constraint_name IN VARCHAR2,
    in_temp_name       IN VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WCONSTRAINT\s+'||get_parse_only_sql(in_constraint_name, '\s')||'\s*((PRIMARY\s+KEY)|(UNIQUE)|(CHECK)|(FOREIGN\s+KEY))';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'CONSTRAINT',
                        in_object_name  => in_constraint_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                  
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WPARTITION\s+BY\s+REFERENCE\s*\(\s*'||get_parse_only_sql(in_constraint_name)||'\s*\)';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'CONSTRAINT',
                        in_object_name  => in_constraint_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                  
  END find_constraint_name;

  -- find all entries for given constraint name 
  PROCEDURE find_log_group_name(
    io_sql_rec        IN OUT NOCOPY gt_sql_rec,
    in_log_group_name IN VARCHAR2,
    in_temp_name      IN VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WSUPPLEMENTAL\s+LOG\s+GROUP\s+'||get_parse_only_sql(in_log_group_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'LOG_GROUP',
                        in_object_name  => in_log_group_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                  
  END find_log_group_name;
  
  -- find all entries for given constraint name 
  PROCEDURE find_index_name(
    io_sql_rec     IN OUT NOCOPY gt_sql_rec,
    in_index_name  IN VARCHAR2,
    in_index_owner IN VARCHAR2,
    in_temp_name   IN  VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WOIDINDEX\s*'||get_parse_only_sql(in_index_name)||'\s*\(';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'INDEX',
                        in_object_name  => in_index_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WINDEX\s*'||get_owner_name_regexp(in_index_name, in_index_owner, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'INDEX',
                        in_object_name  => in_index_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;                
  END find_index_name;
  

  -- find all entries for given lob column 
  PROCEDURE find_lob_segment_name(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_column_name  IN VARCHAR2,
    in_segment_name IN VARCHAR2,
    in_temp_name    IN VARCHAR2
  )
  AS                        
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WLOB\s*\(\s*'||get_parse_only_sql(in_column_name)||'\s*\)\s+STORE\s+AS\s+(BASICFILE\s+|SECUREFILE\s+)?'||get_parse_only_sql(in_segment_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'LOB',
                        in_object_name  => in_segment_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 3
                      );
    END LOOP;                  
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WSTORE\s+AS\s+(BASICFILE\s+|SECUREFILE\s+)?(LOB|CLOB|BINARY\s+XML)\s+'||get_parse_only_sql(in_segment_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'LOB',
                        in_object_name  => in_segment_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 3
                      );
    END LOOP;                  
  END find_lob_segment_name;
  
  -- find all entries for given nested table 
  PROCEDURE find_nested_table_name(
    io_sql_rec     IN OUT NOCOPY gt_sql_rec,
    in_table_name  IN VARCHAR2,
    in_column_name IN VARCHAR2, 
    in_temp_name   IN VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WSTORE\s+AS\s+'||get_parse_only_sql(in_table_name, '(\W|$)');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'TABLE',
                        in_object_name  => in_table_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;  
  END find_nested_table_name;               

  -- find all substitution entries (CREATE TABLE)
  PROCEDURE find_all_substitutions(
    io_sql_rec   IN OUT NOCOPY gt_sql_rec,
    in_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_lob_rec   cort_exec_pkg.gt_lob_rec;
    l_indx      PLS_INTEGER; 
  BEGIN
    -- find all instances for table name
    find_table_name(
      io_sql_rec     => io_sql_rec,
      in_table_name  => in_table_rec.table_name, 
      in_table_owner => in_table_rec.owner, 
      in_temp_name   => in_table_rec.rename_rec.temp_name
    );
    -- find all named constraints
    FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
      IF in_table_rec.constraint_arr(i).generated = 'USER NAME' THEN
        find_constraint_name(
          io_sql_rec         => io_sql_rec,
          in_constraint_name => in_table_rec.constraint_arr(i).constraint_name, 
          in_temp_name       => in_table_rec.constraint_arr(i).rename_rec.temp_name
        );
      END IF;  
    END LOOP;
    -- find all named log groups
    FOR i IN 1..in_table_rec.log_group_arr.COUNT LOOP
      IF in_table_rec.log_group_arr(i).generated = 'USER NAME' THEN
        find_log_group_name(
          io_sql_rec        => io_sql_rec,
          in_log_group_name => in_table_rec.log_group_arr(i).log_group_name, 
          in_temp_name      => in_table_rec.log_group_arr(i).rename_rec.temp_name
        );
      END IF;  
    END LOOP;
    -- find all indexes 
    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      IF in_table_rec.index_arr(i).rename_rec.generated = 'N' THEN
        IF NOT in_table_rec.constraint_indx_arr.EXISTS(in_table_rec.index_arr(i).index_name) THEN
          find_constraint_name(
            io_sql_rec         => io_sql_rec,
            in_constraint_name => in_table_rec.index_arr(i).index_name, 
            in_temp_name       => in_table_rec.index_arr(i).rename_rec.temp_name
          );
        END IF;
        find_index_name(
          io_sql_rec     => io_sql_rec,
          in_index_name  => in_table_rec.index_arr(i).index_name, 
          in_index_owner => in_table_rec.index_arr(i).owner, 
          in_temp_name   => in_table_rec.index_arr(i).rename_rec.temp_name
        );
        IF NOT g_replaced_names_indx.EXISTS('INDEX:"'||in_table_rec.index_arr(i).rename_rec.temp_name||'"') AND 
           in_table_rec.index_arr(i).constraint_name IS NOT NULL AND
           in_table_rec.constraint_indx_arr.EXISTS(in_table_rec.index_arr(i).constraint_name) 
        THEN
          l_indx := in_table_rec.constraint_indx_arr(in_table_rec.index_arr(i).constraint_name);
          IF g_replaced_names_indx.EXISTS('CONSTRAINT:"'||in_table_rec.constraint_arr(l_indx).rename_rec.temp_name||'"') THEN
            g_replaced_names_indx('INDEX:"'||in_table_rec.constraint_arr(l_indx).rename_rec.temp_name||'"') := in_table_rec.index_arr(i).index_name;
          END IF; 
        END IF;
      END IF;  
    END LOOP;
    -- find all named lob segments
    l_indx := in_table_rec.lob_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      -- for lob columns
      IF in_table_rec.lob_arr(l_indx).rename_rec.generated = 'N' THEN
        find_lob_segment_name(
          io_sql_rec      => io_sql_rec,
          in_column_name  => in_table_rec.lob_arr(l_indx).column_name, 
          in_segment_name => in_table_rec.lob_arr(l_indx).lob_name, 
          in_temp_name    => in_table_rec.lob_arr(l_indx).rename_rec.temp_name
        );
      END IF;  
      l_indx := in_table_rec.lob_arr.NEXT(l_indx);
    END LOOP;
    -- for all nested tables
    FOR i IN 1..in_table_rec.nested_tables_arr.COUNT LOOP
      find_nested_table_name(
        io_sql_rec     => io_sql_rec,
        in_table_name  => in_table_rec.nested_tables_arr(i).table_name, 
        in_column_name => in_table_rec.nested_tables_arr(i).parent_table_column, 
        in_temp_name   => in_table_rec.nested_tables_arr(i).rename_rec.temp_name
      );
    END LOOP;
  END find_all_substitutions;
  
  -- find all substitution entries (CREATE INDEX)
  PROCEDURE find_all_substitutions(
    io_sql_rec   IN OUT NOCOPY gt_sql_rec,
    in_index_rec IN cort_exec_pkg.gt_index_rec,
    in_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_search_pos   PLS_INTEGER;
    l_pattern      VARCHAR2(100);
  BEGIN
    -- find index name
    l_search_pos := 1; 
    l_search_pos := skip_whitespace(io_sql_rec.normalized_sql, l_search_pos);
    l_pattern := get_owner_name_regexp(in_index_rec.index_name, in_index_rec.owner, '\W'); 
    l_search_pos := find_substitutable_name(
                      io_sql_rec      => io_sql_rec,
                      in_object_type  => 'INDEX',
                      in_object_name  => in_index_rec.index_name,
                      in_new_name     => in_index_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );

    -- find table name
    l_pattern := '\WON\s+' || get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\W');
    l_search_pos := io_sql_rec.definition_start_pos;
    l_search_pos := find_substitutable_name(
                      io_sql_rec      => io_sql_rec,
                      in_object_type  => 'TABLE',
                      in_object_name  => in_table_rec.table_name,
                      in_new_name     => in_table_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );
    -- FOR BITMAP JOIN INDEXES                 
    -- find table name
    l_pattern := get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\.');
    -- replace in columns
    l_search_pos := io_sql_rec.columns_start_pos;
    l_search_pos := find_substitutable_name(
                      io_sql_rec      => io_sql_rec,
                      in_object_type  => 'TABLE',
                      in_object_name  => in_table_rec.table_name,
                      in_new_name     => in_table_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );
    -- replace in FROM                
    l_pattern := get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\W');
    l_search_pos := io_sql_rec.columns_end_pos;
    l_search_pos := find_substitutable_name(
                      io_sql_rec      => io_sql_rec,
                      in_object_type  => 'TABLE',
                      in_object_name  => in_table_rec.table_name,
                      in_new_name     => in_table_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );
    -- replace in WHERE                
    l_pattern := get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\.');
    l_search_pos := io_sql_rec.columns_end_pos;
    l_search_pos := find_substitutable_name(
                      io_sql_rec      => io_sql_rec,
                      in_object_type  => 'TABLE',
                      in_object_name  => in_table_rec.table_name,
                      in_new_name     => in_table_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );

  END find_all_substitutions;  
  
  PROCEDURE find_all_substitutions(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_sequence_rec IN cort_exec_pkg.gt_sequence_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    -- find all instances for table name
    l_search_pos := 1;    
    l_pattern := '\WSEQUENCE\s+'||get_owner_name_regexp(in_sequence_rec.sequence_name, in_sequence_rec.owner, '(\W|$)');
    l_search_pos := find_substitutable_name(
                      io_sql_rec      => io_sql_rec,
                      in_object_type  => 'SEQUENCE',
                      in_object_name  => in_sequence_rec.sequence_name,
                      in_new_name     => in_sequence_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );
  END find_all_substitutions;

  PROCEDURE find_all_substitutions(
    io_sql_rec  IN OUT NOCOPY gt_sql_rec,
    in_type_rec IN cort_exec_pkg.gt_type_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    -- find all instances for table name
    l_search_pos := 1;    
    l_pattern := '\WTYPE\s+'||get_owner_name_regexp(in_type_rec.type_name, in_type_rec.owner, '\W');

    l_search_pos := find_substitutable_name(
                      io_sql_rec      => io_sql_rec,
                      in_object_type  => 'TYPE',
                      in_object_name  => in_type_rec.type_name,
                      in_new_name     => in_type_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );

    l_pattern := '\WCONSTRUCTOR\sFUNCTION\s+'||get_owner_name_regexp(in_type_rec.type_name, in_type_rec.owner, '\W');

    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitutable_name(
                        io_sql_rec      => io_sql_rec,
                        in_object_type  => 'METHOD',
                        in_object_name  => in_type_rec.type_name,
                        in_new_name     => in_type_rec.rename_rec.temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;                  
                    
  END find_all_substitutions;

  
  -- Public declararions

  /* Replaces all comments and string literals with blank symbols */
  FUNCTION parse_sql(
    in_sql   IN   CLOB
  )
  RETURN gt_sql_rec
  AS
    l_search_pos                PLS_INTEGER;
    l_key                       VARCHAR2(32767);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_expr_start_pos            PLS_INTEGER;
    l_expr_end_pos              PLS_INTEGER;
    l_expr_start_pattern        VARCHAR2(100);
    l_expr_end_pattern          VARCHAR2(100);
    l_expr_type                 VARCHAR2(20);
    l_expr                      CLOB;
    l_space                     CLOB := ' ';
    l_quoted_name               VARCHAR2(100);
    l_expr_cnt                  PLS_INTEGER;
    l_lexical_unit_rec          gt_lexical_unit_rec;
    l_sql_rec                   gt_sql_rec;
  BEGIN
    l_sql_rec.original_sql := in_sql;
    l_sql_rec.normalized_sql := null;

    l_search_pos := 1;
    l_expr_cnt := 0;

    LOOP
      l_expr_start_pattern := q'{(/\*)|(--)|((N|n)?')|((N|n)?(Q|q)'\S)|"}';
      l_key_start_pos := REGEXP_INSTR(l_sql_rec.original_sql, l_expr_start_pattern, l_search_pos, 1, 0);
      l_key_end_pos   := REGEXP_INSTR(l_sql_rec.original_sql, l_expr_start_pattern, l_search_pos, 1, 1);
      EXIT WHEN l_key_start_pos = 0 OR l_key_start_pos IS NULL
             OR l_key_end_pos = 0 OR l_key_end_pos IS NULL
             OR l_expr_cnt > 10000;
      
      l_expr := UPPER(SUBSTR(l_sql_rec.original_sql, l_search_pos, l_key_start_pos-l_search_pos));
      
      l_lexical_unit_rec.unit_type := 'SQL';
      l_lexical_unit_rec.text := l_expr;
      l_lexical_unit_rec.start_position := l_search_pos;
      l_lexical_unit_rec.end_position := l_key_start_pos;
      l_sql_rec.lexical_units_arr(l_search_pos) := l_lexical_unit_rec;

      l_sql_rec.normalized_sql := l_sql_rec.normalized_sql || l_expr;

      l_key := SUBSTR(l_sql_rec.original_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
      CASE
      WHEN l_key = '/*' THEN
        l_expr_end_pattern := '\*/';
        l_expr_start_pos := REGEXP_INSTR(l_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 2;
        
        
        l_expr_type := 'COMMENT';
      WHEN l_key = '--' THEN
        l_expr_end_pattern := '$';
        l_expr_start_pos := REGEXP_INSTR(l_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0, 'm');
        l_expr_end_pos := l_expr_start_pos;
        l_expr_type := 'LINE COMMENT';
      WHEN l_key = '"' THEN
        l_expr_end_pattern := '"';
        l_expr_start_pos := REGEXP_INSTR(l_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 1;
        l_expr_type := 'QUOTED NAME';
      WHEN l_key = 'N'''
        OR l_key = 'n'''
        OR l_key = '''' THEN
        l_expr_end_pattern := '''';
        l_expr_start_pos := REGEXP_INSTR(l_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 1;
        l_expr_type := 'LITERAL';
      WHEN REGEXP_LIKE(l_key, q'{(N|n)?(Q|q)'\S}') THEN
        CASE SUBSTR(l_key, -1)
          WHEN '{' THEN l_expr_end_pattern := '}''';
          WHEN '[' THEN l_expr_end_pattern := ']''';
          WHEN '(' THEN l_expr_end_pattern := ')''';
          WHEN '<' THEN l_expr_end_pattern := '>''';
          ELSE l_expr_end_pattern := SUBSTR(l_key, -1)||'''';
        END CASE;
        l_expr_start_pos := INSTR(l_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos);
        l_expr_end_pos := l_expr_start_pos + 2;
        l_expr_type := 'LITERAL';
      END CASE;
      IF l_expr_start_pos = 0 THEN
        cort_exec_pkg.raise_error( 'Invalid SQL');
      END IF;
      l_expr := SUBSTR(l_sql_rec.original_sql, l_key_start_pos, l_expr_end_pos - l_key_start_pos);
      
      l_lexical_unit_rec.unit_type := l_expr_type;
      l_lexical_unit_rec.text := UPPER(l_expr);
      l_lexical_unit_rec.start_position := l_key_start_pos;
      l_lexical_unit_rec.end_position := l_expr_end_pos;
      l_sql_rec.lexical_units_arr(l_key_start_pos) := l_lexical_unit_rec;
          
      CASE  
      WHEN l_expr_type IN ('COMMENT','LINE COMMENT') THEN
        l_expr := RPAD(l_space, LENGTH(l_expr), l_space);
      WHEN l_expr_type = 'QUOTED NAME' THEN
        -- if simple name quoted then simplify it
        l_quoted_name := SUBSTR(l_expr, 2, LENGTH(l_expr)-2);
        IF is_simple_name(l_quoted_name) THEN
          l_expr := l_space||UPPER(l_quoted_name)||l_space;
        END IF;
      WHEN l_expr_type = 'LITERAL' THEN
        l_expr := RPAD(l_space, LENGTH(l_expr), l_space);
      ELSE
        NULL;
      END CASE;

      l_sql_rec.normalized_sql := l_sql_rec.normalized_sql || l_expr;

      l_search_pos := l_expr_end_pos;
      l_expr_cnt := l_expr_cnt + 1;
    END LOOP;

    l_expr := UPPER(SUBSTR(l_sql_rec.original_sql, l_search_pos));
      
    l_lexical_unit_rec.unit_type := 'SQL';
    l_lexical_unit_rec.text := l_expr;
    l_lexical_unit_rec.start_position := l_search_pos;
    l_lexical_unit_rec.end_position := LENGTH(l_sql_rec.original_sql);
    l_sql_rec.lexical_units_arr(l_search_pos) := l_lexical_unit_rec;
    
    l_sql_rec.normalized_sql := l_sql_rec.normalized_sql || l_expr;


    RETURN l_sql_rec;
  END parse_sql;
  
  FUNCTION get_normalized_sql(
    in_sql_rec      IN gt_sql_rec,
    in_quoted_names IN BOOLEAN DEFAULT TRUE,
    in_str_literals IN BOOLEAN DEFAULT TRUE,
    in_comments     IN BOOLEAN DEFAULT TRUE
  )
  RETURN CLOB
  AS
    l_indx         PLS_INTEGER;
    l_sql          CLOB;
    l_len          PLS_INTEGER;
    l_replace      CLOB;
    l_space        CLOB := ' ';
    l_quoted_name  VARCHAR2(32767);
  BEGIN
    l_indx := in_sql_rec.lexical_units_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      l_len := LENGTH(in_sql_rec.lexical_units_arr(l_indx).text);
      CASE in_sql_rec.lexical_units_arr(l_indx).unit_type 
      WHEN 'QUOTED NAME' THEN  
        IF in_quoted_names THEN
          l_replace := in_sql_rec.lexical_units_arr(l_indx).text;
          -- if simple name quoted then simplify it
          l_quoted_name := SUBSTR(l_replace, 2, LENGTH(l_replace)-2);
          IF is_simple_name(l_quoted_name) THEN
            l_replace := l_space||UPPER(l_quoted_name)||l_space;
          END IF;
        ELSE
          l_replace := RPAD(l_space, l_len, l_space);
        END IF;  
      WHEN 'LITERAL' THEN
        IF in_str_literals THEN
          l_replace := in_sql_rec.lexical_units_arr(l_indx).text;
        ELSE
          l_replace := RPAD(l_space, l_len, l_space);
        END IF;
      WHEN 'COMMENT' THEN
        IF in_comments THEN
          l_replace := in_sql_rec.lexical_units_arr(l_indx).text;
        ELSE
          l_replace := RPAD(l_space, l_len, l_space);
        END IF;
      WHEN 'LINE COMMENT' THEN      
        IF in_comments THEN
          l_replace := in_sql_rec.lexical_units_arr(l_indx).text;
        ELSE
          l_replace := RPAD(l_space, l_len, l_space);
        END IF;
      ELSE
        l_replace := in_sql_rec.lexical_units_arr(l_indx).text;
      END CASE; 
      l_sql := l_sql||l_replace;
      l_indx := in_sql_rec.lexical_units_arr.NEXT(l_indx); 
    END LOOP;
    RETURN l_sql;
  END get_normalized_sql;

  -- parse cort hints
  PROCEDURE parse_params(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    io_params_rec   IN OUT NOCOPY cort_params_pkg.gt_params_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_hint_regexp               VARCHAR2(1000);
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_text                      CLOB;
    l_key                       VARCHAR2(4000);
    l_value                     CLOB;
    l_indx                      PLS_INTEGER;
    l_hints                     arrays.gt_xlstr_arr;
    l_name_regexp               VARCHAR2(500);
    l_value_regexp              VARCHAR2(500);
    l_params_arr                cort_params_pkg.gt_params_arr;
    l_param_names_arr           arrays.gt_str_arr;
    l_name                      VARCHAR2(100);
  BEGIN
    -- data source params:
    --#[release]SQL= 
    -- or 
    --#[release]FILTER= 
    -- where [release] is current release value wrapped with square brackets []. This clause is optional

    l_hint_regexp := cort_params_pkg.gc_prefix;

    l_regexp := cort_params_pkg.gc_prefix||'(\['||
                get_regexp_const(cort_pkg.get_current_release)||'\])?('||
                cort_params_pkg.gc_data_filter_regexp||'|'||
                cort_params_pkg.gc_sql_regexp||')';

    -- find all cort hints
    l_indx := io_sql_rec.lexical_units_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF io_sql_rec.lexical_units_arr(l_indx).unit_type IN ('LINE COMMENT', 'COMMENT') THEN
        CASE io_sql_rec.lexical_units_arr(l_indx).unit_type 
        WHEN 'LINE COMMENT' THEN
          l_text := SUBSTR(io_sql_rec.lexical_units_arr(l_indx).text, 3);
        WHEN 'COMMENT' THEN
          l_text := SUBSTR(io_sql_rec.lexical_units_arr(l_indx).text, 3, LENGTH(io_sql_rec.lexical_units_arr(l_indx).text)-4);
        END CASE;
        IF io_sql_rec.lexical_units_arr(l_indx).start_position BETWEEN io_sql_rec.cort_param_start_pos 
                                                                   AND io_sql_rec.cort_param_end_pos 
        THEN                                            
          l_key_start_pos := REGEXP_INSTR(l_text, l_hint_regexp, 1, 1, 0, 'i');
          l_key_end_pos := REGEXP_INSTR(l_text, l_hint_regexp, 1, 1, 1, 'i');
          IF l_key_start_pos = 1 THEN
            l_hints(l_hints.COUNT+1) := SUBSTR(l_text, l_key_end_pos);
          END IF;  
        END IF;

        IF io_sql_rec.lexical_units_arr(l_indx).start_position >= NVL(io_sql_rec.columns_end_pos,io_sql_rec.definition_start_pos)  
        THEN                                            
          l_key_start_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 0, 'i');
          l_key_end_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 1, 'i');
          IF l_key_start_pos = 1 THEN
            l_key := REGEXP_SUBSTR(l_text, l_regexp, 1, 1, 'i', 2);
            CASE l_key
            WHEN cort_params_pkg.gc_data_filter_regexp THEN 
              io_sql_rec.data_filter := SUBSTR(l_text, l_key_end_pos);
            WHEN cort_params_pkg.gc_sql_regexp THEN 
              io_sql_rec.data_source := SUBSTR(l_text, l_key_end_pos);
            END CASE;  
          END IF;
        END IF;    
        
      END IF;  
      l_indx := io_sql_rec.lexical_units_arr.NEXT(l_indx); 
    END LOOP;

    IF l_hints.COUNT = 0 THEN 
      RETURN;
    END IF;   
    
    l_params_arr := cort_params_pkg.rec_to_array(io_params_rec);
    l_param_names_arr := cort_params_pkg.get_param_names;
    
    FOR i IN 1..l_param_names_arr.COUNT LOOP
      l_name_regexp := l_name_regexp||get_regexp_const(l_param_names_arr(i))||'|';        
    END LOOP; 
    l_name_regexp := TRIM('|' FROM l_name_regexp);
    l_name_regexp := '('||l_name_regexp||')(=)';

    FOR i IN 1..l_hints.COUNT LOOP
      l_search_pos := 1;
      debug('Parsing param string '||l_hints(i));
      WHILE l_search_pos > 0 AND l_search_pos < LENGTH(l_hints(i)) LOOP    
        l_name := UPPER(REGEXP_SUBSTR(l_hints(i), l_name_regexp, l_search_pos, 1, 'im', 1));
        IF l_name IS NOT NULL THEN 
          debug('Found param '||l_name);
          l_search_pos := REGEXP_INSTR(l_hints(i), l_name_regexp, l_search_pos, 1, 1, 'im');
          l_value_regexp := l_params_arr(l_name).get_regexp;
          l_value := REGEXP_SUBSTR(l_hints(i), l_value_regexp, l_search_pos, 1, 'im');
          IF l_value IS NULL THEN  
            cort_log_pkg.debug('wrong param '||l_name||' value. Expected values = '||l_value_regexp);
            l_search_pos := l_search_pos + length(l_name);
          ELSE 
            l_params_arr(l_name).set_value(l_value);
            cort_log_pkg.debug('param '||l_name||' set to '||l_value);
            l_search_pos := REGEXP_INSTR(l_hints(i), l_value_regexp, l_search_pos, 1, 1, 'im');
          END IF;  
        ELSE  
          EXIT;  
        END IF;  
      END LOOP;  

    END LOOP;

    io_params_rec := cort_params_pkg.array_to_rec(l_params_arr);
            
  END parse_params;
  
  -- Parse column definition start/end positions
  PROCEDURE parse_table_sql(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_table_name IN VARCHAR2,
    in_owner_name IN VARCHAR2
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
  BEGIN
    initial_object_parse(
      io_sql_rec       => io_sql_rec,
      in_object_type   => 'TABLE',
      in_object_name   => in_table_name,
      in_object_owner  => in_owner_name
    );

    io_sql_rec.columns_start_pos := io_sql_rec.definition_start_pos;
    io_sql_rec.columns_end_pos := io_sql_rec.definition_start_pos;

    l_parse_only_sql := get_normalized_sql(
                          in_sql_rec      => io_sql_rec, 
                          in_quoted_names => FALSE,
                          in_str_literals => FALSE,
                          in_comments     => FALSE
                       );
  
    l_search_pos := io_sql_rec.definition_start_pos;
    l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
    l_regexp := '\('; -- find a open bracket
    l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos); 
    l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
    IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN    
      io_sql_rec.columns_start_pos := l_key_end_pos;
      debug('Relational columns definition is found');
      -- find a close bracket
      io_sql_rec.columns_end_pos := get_closed_bracket(
                                      in_sql             => l_parse_only_sql,
                                      in_search_position => io_sql_rec.columns_start_pos
                                    );
    ELSE
      debug('Relational columns definition is not found');
      -- find object properties definition 
      l_search_pos := io_sql_rec.definition_start_pos;
      l_regexp := '\WOF\W'; -- check that this is object table
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos);
      IF l_key_start_pos = l_search_pos AND l_key_start_pos > 0 THEN 
        debug('Object table definition is found');
        l_search_pos := l_key_start_pos;
        l_regexp := '\(';
        io_sql_rec.columns_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); -- find a open bracket
        IF io_sql_rec.columns_start_pos > 0 THEN
          debug('Object table columns definition is found');
          -- This contingently position for columns definition. It could be something else.
          -- find a close bracket
          io_sql_rec.columns_end_pos := get_closed_bracket(
                                               in_sql             => l_parse_only_sql,
                                               in_search_position => io_sql_rec.columns_start_pos
                                             );
        ELSE
          debug('Object table columns definition is not found');
        END IF;
      ELSE
        debug('Object table definition is not found');
      END IF;  
    END IF;
  END parse_table_sql;
  
  -- Parse index definition to find table name 
  PROCEDURE parse_index_sql(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_index_name IN VARCHAR2,
    in_owner_name IN VARCHAR2
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_normalized_sql            CLOB;
    l_parse_only_sql            CLOB;
    l_name                      VARCHAR2(1000);
  BEGIN
    initial_object_parse(
      io_sql_rec       => io_sql_rec,
      in_object_type   => 'INDEX',
      in_object_name   => in_index_name,
      in_object_owner  => in_owner_name
    );
    
    l_search_pos := io_sql_rec.definition_start_pos;
    l_search_pos := skip_whitespace(io_sql_rec.normalized_sql, l_search_pos);
    l_regexp := '(ON\s+CLUSTER)\W';
    l_key_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos);
    IF l_key_start_pos > 0 AND l_key_start_pos = l_search_pos THEN
      debug('Found ON CLUSTER');
      io_sql_rec.is_cluster := TRUE;
      l_key_end_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
      io_sql_rec.table_definition_start_pos := l_key_end_pos; 
      l_search_pos := l_key_end_pos;
    ELSE  
      io_sql_rec.is_cluster := FALSE;
      l_search_pos := skip_whitespace(io_sql_rec.normalized_sql, l_search_pos);
      l_regexp := '(ON)\W';
      l_key_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos);
      IF l_key_start_pos > 0 AND l_key_start_pos = l_search_pos THEN
        debug('Found ON');
        l_key_end_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
        io_sql_rec.table_definition_start_pos := l_key_end_pos; 
        l_search_pos := l_key_end_pos;
      ELSE
        debug('Index ON keyword not found');
        cort_exec_pkg.raise_error( 'Index ON keyword not found');
      END IF;  
    END IF;
    
    l_name := read_next_name(
                in_sql        => io_sql_rec.normalized_sql,
                io_search_pos => l_search_pos
             );
             
    IF l_name IS NOT NULL THEN
      l_search_pos := skip_whitespace(io_sql_rec.normalized_sql, l_search_pos);
      l_regexp := '\.'; -- find a dot
      l_key_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos);
      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN
        io_sql_rec.table_owner := l_name;
        l_search_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 1);
        l_name := read_next_name(
                    in_sql        => io_sql_rec.normalized_sql,
                    io_search_pos => l_search_pos
                  );
        io_sql_rec.table_name := l_name;
      ELSE
        io_sql_rec.table_name := l_name;
      END IF;
    END IF;
    
    
    IF NOT io_sql_rec.is_cluster THEN
      l_parse_only_sql := get_normalized_sql(
                            in_sql_rec      => io_sql_rec,
                            in_quoted_names => FALSE,
                            in_str_literals => FALSE,
                            in_comments     => FALSE
                         );
      l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
      l_regexp := '\('; -- find a open bracket
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos); 
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN    
        io_sql_rec.columns_start_pos := l_key_end_pos;
        debug('Relational columns definition is found');
        -- find a close bracket
        io_sql_rec.columns_end_pos := get_closed_bracket(
                                             in_sql             => l_parse_only_sql,
                                             in_search_position => io_sql_rec.columns_start_pos
                                           );
      END IF;     
    END IF;                                
  END parse_index_sql;
     
  -- Parse sequence definition to find table name 
  PROCEDURE parse_sequence_sql(
    io_sql_rec       IN OUT NOCOPY gt_sql_rec,
    in_sequence_name IN VARCHAR2,
    in_owner_name    IN VARCHAR2
  )
  AS
  BEGIN
    initial_object_parse(
      io_sql_rec       => io_sql_rec,
      in_object_type   => 'SEQUENCE',
      in_object_name   => in_sequence_name,
      in_object_owner  => in_owner_name
    );
  END parse_sequence_sql;

  -- Parse type definition to find table name 
  PROCEDURE parse_type_sql(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_type_name  IN VARCHAR2,
    in_owner_name IN VARCHAR2
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_end_pos               PLS_INTEGER;
    l_normalized_sql            CLOB;
  BEGIN
    initial_object_parse(
      io_sql_rec       => io_sql_rec,
      in_object_type   => 'TYPE',
      in_object_name   => in_type_name,
      in_object_owner  => in_owner_name
    );
  END parse_type_sql;
  
  PROCEDURE parse_create_sql(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  AS
  BEGIN
    CASE in_object_type 
    WHEN 'TABLE' THEN
      parse_table_sql(
        io_sql_rec    => io_sql_rec,
        in_table_name => in_object_name,
        in_owner_name => in_object_owner
      );
    WHEN 'INDEX' THEN
      parse_index_sql(
        io_sql_rec    => io_sql_rec,
        in_index_name => in_object_name,
        in_owner_name => in_object_owner
      );
    WHEN 'SEQUENCE' THEN
      parse_sequence_sql(
        io_sql_rec       => io_sql_rec,
        in_sequence_name => in_object_name,
        in_owner_name    => in_object_owner
      );
    WHEN 'TYPE' THEN
      parse_type_sql(
        io_sql_rec    => io_sql_rec,
        in_type_name  => in_object_name,
        in_owner_name => in_object_owner
      );
    END CASE;
  END parse_create_sql;

  PROCEDURE parse_as_select(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
  BEGIN
    io_sql_rec.as_select_flag := FALSE; 
    l_parse_only_sql := get_normalized_sql(
                          in_sql_rec      => io_sql_rec,
                          in_quoted_names => FALSE,
                          in_str_literals => FALSE,
                          in_comments     => FALSE
                       );

    debug('parse_as_select', l_parse_only_sql);
    
    IF io_sql_rec.columns_end_pos > 0 THEN
      l_search_pos := io_sql_rec.columns_end_pos;
    ELSE  
      l_search_pos := io_sql_rec.definition_start_pos;
    END IF;

    IF l_search_pos > 0 THEN
      -- find AS <subquery> clause 
      l_regexp := '\W(AS)(\s|\()+\s*(SELECT|WITH)\W'; -- AS SELECT|AS WITH
      io_sql_rec.as_select_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1);
      IF io_sql_rec.as_select_start_pos > 0 THEN
        debug('AS SELECT clause found');
        io_sql_rec.subquery_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
        io_sql_rec.as_select_flag := TRUE;
      ELSE
        debug('AS SELECT clause not found');
      END IF;
    END IF;      
  END parse_as_select;

  -- set subquery return 0 rows
  PROCEDURE modify_as_select(
    io_sql_rec IN OUT NOCOPY gt_sql_rec
  )
  AS
  BEGIN
    IF io_sql_rec.subquery_start_pos > 0 THEN
      io_sql_rec.original_sql := 
      SUBSTR(io_sql_rec.original_sql, 1, io_sql_rec.subquery_start_pos-1)||CHR(10)||
      'SELECT *'||CHR(10)|| 
      '  FROM ('||CHR(10)||SUBSTR(io_sql_rec.original_sql, io_sql_rec.subquery_start_pos)||CHR(10)||
      '       )'||CHR(10)||
      ' WHERE 1=0';
     END IF;
  END modify_as_select;
  
  -- return TRUE if as_select subquery contains table name
  FUNCTION as_select_from(
    in_sql_rec    IN gt_sql_rec,
    in_table_name IN VARCHAR
  )
  RETURN BOOLEAN
  AS
    l_search VARCHAR2(100);
  BEGIN
    IF is_simple_name(in_table_name) THEN
      l_search := '\s+'||get_regexp_const(in_table_name)||'\W';
    ELSE
      l_search := '"'||get_regexp_const(in_table_name)||'"';
    END IF;
    RETURN in_sql_rec.subquery_start_pos > 0 AND REGEXP_INSTR(in_sql_rec.normalized_sql, l_search, in_sql_rec.subquery_start_pos) > 0; 
  END as_select_from;
  
  -- determines partitions position
  PROCEDURE parse_partitioning(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_parse_only_sql            CLOB;
    
    PROCEDURE parse_subpartitioning
    AS
    BEGIN
      -- find subpartitioning  
      l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
      l_regexp := 'SUBPARTITION\s+BY\s+(LIST|RANGE|HASH)\s*\(';
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
      IF l_key_start_pos = l_search_pos THEN
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1);
        l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
        io_sql_rec.subpartition_type := SUBSTR(l_parse_only_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, '\)', l_search_pos, 1, 1);
      END IF;

      debug('subpartitioning_type = '||io_sql_rec.subpartition_type);
    

      -- if subpartitioned
      IF io_sql_rec.subpartition_type IS NOT NULL THEN
        -- skip template
        l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
        l_regexp := '(SUBPARTITION\s+TEMPLATE)\W';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
        IF l_key_start_pos = l_search_pos THEN
          l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
          l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
          l_regexp := '\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
          ELSIF io_sql_rec.subpartition_type = 'HASH' THEN
            l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
            l_regexp := '(SUBPARTITIONS\s+[0-9]+)\W';
            IF REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0) = l_search_pos THEN
              l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
              -- skip tablespaces
              l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
              l_regexp := 'STORE\s+IN\s*\(';
              l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
              IF l_key_start_pos = l_search_pos THEN
                l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
                l_search_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
              END IF;
            END IF;
          END IF;
        END IF;
      END IF;  

    END parse_subpartitioning;
    
  BEGIN
   l_parse_only_sql := get_normalized_sql(
                           in_sql_rec      => io_sql_rec,
                           in_quoted_names => FALSE,
                           in_str_literals => FALSE,
                           in_comments     => FALSE
                        );

    l_search_pos := io_sql_rec.columns_end_pos+1;
    l_regexp := '\W(PARTITION\s+BY\s+(LIST|RANGE|HASH|REFERENCE|SYSTEM))\W';
    l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos);
    IF l_key_start_pos > 0 THEN
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 2);
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 2);
      io_sql_rec.partition_type := SUBSTR(l_parse_only_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
      l_search_pos := l_key_end_pos; 
    END IF;

    debug('partitioning_type = '||io_sql_rec.partition_type);
    
    
    IF io_sql_rec.partition_type IS NOT NULL THEN
      -- skip column/reference definition 
      IF io_sql_rec.partition_type <> 'SYSTEM' THEN
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, '\(', l_search_pos, 1, 0);
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, '\)', l_search_pos, 1, 1);
      END IF;
      
      CASE io_sql_rec.partition_type
      WHEN 'RANGE' THEN
        -- skip interval definition
        l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
        l_regexp := 'INTERVAL\s*\(';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
        IF l_key_start_pos = l_search_pos THEN
          l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
          l_search_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
          -- skip tablespaces
          l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
          l_regexp := 'STORE\s+IN\s*\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
          END IF;
        END IF;
        -- find subpartitioning  
        parse_subpartitioning;
      WHEN 'LIST' THEN
        -- find subpartitioning  
        parse_subpartitioning;
      WHEN 'HASH' THEN
        -- find subpartitioning  
        parse_subpartitioning;
        l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
        l_regexp := '(PARTITIONS\s+[0-9]+)\W';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0); 
        IF l_key_start_pos = l_search_pos THEN
          io_sql_rec.partitions_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1);
          l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
          -- skip tablespaces
          l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
          l_regexp := 'STORE\s+IN\s*\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
          END IF;
          -- skip overflow tablespace
          l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
          l_regexp := 'OVERFLOW\s+STORE\s+IN\s*\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
          END IF;
          io_sql_rec.partitions_end_pos := l_search_pos; 
          RETURN;
        END IF;  
      WHEN 'SYSTEM' THEN
        -- skip store in
        l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
        l_regexp := '(PARTITIONS\s+[0-9]+)\W';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0); 
        IF l_key_start_pos = l_search_pos THEN
          io_sql_rec.partitions_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1); 
          io_sql_rec.partitions_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
          RETURN;
        END IF;  
      ELSE NULL;
      END CASE;
      
      l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
      l_regexp := '\(';
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0); 
      IF l_key_start_pos = l_search_pos THEN
        -- include brackets into partitions definition
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
        io_sql_rec.partitions_start_pos := l_search_pos-1; 
        io_sql_rec.partitions_end_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
      END IF;
    END IF;
  END parse_partitioning;

  -- replaces partitions definition in original_sql
  PROCEDURE replace_partitions_sql(
    io_sql_rec       IN OUT NOCOPY gt_sql_rec,
    in_partition_sql IN CLOB
  )
  AS
  BEGIN
    IF io_sql_rec.partitions_start_pos > 0 AND 
       io_sql_rec.partitions_end_pos > 0 
    THEN
      io_sql_rec.original_sql := SUBSTR(io_sql_rec.original_sql, 1, io_sql_rec.partitions_start_pos-1)||
                                 '('||CHR(10)||in_partition_sql||CHR(10)||')'||
                                 SUBSTR(io_sql_rec.original_sql, io_sql_rec.partitions_end_pos);
    END IF;            
  END replace_partitions_sql;

  -- parses columns positions and cort-values
  PROCEDURE parse_columns(
    io_sql_rec        IN OUT NOCOPY gt_sql_rec,
    io_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
    l_next_position             PLS_INTEGER;
  BEGIN
    l_parse_only_sql := get_normalized_sql(
                          in_sql_rec      => io_sql_rec,
                          in_quoted_names => FALSE,
                          in_str_literals => FALSE,
                          in_comments     => FALSE
                       );
  
    l_search_pos := io_sql_rec.definition_start_pos;
    
    
    IF io_table_rec.table_type IS NULL THEN     
      l_search_pos := skip_whitespace(l_parse_only_sql, l_search_pos);
      l_regexp := '\('; -- find a open bracket
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos); 
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN    
        io_sql_rec.columns_start_pos := l_key_end_pos;
        debug('Relational columns definition is found');
        -- find a close bracket
        io_sql_rec.columns_end_pos := get_closed_bracket(
                                             in_sql             => l_parse_only_sql,
                                             in_search_position => io_sql_rec.columns_start_pos
                                           );
      ELSE
        debug('Relational columns definition is not found');
      END IF;
    ELSE    
      -- find object properties definition 
      l_search_pos := io_sql_rec.definition_start_pos;
    
      l_regexp := '\WOF\W'; -- check that this is object table
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos); 
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); -- find end of object table definition

      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN 
        debug('Object table definition is found');
        l_search_pos := l_key_end_pos;
        l_regexp := '\(';
        io_sql_rec.columns_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); -- find a open bracket
        IF io_sql_rec.columns_start_pos > 0 THEN
          debug('Object table columns definition is found');
          -- This contingently position for columns definition. It could be something else.
          -- find a close bracket
          io_sql_rec.columns_end_pos := get_closed_bracket(
                                               in_sql             => l_parse_only_sql,
                                               in_search_position => io_sql_rec.columns_start_pos
                                             );
        ELSE
          debug('Object table columns definition is not found');
        END IF;
      ELSE
        debug('Object table definition is not found');
      END IF;  
    END IF;
    
    
    l_search_pos := io_sql_rec.columns_start_pos;
    IF io_sql_rec.columns_start_pos > 0 AND
       io_sql_rec.columns_end_pos > io_sql_rec.columns_start_pos
    THEN  
      FOR i IN 1..io_table_rec.column_arr.COUNT LOOP
        IF io_table_rec.column_arr(i).hidden_column = 'NO' THEN
          IF io_table_rec.table_type IS NULL THEN
            l_regexp := get_column_regexp(in_column_rec => io_table_rec.column_arr(i));
          ELSE
            l_regexp := get_parse_only_sql(io_table_rec.column_arr(i).column_name, '\s')||'\s*';
          END IF;  
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, null, 1);
          l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, null, 1);
          IF l_key_start_pos > 0 THEN
            io_table_rec.column_arr(i).sql_start_position := l_key_start_pos;
            io_table_rec.column_arr(i).sql_end_position := l_key_end_pos;
--            l_search_pos := l_key_end_pos;
            debug('Parsing: Column '||io_table_rec.column_arr(i).column_name||' startpos = '||l_key_start_pos);        
          ELSE
            debug('Parsing: Column '||io_table_rec.column_arr(i).column_name||' not found. Regexp = '||l_regexp);        
          END IF;
        END IF;
      END LOOP;

      l_next_position := io_sql_rec.columns_end_pos;
      FOR i IN REVERSE 1..io_table_rec.column_arr.COUNT LOOP
        IF io_table_rec.column_arr(i).hidden_column = 'NO' AND io_table_rec.column_arr(i).sql_start_position IS NOT NULL THEN
          io_table_rec.column_arr(i).sql_next_start_position := l_next_position;
--          debug('Parsing: Column '||io_table_rec.column_arr(i).column_name||' startpos = '||l_key_start_pos||', next startpos = '||l_next_position);        
          l_next_position := io_table_rec.column_arr(i).sql_start_position;
        END IF;
      END LOOP;
    END IF;  
    
    parse_column_cort_values(
      io_sql_rec    => io_sql_rec,
      io_table_rec  => io_table_rec
    );
  END parse_columns;
  
  -- replaces table name and all names of existing depending objects (constraints, log groups, indexes, lob segments) 
  PROCEDURE replace_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    io_sql_rec   IN OUT NOCOPY gt_sql_rec,
    out_sql      OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    -- get all names in g_normalized sql
    find_all_substitutions(
      io_sql_rec   => io_sql_rec,
      in_table_rec => in_table_rec
    );

    -- when use normalized SQL then text in check constaints is also normalized
    -- need to normalize it when we read the from dictionary before comparison
    -- io_sql := get_normalized_sql;

    int_replace_names(
      in_sql_rec => io_sql_rec,
      out_sql    => out_sql
    );
  END replace_names;

  -- replaces index and table names for CREATE INDEX statement 
  PROCEDURE replace_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    in_index_rec IN cort_exec_pkg.gt_index_rec,
    io_sql_rec   IN OUT NOCOPY gt_sql_rec, 
    out_sql      OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    -- get all names
    find_all_substitutions(
      io_sql_rec   => io_sql_rec,
      in_index_rec => in_index_rec,
      in_table_rec => in_table_rec
    );

    int_replace_names(
      in_sql_rec => io_sql_rec,
      out_sql    => out_sql
   );
  END replace_names;

  -- replaces sequence name 
  PROCEDURE replace_names(
    in_sequence_rec IN cort_exec_pkg.gt_sequence_rec,
    io_sql_rec      IN OUT NOCOPY gt_sql_rec, 
    out_sql         OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    -- get all names
    find_all_substitutions(
      io_sql_rec      => io_sql_rec,
      in_sequence_rec => in_sequence_rec
    );

    int_replace_names(
      in_sql_rec => io_sql_rec,
      out_sql    => out_sql
   );
  END replace_names;

  -- replaces type name 
  PROCEDURE replace_names(
    in_type_rec IN cort_exec_pkg.gt_type_rec,
    io_sql_rec  IN OUT NOCOPY gt_sql_rec, 
    out_sql     OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    -- get all names
    find_all_substitutions(
      io_sql_rec  => io_sql_rec,
      in_type_rec => in_type_rec
    );

    int_replace_names(
      in_sql_rec => io_sql_rec,
      out_sql    => out_sql
   );
  END replace_names;


  PROCEDURE cleanup
  AS
  BEGIN
    g_replaced_names_indx.DELETE;
  END cleanup;
  
  -- replaces names in expression  
  PROCEDURE update_expression(
    io_expression      IN OUT NOCOPY VARCHAR2,
    in_replace_names   IN arrays.gt_name_indx
  )
  AS
    l_expr       VARCHAR2(32767);
    l_sql_rec    gt_sql_rec;
    l_col_name   VARCHAR2(200);
    l_indx       VARCHAR2(20);
    l_new_name   arrays.gt_name;
    l_search_pos PLS_INTEGER;
    l_key_start  PLS_INTEGER;
    l_key_end    PLS_INTEGER;
    l_pattern    VARCHAR2(200);
    l_int_idx    PLS_INTEGER;
    l_text_arr   gt_lexical_unit_arr;
    l_text_rec   gt_lexical_unit_rec;
  BEGIN
    debug('update_expression', io_expression);
    
    l_sql_rec := parse_sql(io_expression);
    
    l_indx := l_sql_rec.lexical_units_arr.LAST;
    WHILE l_indx IS NOT NULL LOOP
      IF l_sql_rec.lexical_units_arr(l_indx).unit_type = 'QUOTED NAME' THEN
        l_col_name := SUBSTR(l_sql_rec.lexical_units_arr(l_indx).text, 2, LENGTH(l_sql_rec.lexical_units_arr(l_indx).text)-2);
        IF in_replace_names.EXISTS(l_col_name) THEN
          l_new_name := '"'||in_replace_names(l_col_name)||'"';
          io_expression := SUBSTR(io_expression, 1, l_sql_rec.lexical_units_arr(l_indx).start_position-1)||l_new_name||SUBSTR(io_expression, l_sql_rec.lexical_units_arr(l_indx).end_position);
        END IF;   
      END IF;
      l_indx := l_sql_rec.lexical_units_arr.PRIOR(l_indx);   
    END LOOP;


    -- reparse sql with substituted names
    l_expr := get_normalized_sql(
                in_sql_rec      => l_sql_rec,
                in_quoted_names => FALSE,
                in_str_literals => FALSE,
                in_comments     => FALSE
              );
    

    l_indx := in_replace_names.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF is_simple_name(l_indx) THEN
        l_search_pos := 1;    
        l_pattern := '(^|\W)('||get_regexp_const(l_indx)||')($|\W)';
        LOOP
          l_key_start := REGEXP_INSTR(l_expr, l_pattern, l_search_pos, 1, 0, NULL, 2);
          EXIT WHEN l_key_start = 0;
          l_key_end := REGEXP_INSTR(l_expr, l_pattern, l_search_pos, 1, 1, NULL, 2); 
          l_text_rec.unit_type := 'SIMPLE NAME';
          l_text_rec.text := SUBSTR(l_expr, l_key_start, l_key_end - l_key_start);
          l_text_rec.start_position := l_key_start;  
          l_text_rec.end_position := l_key_end; 
          l_text_arr(l_text_rec.start_position) := l_text_rec;
          l_search_pos := l_key_end;
        END LOOP;
      END IF;
      l_indx := in_replace_names.NEXT(l_indx);
    END LOOP;
    
    l_int_idx := l_text_arr.LAST;
    WHILE l_int_idx IS NOT NULL LOOP
      IF l_text_arr(l_int_idx).unit_type = 'SIMPLE NAME' THEN
        l_col_name := l_text_arr(l_int_idx).text;
        IF in_replace_names.EXISTS(l_col_name) THEN
          l_new_name := in_replace_names(l_col_name);
          IF NOT is_simple_name(l_new_name) THEN
            l_new_name := '"'||l_new_name||'"';
          END IF;
          io_expression := SUBSTR(io_expression, 1, l_text_arr(l_int_idx).start_position-1)||l_new_name||SUBSTR(io_expression, l_text_arr(l_int_idx).end_position);
        END IF;   
      END IF;   
      l_int_idx := l_text_arr.PRIOR(l_int_idx); 
    END LOOP;
    
    debug('update_expression', io_expression);
  END update_expression;

  -- return original name for renamed object. If it wasn't rename return current name 
  FUNCTION get_original_name(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_indx   VARCHAR2(50); 
  BEGIN
    l_indx := in_object_type||':"'||in_object_name||'"';
    IF g_replaced_names_indx.EXISTS(l_indx) THEN
      RETURN g_replaced_names_indx(l_indx);
    ELSE
      RETURN in_object_name;
    END IF;  
  END get_original_name;

  -- parses explain create statement and return object type, owner and name
  PROCEDURE parse_create_statement(
    in_sql           IN CLOB,
    out_object_type  OUT VARCHAR2,
    out_object_owner OUT VARCHAR2,
    out_object_name  OUT VARCHAR2
  )
  AS
    l_sql                       CLOB;
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(100);
    l_name                      VARCHAR2(100);
    
    FUNCTION read_name
    RETURN VARCHAR2
    AS
      l_ident                  VARCHAR2(100);
    BEGIN
      l_regexp := '\S';
      l_key_start_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos);
      IF l_key_start_pos > 0 THEN
        l_key := SUBSTR(l_sql, l_key_start_pos, 1);
        IF l_key = '"' THEN
          l_regexp := '"';
          l_key_start_pos := l_key_start_pos + 1;
        ELSIF l_key BETWEEN 'A' AND 'Z' THEN
          l_regexp := '[^A-Z0-9_\#\$]';
        ELSE
          RETURN NULL;  
        END IF;
        l_search_pos := l_key_start_pos;
        l_key_end_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos);
        l_ident := SUBSTR(l_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
        IF l_key = '"' THEN
          l_key_end_pos := l_key_end_pos + 1; 
        END IF;   
      END IF;
      RETURN l_ident;
    END read_name;
     
  BEGIN
    l_sql := get_normalized_sql(
               in_sql_rec      => parse_sql(in_sql), 
               in_quoted_names => TRUE,
               in_str_literals => FALSE,
               in_comments     => FALSE
             ); 
    
    -- add trailing space to simplify parsing names.
    l_sql := l_sql ||' ';
    
    l_search_pos := 1;
    l_search_pos := skip_whitespace(l_sql, l_search_pos);
    l_regexp := 'CREATE\s+(GLOBAL\s+TEMPORARY\s+)?(TABLE)|\s*CREATE\s+(UNIQUE\s+|BITMAP\s+)?(INDEX)|\s*CREATE\s+(OR\s+REPLACE\s+)?(TYPE)|\s*CREATE\s+(SEQUENCE)\W';
    l_key_start_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos, 1, 0); -- find table definition
    IF l_key_start_pos = 1 THEN
      -- check is it table
      out_object_type := REGEXP_SUBSTR(l_sql, l_regexp, l_search_pos, 1, NULL, 2);
      IF out_object_type IS NULL THEN
        -- check is it index 
        out_object_type := REGEXP_SUBSTR(l_sql, l_regexp, l_search_pos, 1, NULL, 4);
      END IF;   
      IF out_object_type IS NULL THEN
        -- check is it type 
        out_object_type := REGEXP_SUBSTR(l_sql, l_regexp, l_search_pos, 1, NULL, 6);
      END IF;   
      IF out_object_type IS NULL THEN
        -- check is it sequence 
        out_object_type := REGEXP_SUBSTR(l_sql, l_regexp, l_search_pos, 1, NULL, 7);
      END IF;   
      l_search_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos, 1, 1, NULL, 0);

      l_name := read_name;
      
      IF l_name IS NOT NULL THEN
        l_search_pos := l_key_end_pos;
        l_regexp := '\S';
        l_key_start_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos);
        IF l_key_start_pos > 0 AND 
           SUBSTR(l_sql, l_key_start_pos, 1) = '.'
        THEN
          out_object_owner := l_name;
          l_search_pos := l_key_start_pos + 1;
          out_object_name := read_name;       
        ELSE
          out_object_name := l_name;      
        END IF; 
      END IF;
        
    END IF;
    
  END parse_create_statement;

  -- Parse cort-index expression and checks that it valid create index statement on given table
  PROCEDURE parse_index_sql(
    io_sql_rec        IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_parse_only                CLOB;
    l_indx                      PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_full_name                 VARCHAR2(32767);
    l_owner                     VARCHAR2(100); 
    l_name                      VARCHAR2(100); 
    l_index_rec                 cort_exec_pkg.gt_index_rec;
    l_ddl                       CLOB;
    l_params_rec                cort_params_pkg.gt_params_rec;
  BEGIN
    l_parse_only := get_normalized_sql(
                      in_sql_rec      => io_sql_rec, 
                      in_quoted_names => FALSE,
                      in_str_literals => FALSE,
                      in_comments     => FALSE
                    );
    l_key_start_pos := 1;
    l_key_start_pos := skip_whitespace(l_parse_only, l_key_start_pos);
    l_regexp := '(CREATE)\s+((UNIQUE\s+|BITMAP\s+)?INDEX)\W';
    l_key_start_pos := REGEXP_INSTR(l_parse_only, '\s*(CREATE)\s+((UNIQUE\s+|BITMAP\s+)?INDEX)\W', l_key_start_pos, 1, 1, 'im');
    IF l_key_start_pos > 0 THEN
      l_key_start_pos := skip_whitespace(l_parse_only, l_key_start_pos);
      l_regexp := '(ON)\W';
      l_key_end_pos := REGEXP_INSTR(l_parse_only, l_regexp, l_key_start_pos, 1, 0, 'im', 1);
      IF l_key_end_pos > 0 THEN
        l_full_name := TRIM(SUBSTR(io_sql_rec.normalized_sql, l_key_start_pos, l_key_end_pos - l_key_start_pos));
        l_key_start_pos := INSTR(SUBSTR(l_parse_only, l_key_start_pos, l_key_end_pos - l_key_start_pos),'.');
        IF l_key_start_pos > 0 THEN
          l_owner := SUBSTR(l_full_name, 1, l_key_start_pos - 1);
          l_name := SUBSTR(l_full_name, l_key_start_pos + 1); 
        ELSE
          l_owner := io_sql_rec.schema_name;
          l_name := l_full_name; 
        END IF;
        l_name  := TRIM('"' FROM l_name);
        l_owner := TRIM('"' FROM l_owner);
        debug('parse_index_sql: index_name = '||l_name||', index_owner = '||l_owner);
        
        parse_index_sql(
          io_sql_rec    => io_sql_rec,
          in_index_name => l_name,
          in_owner_name => l_owner
        ); 
      ELSE
        cort_exec_pkg.raise_error('Index definition parsing error');
      END IF;
    ELSE
      cort_exec_pkg.raise_error('Index definition parsing error');
    END IF;  
  END parse_index_sql;
  

  -- Finds index declarations in cort comments 
  PROCEDURE get_cort_indexes(
    in_sql_rec          IN gt_sql_rec,
    in_job_rec          IN cort_jobs%ROWTYPE,
    out_sql_arr         OUT NOCOPY gt_sql_arr
  )
  AS
    l_ddl_arr                   arrays.gt_clob_arr;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_text                      CLOB;
    l_value                     CLOB;
    l_indx                      PLS_INTEGER;
  BEGIN
    -- find cort-index hints

    --#[release]index= 
    -- where [release] is current release value wrapped with square brackets []. This clause is optional
    
    l_regexp := cort_params_pkg.gc_prefix||'(\['||
                get_regexp_const(cort_pkg.get_current_release)||'\])?'||
                cort_params_pkg.gc_index_regexp;

    l_indx := in_sql_rec.lexical_units_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      debug('Lexical @ pos '||l_indx, in_sql_rec.lexical_units_arr(l_indx).text);
      IF l_indx >= in_sql_rec.columns_end_pos AND
         in_sql_rec.lexical_units_arr(l_indx).unit_type IN ('LINE COMMENT', 'COMMENT') 
      THEN
        CASE in_sql_rec.lexical_units_arr(l_indx).unit_type 
        WHEN 'LINE COMMENT' THEN
          l_text := SUBSTR(in_sql_rec.lexical_units_arr(l_indx).text, 3);
        WHEN 'COMMENT' THEN
          l_text := SUBSTR(in_sql_rec.lexical_units_arr(l_indx).text, 3, LENGTH(in_sql_rec.lexical_units_arr(l_indx).text)-4);
        END CASE; 
        l_key_start_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 0, 'i');
        l_key_end_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 1, 'i');
        IF l_key_start_pos = 1 THEN
          l_value := SUBSTR(l_text, l_key_end_pos);
          out_sql_arr := gt_sql_arr();
          debug('get_cort_indexes: index declaration found', TRIM(l_value));
          IF length(TRIM(l_value)) > 0 THEN
            l_ddl_arr(l_ddl_arr.COUNT+1) := TRIM(l_value);
          END IF;  
        END IF;            
      END IF;

      l_indx := in_sql_rec.lexical_units_arr.NEXT(l_indx);
    END LOOP;
    
    IF l_ddl_arr.COUNT > 0 THEN
      out_sql_arr.EXTEND(l_ddl_arr.COUNT);
      -- parsing every index DDL
      FOR i in 1..l_ddl_arr.COUNT LOOP
        debug('index DDL', l_ddl_arr(i));
        
        out_sql_arr(i) := parse_sql(in_sql => l_ddl_arr(i));
        out_sql_arr(i).schema_name := in_job_rec.current_schema;
        parse_index_sql(
          io_sql_rec        => out_sql_arr(i)
        );
        
        IF (out_sql_arr(i).table_name <> in_job_rec.object_name) OR
           (out_sql_arr(i).table_owner <> in_job_rec.object_owner)
        THEN
          cort_exec_pkg.raise_error('Only indexes for table "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'" are allowed');
        END IF;    

      END LOOP;
    END IF;  

  END get_cort_indexes;
  
END cort_parse_pkg;
/