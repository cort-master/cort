CREATE OR REPLACE PACKAGE BODY cort_parse_pkg
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
    $IF arrays.gc_long_name_supported $THEN 
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

  FUNCTION get_release_regexp RETURN VARCHAR2
  AS
  BEGIN
    RETURN '(\['||get_regexp_const(cort_exec_pkg.g_run_params.release.get_value)||'\])'||CASE WHEN cort_exec_pkg.g_run_params.require_release.get_bool_value THEN NULL ELSE '?' END;
  END get_release_regexp;
  

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
      l_regexp := '[A-Z][A-Z0-9_$#]{0,'||to_char(arrays.gc_name_max_length-1)||'}';
      l_key_start_pos := REGEXP_INSTR(in_sql, l_regexp, l_search_pos, 1, 0);
      l_key_end_pos := REGEXP_INSTR(in_sql, l_regexp, l_search_pos, 1, 1);
      l_key := SUBSTR(in_sql, l_key_start_pos, l_key_end_pos - l_key_start_pos);
      io_search_pos := l_key_end_pos;
    ELSE
      l_key := NULL;  
    END CASE;
    RETURN l_key;
  END read_next_name;


  PROCEDURE int_replace_names(
    io_sql_rec IN OUT NOCOPY gt_sql_rec 
  )
  AS
    l_indx         PLS_INTEGER;
    l_replace_rec  gt_replace_rec;
    l_sql          CLOB;
  BEGIN
    -- loop all names start from the end
    l_sql := io_sql_rec.original_sql;
    l_indx := io_sql_rec.replaced_names_arr.LAST;
    WHILE l_indx IS NOT NULL LOOP
      l_replace_rec := io_sql_rec.replaced_names_arr(l_indx);
      l_sql := SUBSTR(l_sql, 1, l_replace_rec.start_pos - 1)||'"'||l_replace_rec.new_name||'"'||SUBSTR(l_sql, l_replace_rec.end_pos);
      l_indx := io_sql_rec.replaced_names_arr.PRIOR(l_indx);
    END LOOP;
    l_sql := SUBSTR(l_sql, 1, io_sql_rec.run_param_start_pos - 1)||' '||SUBSTR(l_sql, io_sql_rec.run_param_end_pos);
    io_sql_rec.original_sql := l_sql;
  END int_replace_names;

  -- find all entries for given name 
  PROCEDURE find_substitutable_name(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_rename_rec   IN cort_exec_pkg.gt_rename_rec,
    in_pattern      IN VARCHAR2,
    io_search_pos   IN OUT PLS_INTEGER,
    in_subexpr      IN PLS_INTEGER DEFAULT NULL
  )
  AS
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    debug('io_search_pos = '||io_search_pos);
    debug('in_pattern = '||in_pattern);
    debug('in_subexpr = '||in_subexpr);
    
    l_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, in_pattern, io_search_pos, 1, 0, NULL, in_subexpr);
    IF l_start_pos > 0 THEN 
      l_replace_rec.object_type := in_rename_rec.object_type;
      l_replace_rec.object_name := in_rename_rec.current_name;
      l_replace_rec.start_pos := l_start_pos;
      l_replace_rec.end_pos   := REGEXP_INSTR(io_sql_rec.normalized_sql, in_pattern, io_search_pos, 1, 1, NULL, in_subexpr);
      l_replace_rec.new_name  := in_rename_rec.temp_name;
      
      IF io_sql_rec.lexical_units_arr.EXISTS(l_start_pos-1) AND 
         io_sql_rec.lexical_units_arr(l_start_pos-1).unit_type = 'QUOTED NAME'
      THEN
        l_replace_rec.start_pos := l_replace_rec.start_pos - 1;
        l_replace_rec.end_pos := l_replace_rec.end_pos + 1;
      END IF;          

      io_sql_rec.replaced_names_arr(l_start_pos) := l_replace_rec;
      g_replaced_names_indx(in_rename_rec.temp_name) := in_rename_rec.current_name;
      io_search_pos := l_replace_rec.end_pos;
    ELSE
      io_search_pos := 0;
    END IF;
  END find_substitutable_name;

  -- find all entries for given table name 
  PROCEDURE find_table_name(
    io_sql_rec     IN OUT NOCOPY gt_sql_rec,
    in_rename_rec  IN cort_exec_pkg.gt_rename_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_full_name   VARCHAR2(500);
  BEGIN
    -- find table declaration
    l_search_pos := 1;
    l_full_name := get_owner_name_regexp(in_rename_rec.current_name, in_rename_rec.object_owner);  
    l_pattern := '\WTABLE\s*'||l_full_name||'(\s|\()';
    LOOP
      EXIT WHEN l_search_pos = 0;              
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 2
      );
    END LOOP;                
    -- find self references   
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WREFERENCES\s*'||l_full_name||'(\s|\()';
    LOOP
      EXIT WHEN l_search_pos = 0;              
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 2
      );
    END LOOP;                
    -- find create index statement
    l_pattern := '\WON\s*'||l_full_name||'(\s|\()';
    l_search_pos := io_sql_rec.columns_start_pos;
    LOOP
      EXIT WHEN l_search_pos = 0;              
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 2
      );
    END LOOP;
  END find_table_name;

  -- find all entries for given constraint name 
  PROCEDURE find_constraint_name(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_rename_rec IN cort_exec_pkg.gt_rename_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WCONSTRAINT\s*'||get_parse_only_sql(in_rename_rec.current_name, '\s')||'\s*((PRIMARY\s+KEY)|(UNIQUE)|(CHECK)|(FOREIGN\s+KEY))';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 1
      );
    END LOOP;                  
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WPARTITION\s+BY\s+REFERENCE\s*\(\s*'||get_parse_only_sql(in_rename_rec.current_name)||'\s*\)';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 1
      );
    END LOOP;                  
  END find_constraint_name;

  -- find all entries for given constraint name 
  PROCEDURE find_log_group_name(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_rename_rec IN cort_exec_pkg.gt_rename_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WSUPPLEMENTAL\s+LOG\s+GROUP\s+'||get_parse_only_sql(in_rename_rec.current_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 1
      );
    END LOOP;                  
  END find_log_group_name;
  
  -- find all entries for given constraint name 
  PROCEDURE find_index_name(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_rename_rec IN cort_exec_pkg.gt_rename_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WOIDINDEX\s*'||get_parse_only_sql(in_rename_rec.current_name)||'\s*\(';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 1
      );
    END LOOP;                
    l_search_pos := io_sql_rec.columns_start_pos;
    l_pattern := '\WINDEX\s*'||get_owner_name_regexp(in_rename_rec.current_name, in_rename_rec.object_owner, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 2
      );
    END LOOP;                
  END find_index_name;
  

  -- find all entries for given lob column 
  PROCEDURE find_lob_segment_name(
    io_sql_rec     IN OUT NOCOPY gt_sql_rec,
    in_rename_rec  IN cort_exec_pkg.gt_rename_rec,
    in_column_name IN VARCHAR2
  )
  AS                        
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WLOB\s*\(\s*'||get_parse_only_sql(in_column_name)||'\s*\)\s*STORE\s+AS(\s+(BASICFILE|SECUREFILE))?\s*'||get_parse_only_sql(in_rename_rec.current_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 3
      );
    END LOOP;                  
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WSTORE\s+AS\s+(BASICFILE\s+|SECUREFILE\s+)?(LOB|CLOB|BINARY\s+XML)\s*'||get_parse_only_sql(in_rename_rec.current_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 3
      );
    END LOOP;                  
  END find_lob_segment_name;
  
  -- find all entries for given nested table 
  PROCEDURE find_nested_table_name(
    io_sql_rec     IN OUT NOCOPY gt_sql_rec,
    in_rename_rec  IN cort_exec_pkg.gt_rename_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    l_search_pos := io_sql_rec.columns_end_pos;
    l_pattern := '\WSTORE\s+AS\s+'||get_parse_only_sql(in_rename_rec.current_name, '(\W|$)');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      find_substitutable_name(
        io_sql_rec      => io_sql_rec,
        in_rename_rec   => in_rename_rec,
        in_pattern      => l_pattern,
        io_search_pos   => l_search_pos,
        in_subexpr      => 1
      );
    END LOOP;  
  END find_nested_table_name;               

  -- find all substitution entries (CREATE TABLE)
  PROCEDURE find_table_substitutions(
    io_sql_rec   IN OUT NOCOPY gt_sql_rec,
    in_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_indx      PLS_INTEGER; 
  BEGIN
    -- find all instances for table name
    find_table_name(
      io_sql_rec    => io_sql_rec,
      in_rename_rec => in_table_rec.rename_rec
    );
    -- find all named constraints
    FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
      IF in_table_rec.constraint_arr(i).generated = 'USER NAME' THEN
        find_constraint_name(
          io_sql_rec    => io_sql_rec,
          in_rename_rec => in_table_rec.constraint_arr(i).rename_rec
        );
        IF in_table_rec.constraint_arr(i).index_name = in_table_rec.constraint_arr(i).constraint_name THEN
          IF in_table_rec.index_indx_arr.EXISTS('"'||in_table_rec.constraint_arr(i).index_owner||'"."'||in_table_rec.constraint_arr(i).index_name||'"') THEN
            l_indx := in_table_rec.index_indx_arr('"'||in_table_rec.constraint_arr(i).index_owner||'"."'||in_table_rec.constraint_arr(i).index_name||'"');
            find_index_name(
              io_sql_rec    => io_sql_rec,
              in_rename_rec => in_table_rec.index_arr(l_indx).rename_rec 
            );
          END IF;
          debug('g_replaced_names_indx('||in_table_rec.constraint_arr(i).rename_rec.temp_name||' = '||in_table_rec.constraint_arr(i).index_name);
          g_replaced_names_indx(in_table_rec.constraint_arr(i).rename_rec.temp_name) := in_table_rec.constraint_arr(i).index_name;
        END IF;
      END IF;  
    END LOOP;
    -- find all named log groups
    FOR i IN 1..in_table_rec.log_group_arr.COUNT LOOP
      IF in_table_rec.log_group_arr(i).generated = 'USER NAME' THEN
        find_log_group_name(
          io_sql_rec    => io_sql_rec,
          in_rename_rec => in_table_rec.log_group_arr(i).rename_rec
        );
      END IF;  
    END LOOP;
    -- find all indexes 
    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      IF in_table_rec.index_arr(i).rename_rec.generated = 'N' THEN
        find_index_name(
          io_sql_rec    => io_sql_rec,
          in_rename_rec => in_table_rec.index_arr(i).rename_rec
        );
      END IF;  
    END LOOP;
    -- find all named lob segments
    l_indx := in_table_rec.lob_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      -- for lob columns
      IF in_table_rec.lob_arr(l_indx).rename_rec.generated = 'N' THEN
        find_lob_segment_name(
          io_sql_rec     => io_sql_rec,
          in_rename_rec  => in_table_rec.lob_arr(l_indx).rename_rec,
          in_column_name => in_table_rec.lob_arr(l_indx).column_name 
        );
      END IF;  
      l_indx := in_table_rec.lob_arr.NEXT(l_indx);
    END LOOP;
    -- for all nested tables
    FOR i IN 1..in_table_rec.nested_tables_arr.COUNT LOOP
      find_nested_table_name(
        io_sql_rec     => io_sql_rec,
        in_rename_rec => in_table_rec.nested_tables_arr(i).rename_rec
      );
    END LOOP;
    
  END find_table_substitutions;
  
  -- find all substitution entries (CREATE INDEX)
  PROCEDURE find_index_substitutions(
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
    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_index_rec.rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );

    -- find table name
    l_pattern := '\WON\s+' || get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\W');
    l_search_pos := io_sql_rec.definition_start_pos;
    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_table_rec.rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );
    -- FOR BITMAP JOIN INDEXES                 
    -- find table name
    l_pattern := get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\.');
    -- replace in columns
    l_search_pos := io_sql_rec.columns_start_pos;
    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_table_rec.rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );
    -- replace in FROM                
    l_pattern := get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\W');
    l_search_pos := io_sql_rec.columns_end_pos;
    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_table_rec.rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );
    -- replace in WHERE                
    l_pattern := get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\.');
    l_search_pos := io_sql_rec.columns_end_pos;
    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_table_rec.rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );

  END find_index_substitutions;  
  
  PROCEDURE find_seq_substitutions(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_rename_rec   IN cort_exec_pkg.gt_rename_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    -- find all instances for table name
    l_search_pos := 1;    
    l_pattern := '\WSEQUENCE\s+'||get_owner_name_regexp(in_rename_rec.current_name, in_rename_rec.object_owner, '(\W|$)');
    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );
  END find_seq_substitutions;

  PROCEDURE find_type_substitutions(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    in_rename_rec   IN cort_exec_pkg.gt_rename_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_rename_rec  cort_exec_pkg.gt_rename_rec;
  BEGIN
    -- find all instances for table name
    l_search_pos := 1;    
    IF in_rename_rec.object_type = 'TYPE' THEN
      l_pattern := '\WTYPE\s+'||get_owner_name_regexp(in_rename_rec.current_name, in_rename_rec.object_owner, '\W');
    ELSIF in_rename_rec.object_type = 'TYPE BODY' THEN
      l_pattern := '\WTYPE\s+BODY\s+'||get_owner_name_regexp(in_rename_rec.current_name, in_rename_rec.object_owner, '\W');
    END IF;  

    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );

    l_pattern := '\WUNDER\s+'||get_owner_name_regexp(in_rename_rec.current_name, in_rename_rec.object_owner, '\W');
    l_rename_rec := in_rename_rec;
    l_rename_rec.object_type := 'TYPE';

    find_substitutable_name(
      io_sql_rec      => io_sql_rec,
      in_rename_rec   => in_rename_rec,
      in_pattern      => l_pattern,
      io_search_pos   => l_search_pos,
      in_subexpr      => 2
    );

    l_pattern := '\WCONSTRUCTOR\s+FUNCTION\s+'||get_parse_only_sql(in_rename_rec.current_name, '\W');
    l_rename_rec := in_rename_rec;
    l_rename_rec.object_type := 'METHOD';
debug('replace type constructor name '||l_pattern);
    l_search_pos := 1;
    LOOP
      find_substitutable_name(
        io_sql_rec    => io_sql_rec,
        in_rename_rec => l_rename_rec,
        in_pattern    => l_pattern,
        io_search_pos => l_search_pos,
        in_subexpr    => 1
      );
      EXIT WHEN l_search_pos = 0;               
    END LOOP;                  
       
  END find_type_substitutions;

  PROCEDURE find_type_synonyms(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_pattern          VARCHAR2(1000);
    l_rename_rec       cort_exec_pkg.gt_rename_rec;
    l_search_pos       PLS_INTEGER;
    l_type_owner_arr   arrays.gt_name_arr;
    l_type_name_arr    arrays.gt_name_arr;  
    l_type_synonym_arr arrays.gt_name_arr;
  BEGIN
    IF NOT cort_options_pkg.gc_rename_types THEN
      RETURN;
    END IF;
    
    SELECT synonym_owner, synonym_name, MAX(type_name) KEEP(DENSE_RANK FIRST ORDER BY job_id DESC) AS type_name
      BULK COLLECT
      INTO l_type_owner_arr, l_type_synonym_arr, l_type_name_arr   
      FROM cort_type_synonyms
     GROUP BY synonym_owner, synonym_name;
      
      
    -- start search with full name match 
    FOR i IN 1..l_type_synonym_arr.COUNT LOOP
      l_rename_rec.current_name := l_type_synonym_arr(i);
      l_rename_rec.object_name := l_type_synonym_arr(i);
      l_rename_rec.object_owner := l_type_owner_arr(i);
      l_rename_rec.object_type := 'TYPE'; 
      l_rename_rec.temp_name := l_type_name_arr(i);
      
      l_pattern := get_parse_only_sql(l_rename_rec.object_owner)||'\s*\.\s*'||get_parse_only_sql(l_rename_rec.current_name, '\W');
      l_search_pos := 1;
      debug('find_type_synonyms.l_pattern = '||l_pattern);
      LOOP
        EXIT WHEN l_search_pos = 0;               
        find_substitutable_name(
          io_sql_rec      => io_sql_rec,
          in_rename_rec   => l_rename_rec,
          in_pattern      => l_pattern,
          io_search_pos   => l_search_pos,
          in_subexpr      => 2
        );
      END LOOP;            
    END LOOP;

    -- continue search with short name match 
    FOR i IN 1..l_type_synonym_arr.COUNT LOOP
      IF l_type_owner_arr(i) = io_sql_rec.current_schema THEN 
        l_rename_rec.current_name := l_type_synonym_arr(i);
        l_rename_rec.object_name := l_type_synonym_arr(i);
        l_rename_rec.object_owner := l_type_owner_arr(i);
        l_rename_rec.object_type := 'TYPE'; 
        l_rename_rec.temp_name := l_type_name_arr(i);

        l_pattern := '([^\.]\s*)'||get_parse_only_sql(l_rename_rec.current_name, '\W');
        l_search_pos := 1;
        debug('find_type_synonyms.l_pattern = '||l_pattern);
        LOOP
          EXIT WHEN l_search_pos = 0;               
          find_substitutable_name(
            io_sql_rec      => io_sql_rec,
            in_rename_rec   => l_rename_rec,
            in_pattern      => l_pattern,
            io_search_pos   => l_search_pos,
            in_subexpr      => 2
          );
        END LOOP;
      END IF;            
    END LOOP;
  END find_type_synonyms;
  
  -- Public declararions

  /* Replaces all comments and string literals with blank symbols */
  PROCEDURE parse_sql(
    in_sql      IN   CLOB,
    out_sql_rec OUT NOCOPY gt_sql_rec
  )
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
  BEGIN
    out_sql_rec.original_sql := in_sql;
    out_sql_rec.normalized_sql := null;

    l_search_pos := 1;
    l_expr_cnt := 0;

    l_expr_start_pattern := q'{(/\*)|(--)|((N|n)?')|((N|n)?(Q|q)'\S)|"}';
    LOOP
      l_key_start_pos := REGEXP_INSTR(out_sql_rec.original_sql, l_expr_start_pattern, l_search_pos, 1, 0);
      l_key_end_pos   := REGEXP_INSTR(out_sql_rec.original_sql, l_expr_start_pattern, l_search_pos, 1, 1);
      EXIT WHEN l_key_start_pos = 0 OR l_key_start_pos IS NULL
             OR l_key_end_pos = 0 OR l_key_end_pos IS NULL
             OR l_expr_cnt > 10000;
      
      l_expr := UPPER(SUBSTR(out_sql_rec.original_sql, l_search_pos, l_key_start_pos-l_search_pos));
      
      l_lexical_unit_rec.unit_type := 'SQL';
      l_lexical_unit_rec.text := l_expr;
      l_lexical_unit_rec.start_position := l_search_pos;
      l_lexical_unit_rec.end_position := l_key_start_pos;
      out_sql_rec.lexical_units_arr(l_search_pos) := l_lexical_unit_rec;

      out_sql_rec.normalized_sql := out_sql_rec.normalized_sql || l_expr;

      l_key := SUBSTR(out_sql_rec.original_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
      CASE
      WHEN l_key = '/*' THEN
        l_expr_end_pattern := '\*/';
        l_expr_start_pos := REGEXP_INSTR(out_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 2;
        l_expr_type := 'COMMENT';
      WHEN l_key = '--' THEN
        l_expr_end_pattern := '$';
        l_expr_start_pos := REGEXP_INSTR(out_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0, 'm');
        l_expr_end_pos := l_expr_start_pos;
        l_expr_type := 'LINE COMMENT';
      WHEN l_key = '"' THEN
        l_expr_end_pattern := '"';
        l_expr_start_pos := REGEXP_INSTR(out_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 1;
        l_expr_type := 'QUOTED NAME';
      WHEN l_key = 'N'''
        OR l_key = 'n'''
        OR l_key = '''' THEN
        l_expr_end_pattern := '''';
        l_expr_start_pos := REGEXP_INSTR(out_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
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
        l_expr_start_pos := INSTR(out_sql_rec.original_sql, l_expr_end_pattern, l_key_end_pos);
        l_expr_end_pos := l_expr_start_pos + 2;
        l_expr_type := 'LITERAL';
      END CASE;
      IF l_expr_start_pos = 0 THEN
        cort_exec_pkg.raise_error( 'Invalid SQL');
      END IF;
      l_expr := SUBSTR(out_sql_rec.original_sql, l_key_start_pos, l_expr_end_pos - l_key_start_pos);
      
      l_lexical_unit_rec.unit_type := l_expr_type;
      l_lexical_unit_rec.text := l_expr;
      l_lexical_unit_rec.start_position := l_key_start_pos;
      l_lexical_unit_rec.end_position := l_expr_end_pos;
      out_sql_rec.lexical_units_arr(l_key_start_pos) := l_lexical_unit_rec;
          
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

      out_sql_rec.normalized_sql := out_sql_rec.normalized_sql || l_expr;

      l_search_pos := l_expr_end_pos;
      l_expr_cnt := l_expr_cnt + 1;
    END LOOP;

    l_expr := UPPER(SUBSTR(out_sql_rec.original_sql, l_search_pos));
      
    l_lexical_unit_rec.unit_type := 'SQL';
    l_lexical_unit_rec.text := l_expr;
    l_lexical_unit_rec.start_position := l_search_pos;
    l_lexical_unit_rec.end_position := LENGTH(out_sql_rec.original_sql);
    out_sql_rec.lexical_units_arr(l_search_pos) := l_lexical_unit_rec;
    
    out_sql_rec.normalized_sql := out_sql_rec.normalized_sql || l_expr;
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

  -- parse cort hints for run_params
  PROCEDURE parse_run_params(
    in_sql          IN CLOB,
    io_params_rec   IN OUT NOCOPY cort_params_pkg.gt_run_params_rec
  )
  AS
    l_hints                     VARCHAR2(32767);
    l_regexp                    VARCHAR2(1000);
    l_search_pos                PLS_INTEGER;
    l_name_regexp               VARCHAR2(500);
    l_value_regexp              VARCHAR2(500);
    l_param_names_arr           arrays.gt_str_arr;
    l_name                      VARCHAR2(30);
    l_value                     VARCHAR2(32767);
  BEGIN
    l_regexp := '(^\s*CREATE\s*\/\*'||cort_params_pkg.gc_prefix||'\s*OR\s+REPLACE\W)';
    IF REGEXP_INSTR(in_sql, l_regexp, 1, 1, 0, 'imn') = 1 THEN 
      l_search_pos := REGEXP_INSTR(in_sql, l_regexp, 1, 1, 1, 'imn');
      l_hints := REGEXP_SUBSTR(in_sql, '(.+)\*\/', l_search_pos, 1, null, 1);
    ELSE
      l_regexp := '(^\s*CREATE\s*--' ||cort_params_pkg.gc_prefix||'[ \t]*OR[ \t]+REPLACE\W)';
      l_search_pos := REGEXP_INSTR(in_sql, l_regexp, 1, 1, 0, 'imn');
      IF l_search_pos = 1 THEN 
        l_search_pos := REGEXP_INSTR(in_sql, l_regexp, 1, 1, 1, 'imn');
        l_hints := REGEXP_SUBSTR(in_sql, '(.+)$', l_search_pos, 1, 'im', 1);
      END IF;  
    END IF;  
    IF l_hints IS NOT NULL THEN
      l_param_names_arr.DELETE;
      l_param_names_arr := cort_params_pkg.get_run_param_names;
      
      FOR i IN 1..l_param_names_arr.COUNT LOOP
        l_name_regexp := l_name_regexp||l_param_names_arr(i)||'|';        
      END LOOP; 
      l_name_regexp := TRIM('|' FROM l_name_regexp);
      l_name_regexp := '('||l_name_regexp||')(=)';

      l_search_pos := 1;
      WHILE l_search_pos > 0 AND l_search_pos < LENGTH(l_hints) LOOP    
        l_name := UPPER(REGEXP_SUBSTR(l_hints, l_name_regexp, l_search_pos, 1, 'im', 1));
        IF l_name IS NOT NULL THEN 
          l_search_pos := REGEXP_INSTR(l_hints, l_name_regexp, l_search_pos, 1, 1, 'im');
          l_value_regexp := cort_params_pkg.get_run_param(io_params_rec, l_name).get_regexp;
          l_value := REGEXP_SUBSTR(l_hints, l_value_regexp, l_search_pos, 1, 'im');
          IF l_value IS NOT NULL THEN  
            cort_params_pkg.set_run_param_value(io_params_rec, l_name, l_value);
            l_search_pos := REGEXP_INSTR(l_hints, l_value_regexp, l_search_pos, 1, 1, 'im');
          ELSE 
            l_search_pos := l_search_pos + length(l_name);
          END IF;  
        ELSE  
          EXIT;  
        END IF;  
      END LOOP;  
    END IF;
  END parse_run_params;

  -- parse cort hints
  PROCEDURE parse_params(
    io_sql_rec      IN OUT NOCOPY gt_sql_rec,
    io_params_rec   IN OUT NOCOPY cort_params_pkg.gt_params_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_hint_regexp               VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_text                      CLOB;
    l_value                     VARCHAR2(32767);
    l_indx                      PLS_INTEGER;
    l_hints                     arrays.gt_clob_arr;
    l_name_regexp               VARCHAR2(500);
    l_value_regexp              VARCHAR2(500);
    l_param_names_arr           arrays.gt_str_arr;
    l_name                      VARCHAR2(30);
  BEGIN
    --#[release] param=value   -
    --           ^              |
    --           | ------------  
    -- where [release] is release value wrapped with square brackets []. This clause might be optional depending on require_release run param value
    -- One hint comment can containt many params as long their regexp are not ended with .*. These params take everyting after = sign and up to end of comment as a value. So it is recommended to place them into individual comments  
    l_hint_regexp := cort_params_pkg.gc_prefix||get_release_regexp;
    

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
        -- all cort hints must be placed anywhere after columns definition  
        IF io_sql_rec.lexical_units_arr(l_indx).start_position >= NVL(io_sql_rec.columns_end_pos,io_sql_rec.run_param_end_pos)                                                        
        THEN                                            
          l_key_start_pos := REGEXP_INSTR(l_text, l_hint_regexp, 1, 1, 0, 'i');
          l_key_end_pos := REGEXP_INSTR(l_text, l_hint_regexp, 1, 1, 1, 'i');
          IF l_key_start_pos = 1 AND l_key_end_pos > 0 THEN
            cort_log_pkg.debug('l_hints('||to_char(l_hints.COUNT+1)||') = '||SUBSTR(l_text, l_key_end_pos));
            l_hints(l_hints.COUNT+1) := SUBSTR(l_text, l_key_end_pos);
          END IF;  
        END IF;
      END IF;  
      l_indx := io_sql_rec.lexical_units_arr.NEXT(l_indx); 
    END LOOP;

    IF l_hints.COUNT = 0 THEN 
      RETURN;
    END IF;   
    
    l_param_names_arr := cort_params_pkg.get_param_names;
    
    FOR i IN 1..l_param_names_arr.COUNT LOOP
      l_name_regexp := l_name_regexp||l_param_names_arr(i)||'|';        
    END LOOP; 
    l_name_regexp := TRIM('|' FROM l_name_regexp);
    l_name_regexp := '('||l_name_regexp||')(=)';

    FOR i IN 1..l_hints.COUNT LOOP
      l_search_pos := 1;
      debug('Parsing param string '||l_hints(i));
      WHILE l_search_pos > 0 AND l_search_pos < LENGTH(l_hints(i)) LOOP    
        l_name := UPPER(REGEXP_SUBSTR(l_hints(i), l_name_regexp, l_search_pos, 1, 'im', 1));
        IF l_name IS NOT NULL THEN 
          cort_log_pkg.debug('Found param '||l_name);
          l_search_pos := REGEXP_INSTR(l_hints(i), l_name_regexp, l_search_pos, 1, 1, 'im');
          l_value_regexp := cort_params_pkg.get_param(io_params_rec, l_name).get_regexp;
          l_value := REGEXP_SUBSTR(l_hints(i), l_value_regexp, l_search_pos, 1, 'im');
          IF l_value IS NULL THEN  
            cort_log_pkg.debug('wrong param '||l_name||' value. Expected values = '||l_value_regexp);
            l_search_pos := l_search_pos + length(l_name);
          ELSE 
            cort_log_pkg.debug('param '||l_name||' set to '||l_value);
            cort_params_pkg.set_param_value(io_params_rec, l_name, l_value);
            l_search_pos := REGEXP_INSTR(l_hints(i), l_value_regexp, l_search_pos, 1, 1, 'im');
          END IF;  
        ELSE  
          EXIT;  
        END IF;  
      END LOOP;  

    END LOOP;
    
  END parse_params;

  -- Initial preparse of CREATE statement for TABLE/INDEX/SEQUENCE/TYPE. Finds run params positions; 
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
    l_regexp                    VARCHAR(1000);
    l_name_regexp               VARCHAR(1000);
  BEGIN
    l_search_pos := 1;
    l_search_pos := skip_whitespace(io_sql_rec.normalized_sql, l_search_pos);
    
    io_sql_rec.object_type := in_object_type;
    io_sql_rec.object_name := in_object_name;
    io_sql_rec.object_owner := in_object_owner;

    CASE in_object_type 
    WHEN 'TABLE' THEN
      l_regexp := '(CREATE)\s+((GLOBAL\s+TEMPORARY\s+)?TABLE)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner); 
    WHEN 'INDEX' THEN
      l_regexp := '(CREATE)\s+((UNIQUE\s+|BITMAP\s+)?INDEX)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner); 
    WHEN 'SEQUENCE' THEN
      l_regexp := '(CREATE)\s+(SEQUENCE)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner); 
    WHEN 'TYPE' THEN
      l_regexp := '(CREATE)\s+((OR\s+REPLACE\s+)?TYPE)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner); 
    WHEN 'TYPE BODY' THEN
      l_regexp := '(CREATE)\s+((OR\s+REPLACE\s+)?TYPE\s+BODY)\W';
      l_name_regexp := get_owner_name_regexp(in_object_name, in_object_owner); 
    END CASE; 
    
    l_key_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 0); -- find object definition
    l_key_end_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 2); -- find end of object definition
    IF l_key_start_pos = l_search_pos THEN
      io_sql_rec.run_param_start_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 1); -- end of keyword CREATE
      io_sql_rec.run_param_end_pos := REGEXP_INSTR(io_sql_rec.normalized_sql, l_regexp, l_search_pos, 1, 0, NULL, 2); -- start of keyword following "CREATE"
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
    ELSE
      cort_exec_pkg.raise_error( 'Object name not found');
    END IF; 
    
    debug('normalized_sql',io_sql_rec.normalized_sql);
  END initial_object_parse;
  
  
  -- Finds data_values params and assigns them to the nearest column
  PROCEDURE parse_column_data_values(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(20);
    l_text                      VARCHAR2(32767);
    l_value                     VARCHAR2(32767);
    l_indx                      PLS_INTEGER;
    l_last_column_index         PLS_INTEGER;
    l_column_index              PLS_INTEGER;

  BEGIN
    --#[release]= 
    -- or
    --#[release]=
    -- where [release] is current release value wrapped with square brackets []. This clause might be optional depending on require_release run param value
    
    l_regexp := cort_params_pkg.gc_prefix||get_release_regexp||'('||cort_params_pkg.gc_value_prefix||')';

  
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
            IF l_key = cort_params_pkg.gc_value_prefix THEN
              l_column_index := io_sql_rec.columns_arr.PRIOR(l_indx);
              IF l_column_index IS NOT NULL THEN
                debug('Parsing: Column at position '||l_indx||' - '||io_sql_rec.columns_arr(l_column_index).column_name||'. Inline data value = '||l_value);
                cort_params_pkg.set_param_value(cort_exec_pkg.g_params, 'DATA_VALUES', io_sql_rec.columns_arr(l_column_index).column_name||'='||l_value);
              ELSE
                debug('Parsing: Column at position '||l_indx||' not found');
              END IF;
            ELSE 
              debug('Parsing: Unknown cort key = '||l_key);
            END IF;
          END IF;
        END IF;            
      END IF;
      EXIT WHEN l_indx > io_sql_rec.columns_end_pos;
      l_indx := io_sql_rec.lexical_units_arr.NEXT(l_indx);
    END LOOP;
  END parse_column_data_values;
  
  -- Parse relational columns
  PROCEDURE parse_relational_columns_sql(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
    l_name                      VARCHAR2(200);
    l_keyword_arr               arrays.gt_str_arr;
    l_keyword_indx              arrays.gt_str_indx;
    l_column_rec                gt_sql_column_rec;
    l_exclusion_list            VARCHAR2(500);
    l_type_expr_arr             arrays.gt_lstr_indx;
    l_type_indx                 VARCHAR2(50);
  BEGIN
    l_search_pos := io_sql_rec.columns_start_pos;
    
    l_parse_only_sql := get_normalized_sql(
                          in_sql_rec      => io_sql_rec, 
                          in_quoted_names => TRUE,
                          in_str_literals => FALSE,
                          in_comments     => FALSE
                       );

    SELECT keyword
      BULK COLLECT 
      INTO l_keyword_arr
      FROM v$reserved_words
     WHERE REGEXP_LIKE(keyword, '[A-Z]+') 
       AND (reserved = 'Y' or keyword in ('ACCESS','ADD','AUDIT','COLUMN','COMMENT','CURRENT','FILE','IMMEDIATE','INCREMENT','INITIAL','LEVEL','MAXEXTENTS','MLSLABEL','MODIFY','NOAUDIT','OFFLINE','ONLINE','PRIVILEGES','REF','ROW','ROWS','ROWID','ROWNUM','SESSION','SUCCESSFUL','SYSDATE','UID','USER','VALIDATE','WHENEVER'))
     ORDER BY 1;
    
    FOR i IN 1..l_keyword_arr.COUNT LOOP
      l_keyword_indx(l_keyword_arr(i)) := i;
    END LOOP;

    debug('l_parse_only_sql', l_parse_only_sql);
    l_exclusion_list := '\W(((CONSTRAINT\s+'||cort_params_pkg.gc_name_regexp||'\s+)?(UNIQUE|PRIMARY\s+KEY|FOREIGN\s+KEY|CHECK))|(SCOPE\s+FOR|SUPPLEMENTAL\s+LOG|PERIOD\s+FOR))\W';
    l_regexp := '\W('||cort_params_pkg.gc_name_regexp||')\s+';
    
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
    l_type_expr_arr('BLOB') := '(BLOB)';
    l_type_expr_arr('CLOB') := '(CLOB))';
    l_type_expr_arr('NCLOB') := '(NCLOB)';
    l_type_expr_arr('BFILE') := '(BFILE)';
    l_type_expr_arr('LONG') := '(LONG)';
    l_type_expr_arr('LONG RAW') := '(LONG\s+RAW)';
    l_type_expr_arr('ROWID') := '(ROWID)';
    l_type_expr_arr('UROWID') := '(UROWID(\s*\(\s*[0-9]+\s*\))?)';    
    l_type_expr_arr('XMLTYPE') := '((SYS.)?XMLTYPE))';    
    l_type_expr_arr('ANYTYPE') := '((SYS.)?ANYTYPE))';    
    l_type_expr_arr('ANYDATA') := '((SYS.)?ANYDATA))';    
    l_type_expr_arr('ANYDATASET') := '((SYS.)?ANYDATASET))';    
    
    io_sql_rec.columns_arr.DELETE;
 
    WHILE l_search_pos > 0 AND l_search_pos < io_sql_rec.columns_end_pos LOOP
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, null, 1);
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, null, 1);
      IF l_key_start_pos > 0 AND l_key_end_pos < io_sql_rec.columns_end_pos THEN
        l_name := SUBSTR(l_parse_only_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
       -- debug('l_name = ['||l_name||'] l_key_start_pos = '||l_key_start_pos||', exclusion start = '||REGEXP_INSTR(l_parse_only_sql, l_exclusion_list, l_search_pos, 1, 0 , null, 0));        
        IF NOT l_keyword_indx.EXISTS(l_name) AND l_key_start_pos <> REGEXP_INSTR(l_parse_only_sql, l_exclusion_list, l_search_pos, 1, 0 , null, 1) THEN
          debug('Parsing: column ['||l_name||'] positon is '||l_key_start_pos);
          l_column_rec := NULL;
          l_column_rec.column_name := l_name;
          l_column_rec.start_position := l_key_start_pos;
          IF NOT io_sql_rec.as_select_flag THEN  
            l_search_pos := skip_whitespace(l_parse_only_sql, l_key_end_pos);
            l_type_indx := l_type_expr_arr.FIRST;
            
            -- Check if column is built-in or ANSI data type
            WHILE l_type_indx IS NOT NULL LOOP
              l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_type_expr_arr(l_type_indx), l_search_pos);
              IF l_key_start_pos = l_search_pos THEN 
                l_column_rec.data_type := l_type_indx; 
                EXIT;
              END IF;    
              l_type_indx := l_type_expr_arr.NEXT(l_type_indx);
            END LOOP;
            --- if data type is no found
            IF l_column_rec.data_type IS NULL THEN 
              l_regexp := '(VISIBLE\s+|INVISIBLE\s+)?(GENERATED\s+ALWAYS\s+)?AS)\W';
              IF REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos) = l_search_pos THEN
                l_column_rec.virtual_flag := TRUE;
              END IF; 
            END IF;
          END IF;  

          io_sql_rec.columns_arr(l_column_rec.start_position) := l_column_rec;
        END IF;
        l_search_pos := l_key_end_pos;
        l_regexp := ',\s*('||cort_params_pkg.gc_name_regexp||')\s+';
      ELSE
        EXIT;  
      END IF;
    END LOOP;
    
    parse_column_data_values(io_sql_rec);
  END parse_relational_columns_sql;


  PROCEDURE parse_as_select(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_start_pos                 PLS_INTEGER;
    l_end_pos                   PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
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
      IF REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1) > 0 THEN
        debug('AS SELECT clause found');
        l_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
        l_end_pos := LENGTH(TRIM(io_sql_rec.normalized_sql)); -- remove all trailing comments
        io_sql_rec.as_select_flag := TRUE;
        io_sql_rec.as_select_sql := SUBSTR(io_sql_rec.original_sql, l_start_pos, l_end_pos - l_start_pos);
        io_sql_rec.as_select_fromitself := REGEXP_INSTR(io_sql_rec.as_select_sql, get_owner_name_regexp(io_sql_rec.object_name, io_sql_rec.object_owner, '\W')) > 0;
        io_sql_rec.original_sql := 
        SUBSTR(io_sql_rec.original_sql, 1, l_start_pos-1)||CHR(10)||
        'SELECT *'||CHR(10)|| 
        '  FROM ('||CHR(10)||io_sql_rec.as_select_sql||CHR(10)||
        '       )'||CHR(10)||
        ' WHERE 1=0'||CHR(10)||SUBSTR(io_sql_rec.original_sql, l_end_pos);
      ELSE
        debug('AS SELECT clause not found');
      END IF;
    END IF;      
  END parse_as_select;


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
    l_sql                       CLOB;
    l_key_columns               VARCHAR2(32767);
    l_cnt                       BINARY_INTEGER;
    l_part_key_column_arr       dbms_utility.uncl_array;
    
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
        io_sql_rec.subpartitioning_type := SUBSTR(l_parse_only_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
        l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, '\)', l_key_start_pos, 1, 1);
        l_key_columns := SUBSTR(io_sql_rec.normalized_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos-1);
        debug('subpartition key columns = '||l_key_columns);
        l_part_key_column_arr.DELETE;
        dbms_utility.comma_to_table(
          list   => l_key_columns, 
          tablen => l_cnt,
          tab    => l_part_key_column_arr
        );
        FOR i IN 1..l_cnt LOOP
          io_sql_rec.subpart_key_column_arr(i) := l_part_key_column_arr(i);
        END LOOP;
      END IF;

      debug('subpartitioning_type = '||io_sql_rec.subpartitioning_type);
    

      -- if subpartitioned
      IF io_sql_rec.subpartitioning_type IS NOT NULL THEN
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
          ELSIF io_sql_rec.subpartitioning_type = 'HASH' THEN
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
      io_sql_rec.partitioning_type := SUBSTR(l_parse_only_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
      l_search_pos := l_key_end_pos; 
    END IF;

    debug('partitioning_type = '||io_sql_rec.partitioning_type);
    
    
    IF io_sql_rec.partitioning_type IS NOT NULL THEN
      -- skip column/reference definition 
      IF io_sql_rec.partitioning_type <> 'SYSTEM' THEN
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, '\(', l_search_pos, 1, 0);
        l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, '\)', l_key_start_pos, 1, 1);
        l_search_pos := l_key_end_pos; 

        l_key_columns := SUBSTR(io_sql_rec.normalized_sql, l_key_start_pos+1, l_key_end_pos-l_key_start_pos-2);
        debug('partition key columns = '||l_key_columns);
        dbms_utility.comma_to_table(
          list   => l_key_columns, 
          tablen => l_cnt,
          tab    => l_part_key_column_arr
        );
        FOR i IN 1..l_cnt LOOP
          io_sql_rec.part_key_column_arr(i) := l_part_key_column_arr(i);
        END LOOP;
          
      END IF;
      
      CASE io_sql_rec.partitioning_type
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

        debug('Parsing: io_sql_rec.partitions_start_pos = '||io_sql_rec.partitions_start_pos); 
        debug('Parsing: io_sql_rec.partitions_end_pos = '||io_sql_rec.partitions_end_pos);

        -- parse number of defined partitions (no more than 3)
        io_sql_rec.partitions_count := 0;
        l_regexp := '\s*PARTITION\W';
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, io_sql_rec.partitions_start_pos, 1, 1);
        l_key_end_pos := 0;
        WHILE l_search_pos < io_sql_rec.partitions_end_pos LOOP
          io_sql_rec.partitions_count := io_sql_rec.partitions_count + 1;  
          l_regexp := ',s*PARTITION\W';
          l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, io_sql_rec.partitions_start_pos, 1, 1);
          EXIT WHEN l_key_end_pos = 0;
          EXIT WHEN io_sql_rec.partitions_count > 2;
          l_search_pos := l_key_end_pos;
        END LOOP;
        debug('Parsing: io_sql_rec.partitions_count = '||io_sql_rec.partitions_count);
        
      END IF;
    END IF;
  END parse_partitioning;

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
      parse_relational_columns_sql(io_sql_rec);
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
    

    -- search and parse "AS SELECT" section
    parse_as_select(
      io_sql_rec => io_sql_rec
    );

    -- parse partitioning types, positions
    parse_partitioning(
      io_sql_rec => io_sql_rec
    );

    -- read cort hints
    parse_params(
      io_sql_rec      => io_sql_rec,
      io_params_rec   => cort_exec_pkg.g_params
    );
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
  BEGIN
    initial_object_parse(
      io_sql_rec       => io_sql_rec,
      in_object_type   => 'TYPE',
      in_object_name   => in_type_name,
      in_object_owner  => in_owner_name
    );
  END parse_type_sql;
  
  -- Parse type body definition 
  PROCEDURE parse_type_body_sql(
    io_sql_rec    IN OUT NOCOPY gt_sql_rec,
    in_type_name  IN VARCHAR2,
    in_owner_name IN VARCHAR2
  )
  AS
  BEGIN
    initial_object_parse(
      io_sql_rec       => io_sql_rec,
      in_object_type   => 'TYPE BODY',
      in_object_name   => in_type_name,
      in_object_owner  => in_owner_name
    );
  END parse_type_body_sql;

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
    WHEN 'TYPE BODY' THEN
      parse_type_body_sql(
        io_sql_rec    => io_sql_rec,
        in_type_name  => in_object_name,
        in_owner_name => in_object_owner
      );
    END CASE;
  END parse_create_sql;

  -- Parse cort-index expression and checks that it valid create index statement on given table
  PROCEDURE parse_index_sql(
    io_sql_rec        IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_parse_only                CLOB;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_full_name                 VARCHAR2(32767);
    l_owner                     VARCHAR2(100); 
    l_name                      VARCHAR2(100); 
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
          l_owner := io_sql_rec.current_schema;
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
  PROCEDURE parse_table_indexes(
    in_sql_rec    IN gt_sql_rec,
    out_sql_arr   OUT NOCOPY gt_sql_arr 
  )
  AS
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_text                      CLOB;
    l_value                     CLOB;
    l_indx                      PLS_INTEGER;
    l_index_rec                 cort_exec_pkg.gt_index_rec;
    l_index_sql                 CLOB;
    l_indxe_sql_rec             gt_sql_rec;
  BEGIN
    -- find cort-index hints

    --#[release]index= 
    -- where [release] is current release value wrapped with square brackets []. This clause is optional
    
    l_regexp := cort_params_pkg.gc_prefix||get_release_regexp||cort_params_pkg.gc_index_regexp;
    
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
          debug('get_cort_indexes: index declaration found', TRIM(l_value));
          IF length(TRIM(l_value)) > 0 THEN
            parse_sql(in_sql => TRIM(l_value), out_sql_rec => l_indxe_sql_rec);
            out_sql_arr(out_sql_arr.COUNT+1) := l_indxe_sql_rec;
          END IF;  
        END IF;            
      END IF;

      l_indx := in_sql_rec.lexical_units_arr.NEXT(l_indx);
    END LOOP;
    
    -- parsing every index DDL
    FOR i in 1..out_sql_arr.COUNT LOOP
      out_sql_arr(i).current_schema := in_sql_rec.current_schema;
      parse_index_sql(
        io_sql_rec => out_sql_arr(i)
      );
        
      IF (out_sql_arr(i).table_name <> in_sql_rec.object_name) OR
         (out_sql_arr(i).table_owner <> in_sql_rec.object_owner)
      THEN
        cort_exec_pkg.raise_error('Only indexes for table "'||in_sql_rec.object_owner||'"."'||in_sql_rec.object_name||'" are allowed');
      END IF;    
/*

      -- modify original SQL - replace original names with temp ones.
      l_index_rec.owner := l_sql_arr(i).object_owner;
      l_index_rec.index_name := l_sql_arr(i).object_name;
      l_index_rec.rename_rec.temp_name := cort_exec_pkg.get_object_temp_name(
                                            in_object_type  => 'INDEX',
                                            in_object_name  => l_sql_arr(i).object_name, 
                                            in_object_owner => l_sql_arr(i).object_owner,
                                            in_prefix       => cort_params_pkg.gc_temp_prefix
                                          );
      debug('Index original DDL', l_sql_arr(i).original_sql);
      replace_names(
        in_table_rec => in_table_rec,
        in_index_rec => l_index_rec,
        io_sql_rec   => l_sql_arr(i),
        out_sql      => l_index_sql
      );

      cort_exec_pkg.add_change(
        io_change_arr,
        cort_exec_pkg.change_rec(
          in_change_sql => l_index_sql,
          in_revert_sql => 'DROP INDEX "'||l_index_rec.owner||'"."'||l_index_rec.rename_rec.temp_name||'"',
          in_group_type => 'RECREATE TABLE'  
        )
      ); */           
    END LOOP;
      
  END parse_table_indexes;
  

  FUNCTION get_partitions_sql(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_sql_rec          IN gt_sql_rec
  )
  RETURN CLOB
  AS
    l_partitions_sql        CLOB;
    l_empty_arr             cort_exec_pkg.gt_partition_arr;
  BEGIN
    IF cort_comp_pkg.comp_partitioning(in_source_table_rec, in_sql_rec) = 0 THEN
      IF in_source_table_rec.subpartitioning_type <> 'NONE' AND in_sql_rec.subpartitioning_type <> 'NONE' THEN 
        IF cort_comp_pkg.comp_subpartitioning(in_source_table_rec, in_sql_rec) = 0 THEN
          -- generate SQL for partitions definitions from source table
          l_partitions_sql := cort_comp_pkg.get_partitions_sql(
                                in_partition_arr    => in_source_table_rec.partition_arr,
                                in_subpartition_arr => in_source_table_rec.subpartition_arr 
                              );
        ELSE
          debug('Source table has different type of subpartitioning: '||in_source_table_rec.subpartitioning_type||', target table subpartitioning: '||in_sql_rec.subpartitioning_type);  
          l_partitions_sql := cort_comp_pkg.get_partitions_sql(
                                in_partition_arr    => in_source_table_rec.partition_arr,
                                in_subpartition_arr => l_empty_arr 
                              );
        END IF;                 
      ELSE
        l_partitions_sql := cort_comp_pkg.get_partitions_sql(
                              in_partition_arr    => in_source_table_rec.partition_arr,
                              in_subpartition_arr => l_empty_arr
                            );
      END IF;
    ELSE
      debug('Source table has different type of partitioning: '||in_source_table_rec.partitioning_type||', target table partitioning: '||in_sql_rec.partitioning_type);  
    END IF;
    
    RETURN l_partitions_sql;  
   
  END get_partitions_sql;
  
  -- modify partitions in original sql  
  PROCEDURE modify_partition_sql(
    in_source_table_rec    IN cort_exec_pkg.gt_table_rec,
    io_sql_rec             IN OUT NOCOPY gt_sql_rec
  )
  AS
    l_partitions_sql  CLOB;
    l_sql             CLOB;
  BEGIN
    IF io_sql_rec.partitioning_type IS NOT NULL AND in_source_table_rec.table_name IS NOT NULL THEN    
      l_partitions_sql := get_partitions_sql(
                            in_source_table_rec => in_source_table_rec,
                            in_sql_rec          => io_sql_rec
                          );
      IF l_partitions_sql IS NOT NULL THEN
        l_sql := SUBSTR(io_sql_rec.original_sql, 1, io_sql_rec.partitions_start_pos-1)||
                 '('||CHR(10)||l_partitions_sql||CHR(10)||')'||
                 SUBSTR(io_sql_rec.original_sql, io_sql_rec.partitions_end_pos);
        debug('Modified partitions sql', l_sql);
        io_sql_rec.original_sql := l_sql; 
      END IF;                             
    END IF;
  END modify_partition_sql;    


  -- replaces table name and all names of existing depending objects (constraints, log groups, indexes, lob segments) 
  PROCEDURE replace_table_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    io_sql_rec   IN OUT NOCOPY gt_sql_rec
  )
  AS
  BEGIN
    -- get all names in g_normalized sql
    find_table_substitutions(
      io_sql_rec   => io_sql_rec,
      in_table_rec => in_table_rec
    );
    
    find_type_synonyms(
      io_sql_rec   => io_sql_rec
    );
    
--    io_sql_rec.temp_name := in_table_rec.rename_rec.temp_name;

    -- when use normalized SQL then text in check constraints are normalized
    -- need to normalize it when we read the from dictionary before comparison

    int_replace_names(
      io_sql_rec => io_sql_rec
    );
  END replace_table_names;
  
  -- replace all cort synonyms for types with real type names  
  PROCEDURE replace_type_synonyms(
    io_sql_rec   IN OUT NOCOPY gt_sql_rec
  )
  AS
  BEGIN
    find_type_synonyms(
      io_sql_rec   => io_sql_rec
    );

    int_replace_names(
      io_sql_rec => io_sql_rec
    );
  END replace_type_synonyms;

  -- replaces index and table names for CREATE INDEX statement 
  PROCEDURE replace_index_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    in_index_rec IN cort_exec_pkg.gt_index_rec,
    io_sql_rec   IN OUT NOCOPY gt_sql_rec
  )
  AS
  BEGIN
    -- get all names
    find_index_substitutions(
      io_sql_rec   => io_sql_rec,
      in_index_rec => in_index_rec,
      in_table_rec => in_table_rec
    );

    int_replace_names(
      io_sql_rec => io_sql_rec
   );
  END replace_index_names;

  -- replaces sequence name 
  PROCEDURE replace_seq_names(
    in_rename_rec   IN cort_exec_pkg.gt_rename_rec,
    io_sql_rec      IN OUT NOCOPY gt_sql_rec
  )
  AS
  BEGIN
    -- get all names
    find_seq_substitutions(
      io_sql_rec    => io_sql_rec,
      in_rename_rec => in_rename_rec
    );

    int_replace_names(
      io_sql_rec => io_sql_rec
   );
  END replace_seq_names;

  -- replaces type name 
  PROCEDURE replace_type_names(
    in_rename_rec   IN cort_exec_pkg.gt_rename_rec,
    io_sql_rec      IN OUT NOCOPY gt_sql_rec
  )
  AS
  BEGIN
    find_type_synonyms(
      io_sql_rec   => io_sql_rec
    );

    -- get all names
    find_type_substitutions(
      io_sql_rec    => io_sql_rec,
      in_rename_rec => in_rename_rec
    );

    int_replace_names(
      io_sql_rec => io_sql_rec
   );
  END replace_type_names;

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
    
    parse_sql(io_expression, l_sql_rec);
    
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
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_indx   VARCHAR2(200); 
  BEGIN
    l_indx := in_object_name;
    IF g_replaced_names_indx.EXISTS(l_indx) THEN
      debug('get_original_name("'||l_indx||'") = '||g_replaced_names_indx(l_indx));
      RETURN g_replaced_names_indx(l_indx);
    ELSE
      debug('get_original_name("'||l_indx||'") = ???');
      RETURN l_indx;
    END IF;  
  END get_original_name;

  
  PROCEDURE cleanup
  AS
  BEGIN
    g_replaced_names_indx.DELETE;    
  END cleanup;
  

  -- parses explain create statement and return object type, owner and name
  PROCEDURE parse_explain_sql(
    in_sql           IN CLOB,
    out_object_type  OUT VARCHAR2,
    out_object_owner OUT VARCHAR2,
    out_object_name  OUT VARCHAR2
  )
  AS
    l_sql                       CLOB;
    l_sql_rec                   gt_sql_rec;
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_name                      VARCHAR2(100);
    
    FUNCTION read_name
    RETURN VARCHAR2
    AS
      l_ident                  VARCHAR2(100);
      l_key                    VARCHAR2(100);
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
    parse_sql(in_sql, l_sql_rec);
    l_sql := get_normalized_sql(
               in_sql_rec      => l_sql_rec, 
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

      l_name := read_next_name(l_sql, l_search_pos);
      
      IF l_name IS NOT NULL THEN
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
    
  END parse_explain_sql;


  FUNCTION parse_qualified_col_expr(
    in_qualified_col_name IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    s          VARCHAR2(4000);
    s2         VARCHAR2(4000);
    qname      VARCHAR2(4000);
    l_cnt      PLS_INTEGER;
    qname_subs arrays.gt_str_indx;
    obj_name   VARCHAR2(4000);
    obj_type   VARCHAR2(4000);
    l_systype  BOOLEAN;
  BEGIN
    -- get all double-quotes names and replace them with placeholders
    s := in_qualified_col_name;
    
    s := regexp_replace(s, '^TREAT\(SYS_NC_ROWINFO\$ AS', 'TREAT("SYS_NC_ROWINFO$" AS ', 1, 1);

    l_systype := regexp_like(s, '^SYS_TYPEID\((.+)\)$');
    IF l_systype THEN
      s := regexp_substr(s, '^SYS_TYPEID\((.+)\)$', 1, 1, null, 1);
    END IF;
    
    s2 := s;
    l_cnt := 1;
    LOOP 
      qname := regexp_substr(s, '"([^"]+)"', 1, l_cnt, null, 1);
      EXIT WHEN qname is null;
      IF NOT is_simple_name(qname) THEN 
        IF NOT qname_subs.EXISTS(qname) THEN
          qname_subs(qname) := '{NAME#'||l_cnt||'}';
        END IF;
        s2 := regexp_replace(s2, '"([^"]+)"', '{NAME#'||l_cnt||'}', 1, l_cnt);
      END IF;  
      l_cnt := l_cnt + 1;
    END LOOP;
    
    s := s2;
    l_cnt := 1;
    LOOP 
      obj_name := regexp_substr(s, 'TREAT\((.+) AS (.+)\)', 1, 1, null, 1);
      obj_type := regexp_substr(s, 'TREAT\((.+) AS (.+)\)', 1, 1, null, 2);
      EXIT WHEN obj_name is null;
      
      IF INSTR(obj_type, '.') > 1 THEN
        obj_type := SUBSTR(obj_type, INSTR(obj_type, '.') + 1);
      END IF; 
      l_cnt := l_cnt + 1;
      
      s := regexp_replace(s, 'TREAT\((.+) AS (.+)\)', obj_name||'['||obj_type||']', 1, 1);
    END LOOP;
    
    qname := qname_subs.FIRST;
    WHILE qname IS NOT NULL LOOP
      s := replace(s, qname_subs(qname), '"'||qname||'"' );
      qname := qname_subs.NEXT(qname);
    END LOOP;
    
     IF is_simple_name(s) THEN
       s := '"'||s||'"';
     END IF; 
/*    
    IF in_qualified_col_name NOT IN ('SYS_NC_OID$','SYS_NC_TYPEID$','SYS_NC_ROWINFO$') THEN 
      IF in_object_table_flag THEN 
        s := '"SYS_NC_ROWINFO$".'||s;
      END IF;
    END IF;  
*/    
    IF l_systype THEN
      s := 'SYS_TYPEID('||s||')';
    END IF;

    RETURN s;
  END parse_qualified_col_expr;
  
END cort_parse_pkg;
/