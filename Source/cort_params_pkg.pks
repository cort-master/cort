CREATE OR REPLACE PACKAGE cort_params_pkg 
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
  Description: Type and API for main application parameters
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added API to read/write param default values
  15.00   | Rustam Kafarov    | Added IGNORE_ORDER param
  16.00   | Rustam Kafarov    | Added DEFERRED_DATA_COPY param
  17.00   | Rustam Kafarov    | Added TIMING param
  18.03   | Rustam Kafarov    | Added STATS param instead of keep_stats
  19.00   | Rustam Kafarov    | All parameters defined in this package  
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- static configuration params
  -- configuration
  gc_schema                  CONSTANT VARCHAR2(30)   := SYS_CONTEXT('USERENV','CURRENT_USER');
  gc_stat_table              CONSTANT VARCHAR2(30)   := 'CORT_STAT'; 
  gc_import_stat_table       CONSTANT VARCHAR2(30)   := 'CORT_IMPORT_STAT';
  gc_context_name            CONSTANT VARCHAR2(30)   := '';  -- specify here context name
  gc_context_setter          CONSTANT VARCHAR2(1000) := '';  -- specify here context setter procedure
  -- special control symbols
  gc_prefix                  CONSTANT VARCHAR2(1)    := '#';
  gc_value_prefix            CONSTANT VARCHAR2(30)   := '=';
  gc_force_value_prefix      CONSTANT VARCHAR2(30)   := '==';
  -- special syntax regexp
  gc_index_regexp            CONSTANT VARCHAR2(500)  := '\s*INDEX\s*=';
  gc_data_filter_regexp      CONSTANT VARCHAR2(1000) := '\s*FILTER\s*=';
  gc_sql_regexp              CONSTANT VARCHAR2(1000) := '\s*SQL\s*=';
  -- generated object name prefixes
  gc_rlbk_prefix             CONSTANT VARCHAR2(5)    := 'rlbk#';
  gc_temp_prefix             CONSTANT VARCHAR2(5)    := '~tmp#';
  gc_swap_prefix             CONSTANT VARCHAR2(5)    := '~swp#';
  gc_prev_prefix             CONSTANT VARCHAR2(5)    := 'prev#';
  gc_rename_prefix           CONSTANT VARCHAR2(5)    := 'old#';

  FUNCTION init(in_line IN NUMBER, in_default IN VARCHAR, in_regexp in VARCHAR2, in_case_sensitive IN BOOLEAN DEFAULT FALSE) RETURN cort_param_obj;
  FUNCTION init(in_line IN NUMBER, in_default IN BOOLEAN) RETURN cort_param_obj;
  FUNCTION init(in_line IN NUMBER, in_default IN NUMBER, in_regexp in VARCHAR2) RETURN cort_param_obj;
  FUNCTION init(in_line IN NUMBER, in_default IN ARRAY, in_regexp in VARCHAR2) RETURN cort_param_obj;
  
  -- dynamic params, define CORT behavior
  TYPE gt_params_rec IS RECORD(         
    application              cort_param_obj := init($$plsql_line, 'DEFAULT',                                            '([A-Za-z][A-Za-z0-9_]{0,19})'),
    prev_schema              cort_param_obj := init($$plsql_line, '',                                                   '([A-Za-z][A-Za-z0-9_#$]{0,29})'),
    alias                    cort_param_obj := init($$plsql_line, 'A',                                                  '([A-Za-z][A-Za-z0-9_#$]{0,29})'),
    parallel                 cort_param_obj := init($$plsql_line, 1,                                                    '[0-9]{1,3}'),  
--    echo                     cort_param_obj := init($$plsql_line, array('SQL'),                                         'SQL|DEBUG|TIMING'),
    test                     cort_param_obj := init($$plsql_line, FALSE),
    debug                    cort_param_obj := init($$plsql_line, FALSE),
    change                   cort_param_obj := init($$plsql_line, 'OPTIMISED',                                          'OPTIMISED|ALWAYS|RECREATE|MOVE|ALTER'),
    physical_attr            cort_param_obj := init($$plsql_line, array(),                                              'TABLE|PARTITION|SUBPARTITION|INDEX|INDEX_PARTITION|INDEX_SUBPARTITION|LOB'),
    tablespace               cort_param_obj := init($$plsql_line, array(),                                              'TABLE|PARTITION|SUBPARTITION|INDEX|INDEX_PARTITION|INDEX_SUBPARTITION|LOB'),
    data                     cort_param_obj := init($$plsql_line, 'COPY',                                               'COPY|NONE|DEFERRED_COPY|PRELIMINARY_COPY'),
    drop_column              cort_param_obj := init($$plsql_line, 'DROP',                                               'DROP|SET_UNUSED|SET_INVISIBLE|RECREATE'),
    compare                  cort_param_obj := init($$plsql_line, array('IGNORE_UNUSED'),                               'IGNORE_UNUSED|IGNORE_INVISIBLE|IGNORE_ORDER'),
    keep_partitions          cort_param_obj := init($$plsql_line, array('PARTITIONS','SUBPARTITIONS'),                  'PARTITIONS|SUBPARTITIONS'),
    keep_objects             cort_param_obj := init($$plsql_line, array('REFERENCES','PRIVILEGES','TRIGGERS',
                                                                        'POLICIES','COMMENTS'),                         'REFERENCES|PRIVILEGES|TRIGGERS|POLICIES|COMMENTS'),
    validate                 cort_param_obj := init($$plsql_line, FALSE),                                               -- validate references, partitions
    -- threading                                   
    $IF cort_options_pkg.gc_threading $THEN
    threading                cort_param_obj := init($$plsql_line, 'AUTO',                                               'AUTO|MAX|NONE|[0-9]{1,2}'),
    $END
    --dbms_stats params 
    stats                    cort_param_obj := init($$plsql_line, 'AUTO',                                               'AUTO|NONE|COPY|ALL|APPROX_GLOBAL AND PARTITION|GLOBAL|GLOBAL AND PARTITION|PARTITION|SUBPARTITION'),
    stats_estimate_pct       cort_param_obj := init($$plsql_line, 0.1,                                                  '(100)|([0-9]{1,2}(.[0-9]{1,6})?)'),
    stats_method_opt         cort_param_obj := init($$plsql_line, '{}',                                                 '{[^{}]*}', TRUE) -- case sensitive
  ); 

  TYPE gt_params_arr IS TABLE OF cort_param_obj INDEX BY VARCHAR2(30);
  
  -- setter for dynamic SQL - for intenal use only!!!
  PROCEDURE set_params_rec(in_params_rec IN gt_params_rec);
  
  -- getter for dynamic SQL - for intenal use only!!!
  FUNCTION get_params_rec
  RETURN gt_params_rec;

  -- setter for dynamic SQL - for intenal use only!!!
  PROCEDURE set_params_arr(in_params_arr IN gt_params_arr);
  
  -- getter for dynamic SQL - for intenal use only!!!
  FUNCTION get_params_arr
  RETURN gt_params_arr;
  
  
  -- return single parameter by name 
  FUNCTION get_param(
    in_params_rec IN gt_params_rec,
    in_param_name IN VARCHAR2
  )
  RETURN cort_param_obj;
  
  -- return single parameter value by name 
  FUNCTION get_param_value(
    in_params_rec IN gt_params_rec,
    in_param_name IN VARCHAR2
  )
  RETURN VARCHAR2;
  
  -- set single parameter value by name 
  PROCEDURE set_param_value(
    io_params_rec  IN OUT NOCOPY gt_params_rec,
    in_param_name  IN VARCHAR2,
    in_param_value IN VARCHAR2
  );
  
  -- return arrays of param names
  FUNCTION get_param_names RETURN arrays.gt_str_arr;
  FUNCTION get_param_names_indx RETURN arrays.gt_str_indx;

  FUNCTION rec_to_array(in_params_rec IN gt_params_rec)
  RETURN gt_params_arr;

  FUNCTION array_to_rec(in_params_arr IN gt_params_arr)
  RETURN gt_params_rec;

  FUNCTION array_to_xml(
    in_params_arr IN gt_params_arr
  )
  RETURN CLOB;

  FUNCTION rec_to_xml(
    in_params_rec IN gt_params_rec 
  )
  RETURN CLOB;

  PROCEDURE read_from_xml(
    in_xml        IN XMLType,
    io_params_arr IN OUT NOCOPY gt_params_arr 
  );

  PROCEDURE read_from_xml(
    in_xml        IN XMLType,
    io_params_rec IN OUT NOCOPY gt_params_rec
  );
  
  PROCEDURE print_params(in_params_rec IN gt_params_rec);

END cort_params_pkg;
/
