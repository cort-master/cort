CREATE OR REPLACE PACKAGE cort_params_pkg 
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
                                Removed param keep_partitions. Added copy_subpartitions, partitions_source, compression 
  21.00   | Rustam Kafarov    | Simplified API implementation. Split params into run and change specific
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
  gc_cast_object_data_prefix CONSTANT VARCHAR2(30)   := '<<CAST "%" AS "%"."%">>';
  
  -- special syntax regexp
  gc_index_regexp            CONSTANT VARCHAR2(500)  := '\s*INDEX\s*=';
  -- generated object name prefixes
  gc_rlbk_prefix             CONSTANT VARCHAR2(5)    := 'rlbk#';
  gc_temp_prefix             CONSTANT VARCHAR2(5)    := '~tmp#';
  gc_swap_prefix             CONSTANT VARCHAR2(5)    := '~swp#';
  gc_prev_prefix             CONSTANT VARCHAR2(5)    := 'prev#';
  gc_rename_prefix           CONSTANT VARCHAR2(5)    := 'old#';
  

  FUNCTION init(in_line IN NUMBER, in_default IN VARCHAR, in_regexp in VARCHAR2, in_case_sensitive IN BOOLEAN DEFAULT FALSE) RETURN cort_param_obj;
  FUNCTION init(in_line IN NUMBER, in_default IN BOOLEAN) RETURN cort_param_obj;
  FUNCTION init(in_line IN NUMBER, in_default IN NUMBER, in_regexp in VARCHAR2) RETURN cort_param_obj;
  FUNCTION init_dictionary(in_line IN NUMBER) RETURN cort_param_obj;
  

  $IF arrays.gc_long_name_supported $THEN
    gc_name_regexp    CONSTANT VARCHAR2(100) := '(([A-Za-z][A-Za-z0-9_#\$]{0,127})|("[^"]{1,128}"))';
  $ELSE
    gc_name_regexp    CONSTANT VARCHAR2(100) := '(([A-Za-z][A-Za-z0-9_#\$]{0,29})|("[^"]{1,30}"))';
  $END
  gc_ditcitonary_regexp  CONSTANT VARCHAR2(100) := gc_name_regexp||'=.*';
  
  gc_max_thread_number   NUMBER;
  
  -- run parameters
  -- should be placed in the same hint with OR REPLACE  
  TYPE gt_run_params_rec IS RECORD(
--    echo                     cort_param_obj := init($$plsql_line, array('SQL'),                                         'SQL|DEBUG|TIMING|REVERT_SQL'),
    test                     cort_param_obj := init($$plsql_line, FALSE),                                                -- enabling/disabling testing mode
    debug                    cort_param_obj := init($$plsql_line, FALSE),                                                -- enabling/disabling debugging mode
    application              cort_param_obj := init($$plsql_line, 'DEFAULT',                                            '([A-Za-z][A-Za-z0-9_]{0,19})'), -- application name
    release                  cort_param_obj := init($$plsql_line, '',                                                   '([A-Za-z0-9_\.]{1,10})'), -- release name
    build                    cort_param_obj := init($$plsql_line, '',                                                   '([A-F0-9]{18,30})'), -- build name
    threading                cort_param_obj := init($$plsql_line, 'AUTO',                                               'AUTO|MAX|NONE|[0-9]{1,2}'),     -- threading
    parallel                 cort_param_obj := init($$plsql_line, 1,                                                    '[0-9]{1,3}'),  -- parallel degree for internal DML/DDL
    async                    cort_param_obj := init($$plsql_line, FALSE),                                               -- run job copy asynchronously
    require_release          cort_param_obj := init($$plsql_line, FALSE),                                               -- require to prefix all change hints with release number. All unprefixed hints will be ignored
    --dbms_stats params 
    stats                    cort_param_obj := init($$plsql_line, 'AUTO',                                               'AUTO|NONE|COPY|ALL|APPROX_GLOBAL AND PARTITION|GLOBAL|GLOBAL AND PARTITION|PARTITION|SUBPARTITION'), --indicates how statistics should be gathered after recreating
    stats_estimate_pct       cort_param_obj := init($$plsql_line, 0.1,                                                  '(100)|([0-9]{1,2}(.[0-9]{1,6})?)'), -- gathering stats estimation percentage
    stats_method_opt         cort_param_obj := init($$plsql_line, '',                                                   '.*', TRUE), -- stats gathering method 
    stats_parallel           cort_param_obj := init($$plsql_line, 8,                                                    '[0-9]{1,3}'),  -- parallel degree for dbms_stats
    purge                    cort_param_obj := init($$plsql_line, FALSE)                                                -- enabling/disabling PURGE option when dropping objects and FORCE option when dropping types 
  );  

  -- change parameters
  -- could be prefixed with release number and specified in separate comments from run params
  TYPE gt_params_rec IS RECORD(
    alias                    cort_param_obj := init($$plsql_line, 'A',                                                  '([A-Za-z][A-Za-z0-9_#$]{0,29})'),-- default alias used for data migration script
    short_name               cort_param_obj := init($$plsql_line, '',                                                   '([^"]{1,20})'),-- short name for names longer than 20 to create unique naming for previous versions
    change                   cort_param_obj := init($$plsql_line, 'OPTIMISED',                                          'OPTIMISED|ALWAYS|RECREATE|MOVE|ALTER'), -- indicates default CORT behaviour in case of changing structure
    physical_attr            cort_param_obj := init($$plsql_line, '()',                                                 'TABLE|PARTITION|SUBPARTITION|INDEX|INDEX_PARTITION|INDEX_SUBPARTITION|LOB|LOB_PARTITION|LOB_SUBPARTITION'),-- indicates if physical attributes should not be ignored
    data                     cort_param_obj := init($$plsql_line, 'COPY',                                               'COPY|NONE|DEFERRED_COPY|PRELIMINARY_COPY'),-- indicates how data should be copied
    compare                  cort_param_obj := init($$plsql_line, '(IGNORE_UNUSED,IGNORE_INVISIBLE)',                   'IGNORE_UNUSED|IGNORE_INVISIBLE|IGNORE_ORDER'), -- indicates method of comparing columns
    references               cort_param_obj := init($$plsql_line, 'VALIDATE',                                           'VALIDATE|NOVALIDATE|DROP'), -- indicates how to restore FK references from another tables in case of table/PK recreate
    data_values              cort_param_obj := init_dictionary($$plsql_line),                                            -- columns new values which override default value. Each value should be placed into individual comment
    data_filter              cort_param_obj := init($$plsql_line, '',                                                   '.*', TRUE), -- filtering condition which could be injected after FROM prev$<table_name> a. Individual comment
    data_source              cort_param_obj := init($$plsql_line, '',                                                   '.*', TRUE), -- custom source for data which overrides standard source from prev$<table_name> and data_filter. Individual comment 
    partitions_source        cort_param_obj := init($$plsql_line, '',                                                   gc_name_regexp||'|""', TRUE) -- table name where partitions need to be copied from. By default all existing partitions are retainedif only 1 partition is specified .
  ); 

  TYPE gt_params_arr IS TABLE OF cort_param_obj INDEX BY VARCHAR2(30);
  
  -- internal function used for Oracle version < 12.
  $IF dbms_db_version.version < 12 $THEN
  FUNCTION get_params_rec RETURN gt_params_rec;
  PROCEDURE set_params_rec(in_value IN gt_params_rec);

  FUNCTION get_run_params_rec RETURN gt_run_params_rec;
  PROCEDURE set_run_params_rec(in_value IN gt_run_params_rec);
  $END
  

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
  
  -- return single parameter by name 
  FUNCTION get_run_param(
    in_params_rec IN gt_run_params_rec,
    in_param_name IN VARCHAR2
  )
  RETURN cort_param_obj;
  
  -- return single parameter value by name 
  FUNCTION get_run_param_value(
    in_params_rec IN gt_run_params_rec,
    in_param_name IN VARCHAR2
  )
  RETURN VARCHAR2;
  
  -- set single parameter value by name 
  PROCEDURE set_run_param_value(
    io_params_rec  IN OUT NOCOPY gt_run_params_rec,
    in_param_name  IN VARCHAR2,
    in_param_value IN VARCHAR2
  );
  
  -- reset all params to default values
  PROCEDURE reset_params(
    io_params_rec IN OUT NOCOPY gt_params_rec
  );

  -- reset all params to default values
  PROCEDURE reset_run_params(
    io_params_rec IN OUT NOCOPY gt_run_params_rec
  );

  -- return arrays of param names
  FUNCTION get_param_names RETURN arrays.gt_str_arr;
  
  -- return arrays of run param names
  FUNCTION get_run_param_names RETURN arrays.gt_str_arr;

  FUNCTION param_exists(in_param_name IN VARCHAR2) 
  RETURN BOOLEAN;

  FUNCTION run_param_exists(in_param_name IN VARCHAR2) 
  RETURN BOOLEAN;

  FUNCTION write_to_xml(
    in_params_rec IN gt_params_rec 
  )
  RETURN CLOB;

  FUNCTION write_to_xml(
    in_params_rec IN gt_run_params_rec 
  )
  RETURN CLOB;

  PROCEDURE read_from_xml(
    io_params_rec IN OUT NOCOPY gt_params_rec,
    in_xml        IN XMLType
  );
  
  PROCEDURE read_from_xml(
    io_params_rec IN OUT NOCOPY gt_run_params_rec,
    in_xml        IN XMLType
  );

  -- for debug purposes only 
  PROCEDURE print_params(in_params_rec IN gt_params_rec);

  -- for debug purposes only 
  PROCEDURE print_params(in_params_rec IN gt_run_params_rec);
  
END cort_params_pkg;
/
