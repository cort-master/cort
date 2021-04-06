CREATE OR REPLACE PACKAGE BODY cort_exec_pkg
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
  Description: Main package executing table recreation and rollback with current user privileges
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.01   | Rustam Kafarov    | Added support for sequences, indexes, builds, create table as select. Orevall improvements
  15.00   | Rustam Kafarov    | Added data copy/move
  16.00   | Rustam Kafarov    | Added defferred data copy, rename table, create prev synonym
  17.00   | Rustam Kafarov    | Standardised input params for create_or_replace callback procedures, used current_schema.
  18.00   | Rustam Kafarov    | Used new parsing API. Fixed create as select from itself.
  19.00   | Rustam Kafarov    | Revised parameters
  20.00   | Rustam Kafarov    | Added support of long names introduced in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------
*/

  TYPE    gt_index_compare_rec        IS RECORD(
    index_name       arrays.gt_name,
    index_owner      arrays.gt_name,
    temp_name        arrays.gt_name,
    source_tab_indx  PLS_INTEGER,
    target_tab_indx  PLS_INTEGER,
    compare_result   PLS_INTEGER,
    frwd_ddl_arr     arrays.gt_clob_arr,
    rlbk_ddl_arr     arrays.gt_clob_arr
  );
  TYPE    gt_index_compare_arr        IS TABLE OF gt_index_compare_rec        INDEX BY PLS_INTEGER;

  SUBTYPE gt_all_indexes_rec          IS all_indexes%ROWTYPE;
  SUBTYPE gt_all_part_indexes_rec     IS all_part_indexes%ROWTYPE;
  SUBTYPE gt_all_join_ind_columns_rec IS all_join_ind_columns%ROWTYPE;
  SUBTYPE gt_all_constraints_rec      IS all_constraints%ROWTYPE;

  TYPE    gt_all_indexes_arr          IS TABLE OF gt_all_indexes_rec          INDEX BY PLS_INTEGER;
  TYPE    gt_all_join_ind_columns_arr IS TABLE OF gt_all_join_ind_columns_rec INDEX BY PLS_INTEGER;
  TYPE    gt_all_constraints_arr      IS TABLE OF gt_all_constraints_rec      INDEX BY PLS_INTEGER;

  g_job_rec              cort_jobs%ROWTYPE;

  ge_table_not_exist     EXCEPTION;
  PRAGMA                 EXCEPTION_INIT(ge_table_not_exist, -00942);


  -- output debug text
  PROCEDURE debug(
    in_text      IN CLOB,
    in_details   IN CLOB DEFAULT NULL
  )
  AS
  BEGIN
    IF g_params.debug.get_bool_value THEN
      cort_log_pkg.debug(substr(in_text,1, 4000), in_details);
    END IF;
  END debug;

  FUNCTION get_routin_name(
    in_owner IN VARCHAR2,
    in_name  IN VARCHAR2,
    in_line  IN NUMBER,
    in_type  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result arrays.gt_name;
  BEGIN
    BEGIN
      SELECT routine_name
        INTO l_result
        FROM (SELECT a.* ,
                     last_value(UPPER(REGEXP_SUBSTR(text, '(PROCEDURE|FUNCTION)\s+([A-Za-z][A-Za-z0-9_#$]{0,29})', 1, 1, 'i', 2)) ignore nulls) over(order by line) as routine_name
                FROM all_source a
               WHERE owner = in_owner
                 AND name = in_name
                 AND type = in_type
             )
       WHERE line = in_line;
    EXCEPTION
      WHEN no_data_found THEN
        l_result := null;
    END;
    l_result := NVL(l_result,in_name);
    RETURN l_result;
  END get_routin_name;

  PROCEDURE start_timer
  AS
    l_owner   arrays.gt_name;
    l_name    arrays.gt_name;
    l_line    NUMBER;
    l_type    VARCHAR2(100);
    l_routine VARCHAR2(100);
  BEGIN
    IF g_params.debug.get_bool_value THEN
      owa_util.who_called_me(
        owner          => l_owner,
        name           => l_name,
        lineno         => l_line,
        caller_t       => l_type
      );
      l_routine := get_routin_name(l_owner, l_name, l_line, l_type);
      IF l_routine IS NOT NULL THEN
        cort_log_pkg.start_timer(l_routine);
      END IF;
    END IF;
  END start_timer;

  PROCEDURE stop_timer
  AS
    l_owner   arrays.gt_name;
    l_name    arrays.gt_name;
    l_line    NUMBER;
    l_type    VARCHAR2(100);
    l_routine VARCHAR2(100);
  BEGIN
    IF g_params.debug.get_bool_value THEN
      owa_util.who_called_me(
        owner          => l_owner,
        name           => l_name,
        lineno         => l_line,
        caller_t       => l_type
      );
      l_routine := get_routin_name(l_owner, l_name, l_line, l_type);
      IF l_routine IS NOT NULL THEN
        cort_log_pkg.stop_timer(l_routine);
      END IF;
    END IF;
  END stop_timer;


  -- format begin/end message
  FUNCTION format_message(
    in_template     IN VARCHAR2,
    in_object_type  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(1000);
  BEGIN
    l_result := in_template;
    l_result := REPLACE(l_result, '[object_type]', in_object_type);
    l_result := REPLACE(l_result, '[object_owner]', in_object_owner);
    l_result := REPLACE(l_result, '[object_name]', in_object_name);
    RETURN l_result;
  END format_message;

  -- output sql statement
  PROCEDURE echo_sql(
    in_sql IN CLOB
  )
  AS
  BEGIN
    IF SUBSTR(in_sql,-1) = ';' THEN
      -- PL/SQL BLOCK
      cort_log_pkg.echo(in_sql||CHR(10)||'/');
    ELSE
      -- DDL or SQL
      cort_log_pkg.echo(in_sql||';');
    END IF;
  END echo_sql;


  PROCEDURE raise_error(
    in_msg  IN VARCHAR,
    in_code IN NUMBER DEFAULT -20000
  )
  AS
  BEGIN
    cort_log_pkg.error(in_msg);
    RAISE_APPLICATION_ERROR(in_code, in_msg, TRUE);
  END raise_error;

  PROCEDURE check_if_process_active(in_job_rec IN cort_jobs%ROWTYPE)
  AS
  BEGIN
    IF in_job_rec.sid IS NULL THEN
      RETURN;
    END IF;

    IF NOT cort_job_pkg.is_job_alive(in_job_rec) THEN
      raise_error('Operation cancelled for sid = '||in_job_rec.sid, -20900);
    END IF;

    IF in_job_rec.action = 'THREADING' THEN
      cort_job_pkg.check_sibling_jobs_alive(in_job_rec);
    END IF;
  END check_if_process_active;


  -- wrapper for oracle 10g to support CLOB
  PROCEDURE execute_immediate_10g(
    in_sql IN CLOB
  )
  AS
    l_ddl      VARCHAR2(32767);
    l_str_arr  dbms_sql.varchar2a;
    h          INTEGER;
    l_cnt      NUMBER;
  BEGIN
    IF LENGTH(in_sql) <= 32767 THEN
      l_ddl := in_sql;
      EXECUTE IMMEDIATE l_ddl;
    ELSE
      cort_aux_pkg.clob_to_varchar2a(
        in_clob     => in_sql,
        out_str_arr => l_str_arr
      );
      h := dbms_sql.open_cursor;
      BEGIN
        dbms_sql.parse(h, l_str_arr, 1, l_str_arr.COUNT, FALSE, dbms_sql.native);
        l_cnt := dbms_sql.execute(h);
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.error('Error in parsing/executing sql', in_sql);
          IF dbms_sql.is_open(h) THEN
            dbms_sql.close_cursor(h);
          END IF;
          RAISE;
      END;
      IF dbms_sql.is_open(h) THEN
        dbms_sql.close_cursor(h);
      END IF;
    END IF;
  END execute_immediate_10g;

  -- executes DDL command
  PROCEDURE execute_immediate(
    in_sql   IN CLOB,
    in_echo  IN BOOLEAN DEFAULT TRUE,
    in_test  IN BOOLEAN DEFAULT FALSE
  )
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    cort_exec_pkg.check_if_process_active(in_job_rec => g_job_rec);

    IF in_sql IS NOT NULL AND LENGTH(in_sql) > 0 THEN
      IF in_echo THEN
        echo_sql(in_sql);
      END IF;

      IF in_test THEN
        cort_log_pkg.test(
          in_text => in_sql
        );
      ELSE
        cort_log_pkg.execute(
          in_text      => in_sql
        );
        BEGIN
        $IF dbms_db_version.version >= 11 $THEN
          EXECUTE IMMEDIATE in_sql;
        $ELSE
          execute_immediate_10g(in_sql);
        $END
        EXCEPTION
          WHEN OTHERS THEN
            cort_log_pkg.update_exec_time;
            cort_log_pkg.error('Error in parsing/executing sql',in_sql);
            RAISE;
        END;
        cort_log_pkg.update_exec_time;
      END IF;
    END IF;
    COMMIT;
  END execute_immediate;

  -- executes PL/SQL convertion from XML type
  PROCEDURE execute_immediate_xml(
    in_sql   IN CLOB,
    in_xml   IN XMLTYPE
  )
  AS
  BEGIN
    cort_exec_pkg.check_if_process_active(in_job_rec => g_job_rec);
    IF in_sql IS NOT NULL AND LENGTH(in_sql) > 0 THEN
      cort_log_pkg.execute(
        in_text => in_sql
      );
      BEGIN
        EXECUTE IMMEDIATE in_sql USING IN in_xml;
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.update_exec_time;
          cort_log_pkg.error('Error in parsing/executing XML sql ',in_sql);
          RAISE;
      END;
      cort_log_pkg.update_exec_time;
    END IF;
  END execute_immediate_xml;

  -- executes PL/SQL convertion to XML type
  PROCEDURE execute_immediate_xml(
    in_sql   IN CLOB,
    out_xml  OUT NOCOPY XMLTYPE
  )
  AS
  BEGIN
    cort_exec_pkg.check_if_process_active(in_job_rec => g_job_rec);
    IF in_sql IS NOT NULL AND LENGTH(in_sql) > 0 THEN
      cort_log_pkg.execute(
        in_text => in_sql
      );
      BEGIN
        EXECUTE IMMEDIATE in_sql USING OUT out_xml;
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.update_exec_time;
          cort_log_pkg.error('Error in parsing/executing XML sql', in_sql);
          RAISE;
      END;
      cort_log_pkg.update_exec_time;
    END IF;
  END execute_immediate_xml;

  -- executes PL/SQL block with single string param
  PROCEDURE execute_immediate_param(
    in_sql         IN VARCHAR2,
    in_param_value IN VARCHAR2
  )
  AS
  BEGIN
    cort_exec_pkg.check_if_process_active(in_job_rec => g_job_rec);
    IF in_sql IS NOT NULL AND LENGTH(in_sql) > 0 THEN
      cort_log_pkg.execute(
        in_text => in_sql
      );
      BEGIN
        EXECUTE IMMEDIATE in_sql USING IN in_param_value;
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.update_exec_time;
          cort_log_pkg.error('Error in parsing/executing parameters sql', in_sql);
          RAISE;
      END;
      cort_log_pkg.update_exec_time;
    END IF;
  END execute_immediate_param;

  PROCEDURE int_apply_changes(
    in_frwd_alter_stmt_arr IN arrays.gt_clob_arr, -- forward alter statements
    in_rlbk_alter_stmt_arr IN arrays.gt_clob_arr, -- rollback alter statements
    io_last_ddl_index      IN OUT NOCOPY PLS_INTEGER,
    in_revert              IN BOOLEAN DEFAULT TRUE,
    in_test                IN BOOLEAN DEFAULT FALSE,
    in_echo                IN BOOLEAN DEFAULT TRUE
  )
  AS
    l_indx                 PLS_INTEGER;
    l_first_indx           PLS_INTEGER;
    e_compiled_with_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_compiled_with_errors,-24344);
  BEGIN
    start_timer;

    IF io_last_ddl_index IS NULL THEN
      l_indx := in_frwd_alter_stmt_arr.FIRST;
    ELSE
      l_indx := in_frwd_alter_stmt_arr.NEXT(io_last_ddl_index);
    END IF;
    l_first_indx := l_indx;
    WHILE l_indx IS NOT NULL LOOP
      IF LENGTH(in_frwd_alter_stmt_arr(l_indx)) > 0 THEN
        BEGIN
          execute_immediate(in_frwd_alter_stmt_arr(l_indx), in_echo, in_test);
          io_last_ddl_index := l_indx;
        EXCEPTION
          -- if trigger created with errors then do not raise exception and carry on
          WHEN e_compiled_with_errors THEN
            NULL;
          WHEN OTHERS THEN
            IF in_revert THEN
              debug('Error. Rolling back...');
              l_indx := in_rlbk_alter_stmt_arr.PRIOR(l_indx);
              WHILE l_indx IS NOT NULL AND l_indx >= l_first_indx LOOP
                execute_immediate(in_rlbk_alter_stmt_arr(l_indx), in_echo, in_test);
                l_indx := in_rlbk_alter_stmt_arr.PRIOR(l_indx);
              END LOOP;
              debug('End of rollback...');
            END IF;
            cort_log_pkg.echo('io_last_ddl_index = '||io_last_ddl_index);
            cort_log_pkg.error('Error. Changes reverted...');
            RAISE;
        END;
      END IF;
      -- go to next element
      l_indx := in_frwd_alter_stmt_arr.NEXT(l_indx);
    END LOOP;

    stop_timer;
  END int_apply_changes;

  -- execute DDL changes
  PROCEDURE apply_changes(
    in_frwd_alter_stmt_arr IN arrays.gt_clob_arr, -- forward alter statements
    in_rlbk_alter_stmt_arr IN arrays.gt_clob_arr, -- rollback alter statements
    io_last_ddl_index      IN OUT NOCOPY PLS_INTEGER, -- index in array from which we want to start
    in_revert              IN BOOLEAN DEFAULT TRUE,
    in_test                IN BOOLEAN DEFAULT FALSE,
    in_echo                IN BOOLEAN DEFAULT TRUE
  )
  AS
    l_frwd_alter_stmt_arr  arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr  arrays.gt_clob_arr;
  BEGIN
    $IF cort_options_pkg.gc_threading $THEN
      cort_thread_exec_pkg.find_threading_statements(
        in_frwd_alter_stmt_arr  => in_frwd_alter_stmt_arr,
        in_rlbk_alter_stmt_arr  => in_rlbk_alter_stmt_arr,
        out_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
        out_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr
      );
      int_apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr,
        io_last_ddl_index      => io_last_ddl_index,
        in_revert              => in_revert,
        in_test                => in_test,
        in_echo                => in_echo
      );
    $ELSE
      int_apply_changes(
        in_frwd_alter_stmt_arr => in_frwd_alter_stmt_arr,
        in_rlbk_alter_stmt_arr => in_rlbk_alter_stmt_arr,
        io_last_ddl_index      => io_last_ddl_index,
        in_revert              => in_revert,
        in_test                => in_test,
        in_echo                => in_echo
      );
    $END
  END apply_changes;

  -- execute DDL changes
  PROCEDURE apply_changes(
    in_frwd_alter_stmt_arr IN arrays.gt_clob_arr, -- forward alter statements
    in_rlbk_alter_stmt_arr IN arrays.gt_clob_arr, -- rollback alter statements
    in_test                IN BOOLEAN DEFAULT FALSE,
    in_echo                IN BOOLEAN DEFAULT TRUE
  )
  AS
    l_last_ddl_index      PLS_INTEGER;
  BEGIN
    apply_changes(
      in_frwd_alter_stmt_arr => in_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => in_rlbk_alter_stmt_arr,
      io_last_ddl_index      => l_last_ddl_index,
      in_revert              => TRUE,
      in_test                => in_test,
      in_echo                => in_echo
    );
  END apply_changes;

  PROCEDURE apply_changes_ignore_errors(
    io_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    in_test        IN BOOLEAN DEFAULT FALSE,
    in_echo        IN BOOLEAN DEFAULT TRUE,
    in_reverse     IN BOOLEAN DEFAULT FALSE
  )
  AS
    l_indx                 PLS_INTEGER;
    e_compiled_with_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_compiled_with_errors,-24344);
  BEGIN
    IF in_reverse THEN
      l_indx := io_stmt_arr.LAST;
    ELSE
      l_indx := io_stmt_arr.FIRST;
    END IF;
    WHILE l_indx IS NOT NULL LOOP
      IF LENGTH(io_stmt_arr(l_indx)) > 0 THEN
        BEGIN
          execute_immediate(io_stmt_arr(l_indx), in_echo, in_test);
        EXCEPTION
          -- if trigger created with errors then do not raise exception and carry on
          WHEN e_compiled_with_errors THEN
            NULL;
          WHEN OTHERS THEN
            io_stmt_arr(l_indx) := NULL;
        END;
      END IF;
      -- go to next element
      IF in_reverse THEN
        l_indx := io_stmt_arr.PRIOR(l_indx);
      ELSE
        l_indx := io_stmt_arr.NEXT(l_indx);
      END IF;
    END LOOP;
  END apply_changes_ignore_errors;

  -- Returns TRUE if table exists otherwise returns FALSE
  FUNCTION object_exists(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_objects
     WHERE owner = in_object_owner
       AND object_name = in_object_name
       AND object_type = in_object_type;
    RETURN l_cnt > 0;
  END object_exists;

  -- Returns TRUE if table exists otherwise returns FALSE
  FUNCTION object_exists(
    in_rename_rec  IN gt_rename_rec
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_objects
     WHERE owner = in_rename_rec.object_owner
       AND object_name = in_rename_rec.current_name
       AND object_type = in_rename_rec.object_type;
    RETURN l_cnt > 0;
  END object_exists;

  -- Returns TRUE if constraint exists otherwise returns FALSE.
  FUNCTION constraint_exists(
    in_cons_name   IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_constraints
     WHERE owner = in_owner
       AND constraint_name = in_cons_name;
    RETURN l_cnt > 0;
  END constraint_exists;

  -- Returns TRUE if log group exists otherwise returns FALSE.
  FUNCTION log_group_exists(
    in_log_group_name IN VARCHAR2,
    in_owner          IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_log_groups
     WHERE owner = in_owner
       AND log_group_name = in_log_group_name;
    RETURN l_cnt > 0;
  END log_group_exists;

  -- return DROP object DDL
  FUNCTION get_drop_object_ddl(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_purge        IN BOOLEAN DEFAULT FALSE
  )
  RETURN VARCHAR2
  AS
    l_sql VARCHAR2(32767);
  BEGIN
    l_sql :=  'DROP '||in_object_type||' "'||in_object_owner||'"."'||in_object_name||'"';
    IF in_object_type = 'TABLE' THEN
      l_sql := l_sql||' CASCADE CONSTRAINTS';
      IF in_purge THEN
        l_sql := l_sql||' PURGE';
      END IF;
    END IF;

    IF in_object_type = 'INDEX' THEN
      l_sql := l_sql||' ONLINE';
    END IF;

    IF in_object_type = 'TYPE' THEN
      l_sql := l_sql||' FORCE';
    END IF;
    RETURN l_sql;
  END get_drop_object_ddl;

  -- warpper for TABLE
  FUNCTION get_drop_table_ddl(
    in_table_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'TABLE',
             in_object_name  => in_table_name,
             in_object_owner => in_owner
           );
  END get_drop_table_ddl;

  -- warpper for INDEX
  FUNCTION get_drop_index_ddl(
    in_index_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'INDEX',
             in_object_name  => in_index_name,
             in_object_owner => in_owner
           );
  END get_drop_index_ddl;

  -- warpper for SEQUENCE
  FUNCTION get_drop_sequence_ddl(
    in_sequence_name  IN VARCHAR2,
    in_owner          IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'SEQUENCE',
             in_object_name  => in_sequence_name,
             in_object_owner => in_owner
           );
  END get_drop_sequence_ddl;


  -- warpper for TYPE
  FUNCTION get_drop_type_ddl(
    in_type_name  IN VARCHAR2,
    in_owner      IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'TYPE',
             in_object_name  => in_type_name,
             in_object_owner => in_owner
           );
  END get_drop_type_ddl;

  -- warpper for VIEW
  FUNCTION get_drop_view_ddl(
    in_view_name  IN VARCHAR2,
    in_owner      IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'VIEW',
             in_object_name  => in_view_name,
             in_object_owner => in_owner
           );
  END get_drop_view_ddl;

  -- Drops object if it exists
  PROCEDURE exec_drop_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_echo         IN BOOLEAN DEFAULT TRUE,
    in_test         IN BOOLEAN DEFAULT FALSE,
    in_purge        IN BOOLEAN DEFAULT FALSE
  )
  AS
    l_sql           VARCHAR2(1000);
  BEGIN
    IF object_exists(
         in_object_type  => in_object_type,
         in_object_name  => in_object_name,
         in_object_owner => in_object_owner
       )
    THEN
      l_sql := get_drop_object_ddl(
                 in_object_type  => in_object_type,
                 in_object_name  => in_object_name,
                 in_object_owner => in_object_owner,
                 in_purge        => in_purge
               );
      execute_immediate(
        in_sql  => l_sql,
        in_echo => in_echo,
        in_test => in_test
      );
    END IF;
  END exec_drop_object;

  -- wrapper for table
  PROCEDURE exec_drop_table(
    in_table_name   IN VARCHAR2,
    in_owner        IN VARCHAR2,
    in_echo         IN BOOLEAN DEFAULT TRUE,
    in_test         IN BOOLEAN DEFAULT FALSE,
    in_purge        IN BOOLEAN DEFAULT TRUE
  )
  AS
  BEGIN
    start_timer;
    exec_drop_object(
      in_object_type  => 'TABLE',
      in_object_owner => in_owner,
      in_object_name  => in_table_name,
      in_echo         => in_echo,
      in_test         => in_test,
      in_purge        => in_purge
    );
    stop_timer;
  END exec_drop_table;


  PROCEDURE create_prev_synonym(
    in_synonym_name  IN VARCHAR2,
    in_table_name    IN VARCHAR2,
    in_table_owner   IN VARCHAR2
  )
  AS
    l_sql         CLOB;
    l_prev_schema arrays.gt_name;
  BEGIN
    debug('Create synonym "'||in_synonym_name||'" for "'||in_table_owner||'"."'||in_table_name||'"');
    l_prev_schema := g_params.prev_schema.get_value;
    IF l_prev_schema IS NOT NULL THEN
       -- prev schema synonym
       l_sql := 'begin
         "'||l_prev_schema||'".cort_prev_exec_pkg.create_prev_synonym(
           in_synonym_name => '''||in_synonym_name||''',
           in_table_name   => '''||in_table_name||''',
           in_table_owner  => '''||in_table_owner||'''
         );
       end;';
    ELSIF cort_params_pkg.gc_prev_prefix IS NOT NULL THEN
      -- same schema synonym
      l_sql := 'CREATE OR REPLACE SYNONYM "'||in_table_owner||'"."'||substr(cort_params_pkg.gc_prev_prefix||in_synonym_name, 1, 30)||'" FOR "'||in_table_owner||'"."'||in_table_name||'"';
    ELSE
      raise_error('Not set CORT prev schema/prefix');
    END IF;
    debug('Prev synonym SQL: '||l_sql);
    execute_immediate(
      in_sql  => l_sql,
      in_echo => FALSE,
      in_test => FALSE
    );
  END create_prev_synonym;

  PROCEDURE cleanup_history(
    in_object_owner  IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_type   IN VARCHAR2
  )
  AS
    l_revert_object_arr arrays.gt_str_arr;
  BEGIN
    SELECT revert_name
      BULK COLLECT
      INTO l_revert_object_arr
      FROM cort_objects
     WHERE object_owner = in_object_owner
       AND object_name = in_object_name
       AND object_type = in_object_type
       AND revert_name IS NOT NULL;

    FOR i IN 1..l_revert_object_arr.COUNT LOOP
      exec_drop_object(
        in_object_type  => in_object_type,
        in_object_name  => l_revert_object_arr(i),
        in_object_owner => in_object_owner
      );
    END LOOP;
    -- delete any previous records for given object
    cort_aux_pkg.cleanup_history(
      in_object_type   => in_object_type,
      in_object_name   => in_object_name,
      in_object_owner  => in_object_owner
    );
  END cleanup_history;

  -- return string higher than given but not longer than 30 chars
  FUNCTION get_next_name(in_name IN VARCHAR2)
  RETURN VARCHAR2
  AS
    l_next_name VARCHAR2(24);
    l_substr    VARCHAR2(24);
    l_number    BINARY_INTEGER;
  BEGIN
    l_substr := SUBSTR(in_name, 1, 24);
    l_substr := SUBSTR(in_name, -4, 4);
    l_number := utl_raw.cast_to_binary_integer(utl_raw.cast_to_raw(l_substr));
    l_number := l_number + 1;
    l_substr := utl_raw.cast_to_varchar2(utl_raw.cast_from_binary_integer(l_number));
    l_substr := REPLACE(l_substr, '"', '#');
    l_next_name := SUBSTR(in_name, 1, LENGTH(in_name)-4)||l_substr;
    RETURN l_next_name;
  END get_next_name;

  FUNCTION check_object_exist(
    in_owner IN VARCHAR2,
    in_name  IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    BEGIN
      SELECT 1
        INTO l_cnt
        FROM all_objects
       WHERE owner = in_owner
         AND object_name = in_name
         AND ROWNUM <= 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;
    RETURN l_cnt = 1;
  END check_object_exist;

  -- Returns temp name for given object (table|index|trigger|lob)
  FUNCTION get_object_temp_name(
    in_object_id   IN VARCHAR2,
    in_owner       IN VARCHAR2,
    in_prefix      IN VARCHAR2 DEFAULT cort_params_pkg.gc_temp_prefix
  )
  RETURN VARCHAR2
  AS
    l_temp_name VARCHAR2(24);
  BEGIN
    l_temp_name := in_prefix||TO_CHAR(in_object_id, 'fm0XXXXXXXXXXX');
    WHILE check_object_exist(in_owner, l_temp_name) LOOP
      l_temp_name := get_next_name(l_temp_name);
    END LOOP;
    RETURN l_temp_name;
  END get_object_temp_name;

  -- Returns temp name for current session (tab|ind|trg|lob)
  FUNCTION get_object_temp_name(
    in_object_type IN VARCHAR2,
    in_owner       IN VARCHAR2,
    in_prefix      IN VARCHAR2 DEFAULT cort_params_pkg.gc_temp_prefix
  )
  RETURN VARCHAR2
  AS
    l_object_type VARCHAR2(3);
    l_temp_name   VARCHAR2(24);
  BEGIN
    l_object_type := CASE in_object_type
                       WHEN 'TABLE'    THEN 'TAB'
                       WHEN 'INDEX'    THEN 'IND'
                       WHEN 'TRIGGER'  THEN 'TRG'
                       WHEN 'LOB'      THEN 'LOB'
                       WHEN 'SEQUENCE' THEN 'SEQ'
                       WHEN 'TYPE'     THEN 'TYP'
                       WHEN 'SYNONYM'  THEN 'SYN'
                       ELSE 'OBJ'
                     END;

                    --    AAAAA(5)   TTT(3)          XXXXXXXXXX(10)                    FFFFFF(6)
    l_temp_name := SUBSTR(in_prefix||l_object_type||cort_aux_pkg.get_time_str||TO_CHAR(SYS_CONTEXT('USERENV','SID'),'fm0XXXXX'),1, 24);
    WHILE check_object_exist(in_owner, l_temp_name) LOOP
      l_temp_name := get_next_name(l_temp_name);
    END LOOP;
    RETURN l_temp_name;
  END get_object_temp_name;

  -- Initializes and return gt_rename_rec record
  FUNCTION get_rename_rec(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_object_type  IN VARCHAR2
  )
  RETURN gt_rename_rec
  AS
    l_rec gt_rename_rec;
  BEGIN
    l_rec.object_type  := in_object_type;
    l_rec.object_name  := cort_parse_pkg.get_original_name(
                            in_object_type => in_object_type,
                            in_object_name => in_object_name
                          );
    l_rec.object_owner := in_object_owner;
    l_rec.current_name := in_object_name;

    SELECT object_id, generated
      INTO l_rec.object_id, l_rec.generated
      FROM all_objects
     WHERE owner = in_object_owner
       AND object_type = in_object_type
       AND object_name = in_object_name
       AND subobject_name IS NULL;

    l_rec.temp_name := get_object_temp_name(
                         in_object_type => in_object_type,
                         in_owner       => l_rec.object_owner,
                         in_prefix      => cort_params_pkg.gc_temp_prefix
                       );
    l_rec.cort_name := get_object_temp_name(
                         in_object_id   => l_rec.object_id,
                         in_owner       => l_rec.object_owner,
                         in_prefix      => cort_params_pkg.gc_rlbk_prefix
                       );
    l_rec.rename_name := get_object_temp_name(
                           in_object_id   => l_rec.object_id,
                           in_owner       => l_rec.object_owner,
                           in_prefix      => cort_params_pkg.gc_rename_prefix
                         );

    RETURN l_rec;
  END get_rename_rec;

  -- Initializes and return gt_rename_rec record for constraints and log groups
  FUNCTION get_rename_rec(
    in_table_name_rec IN gt_rename_rec,
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_object_type    IN VARCHAR2,
    in_index          IN PLS_INTEGER,
    in_generated      IN VARCHAR2
  )
  RETURN gt_rename_rec
  AS
    l_rec     gt_rename_rec;
    l_prefix  VARCHAR2(10);
  BEGIN
    l_rec.object_type  := in_object_type;
    l_rec.object_name  := cort_parse_pkg.get_original_name(
                            in_object_type => in_object_type,
                            in_object_name => in_object_name
                          );
    l_rec.object_owner := in_object_owner;
    l_rec.object_id := NULL;
    l_rec.generated := in_generated;
    l_rec.current_name := in_object_name;
    l_prefix := '$'||SUBSTR(in_object_type,1,1);
    l_rec.temp_name := in_table_name_rec.temp_name||l_prefix||TO_CHAR(in_index,'fm0XXX');
    l_rec.cort_name := in_table_name_rec.cort_name||l_prefix||TO_CHAR(in_index,'fm0XXX');
    l_rec.rename_name := in_table_name_rec.rename_name||l_prefix||TO_CHAR(in_index,'fm0XXX');
    l_rec.parent_object_name := in_table_name_rec.current_name;
    RETURN l_rec;
  END get_rename_rec;

  -- return column indx in table column_arr by name
  FUNCTION get_column_indx(
    in_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_column_name IN VARCHAR2
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF in_table_rec.column_indx_arr.EXISTS(in_column_name) THEN
      RETURN in_table_rec.column_indx_arr(in_column_name);
    ELSE
      IF in_table_rec.column_qualified_indx_arr.EXISTS(in_column_name) THEN
        RETURN in_table_rec.column_qualified_indx_arr(in_column_name);
      ELSE
        RETURN NULL;
      END IF;
    END IF;
  END get_column_indx;


  PROCEDURE read_subpartition_template(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_subpartition_templates_arr IS TABLE OF all_subpartition_templates%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_lob_templates_arr          IS TABLE OF all_lob_templates%ROWTYPE INDEX BY PLS_INTEGER;
    l_subpartition_templates_arr  t_subpartition_templates_arr;
    l_subpart_template_indx       arrays.gt_name_indx;
    l_lob_templates_arr           t_lob_templates_arr;
    l_lob_template_rec            gt_lob_template_rec;
    l_indx                        PLS_INTEGER;
    l_cnt                         PLS_INTEGER;
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_subpartition_templates_arr
      FROM all_subpartition_templates
     WHERE user_name = io_table_rec.owner
       AND table_name = io_table_rec.table_name
     ORDER BY subpartition_position;

    FOR i IN 1..l_subpartition_templates_arr.COUNT LOOP
      io_table_rec.subpartition_template_arr(i).subpartition_type := io_table_rec.subpartitioning_type;
      io_table_rec.subpartition_template_arr(i).subpartition_name := l_subpartition_templates_arr(i).subpartition_name;
      io_table_rec.subpartition_template_arr(i).subpartition_position := l_subpartition_templates_arr(i).subpartition_position;
      io_table_rec.subpartition_template_arr(i).tablespace_name := l_subpartition_templates_arr(i).tablespace_name;
      io_table_rec.subpartition_template_arr(i).high_bound := l_subpartition_templates_arr(i).high_bound;
      l_subpart_template_indx(l_subpartition_templates_arr(i).subpartition_name) := i;
    END LOOP;

    SELECT DISTINCT *   -- use distinct as a workaround for Oracle bug - missing join condition which lead to rows duplication
      BULK COLLECT
      INTO l_lob_templates_arr
      FROM all_lob_templates
     WHERE user_name = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_lob_templates_arr.COUNT LOOP
      l_lob_template_rec.subpartition_name := l_lob_templates_arr(i).subpartition_name;
      l_lob_template_rec.lob_column_name := l_lob_templates_arr(i).lob_col_name;
      l_lob_template_rec.lob_segment_name := l_lob_templates_arr(i).lob_segment_name;
      l_lob_template_rec.tablespace_name := l_lob_templates_arr(i).tablespace_name;
      IF l_subpart_template_indx.EXISTS(l_lob_template_rec.subpartition_name) THEN
        l_indx := l_subpart_template_indx(l_lob_template_rec.subpartition_name);
        l_cnt := io_table_rec.subpartition_template_arr(l_indx).lob_template_arr.COUNT;
        io_table_rec.subpartition_template_arr(l_indx).lob_template_arr(l_cnt+1) := l_lob_template_rec;
      END IF;
    END LOOP;

  END read_subpartition_template;

  -- read table attributes from all_tables/all_part_tables
  PROCEDURE read_table(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    in_read_data       IN BOOLEAN,
    out_table_rec      OUT NOCOPY gt_table_rec
  )
  AS
    l_all_tables_rec           all_all_tables%ROWTYPE;
    l_indexes_rec              all_indexes%ROWTYPE;
    l_part_tables_rec          all_part_tables%ROWTYPE;
    l_part_indexes_rec         all_part_indexes%ROWTYPE;
    l_tablespace_name_arr      arrays.gt_name_arr;
    l_block_size_arr           arrays.gt_num_arr;
    l_sql                      VARCHAR2(1000);
    l_cnt                      PLS_INTEGER;
  BEGIN
    start_timer;
    BEGIN
      SELECT *
        INTO l_all_tables_rec
        FROM all_all_tables
       WHERE owner = in_owner
         AND table_name = in_table_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        raise_error('Table "'||in_owner||'"."'||in_table_name||'" not found');
        RETURN;
    END;

    out_table_rec.compressed_partitions := FALSE;
    out_table_rec.owner :=  l_all_tables_rec.owner;
    out_table_rec.table_name := l_all_tables_rec.table_name;
    out_table_rec.tablespace_name := l_all_tables_rec.tablespace_name;
    out_table_rec.cluster_name := l_all_tables_rec.cluster_name;
    out_table_rec.cluster_owner := l_all_tables_rec.cluster_owner;
    out_table_rec.iot_name := l_all_tables_rec.iot_name;
    out_table_rec.iot_type := l_all_tables_rec.iot_type;
    out_table_rec.partitioned := l_all_tables_rec.partitioned;
    out_table_rec.temporary := l_all_tables_rec.temporary;
    out_table_rec.duration := l_all_tables_rec.duration;
    out_table_rec.secondary := l_all_tables_rec.secondary;
    out_table_rec.nested := l_all_tables_rec.nested;
    out_table_rec.object_id_type := l_all_tables_rec.object_id_type;
    out_table_rec.table_type_owner := l_all_tables_rec.table_type_owner;
    out_table_rec.table_type := l_all_tables_rec.table_type;
    out_table_rec.row_movement := l_all_tables_rec.row_movement;
    out_table_rec.dependencies := l_all_tables_rec.dependencies;
    out_table_rec.physical_attr_rec.pct_free := NVL(l_all_tables_rec.pct_free,10);
    out_table_rec.physical_attr_rec.pct_used := NVL(l_all_tables_rec.pct_used,10);
    out_table_rec.physical_attr_rec.ini_trans := NVL(l_all_tables_rec.ini_trans,1);
    out_table_rec.physical_attr_rec.max_trans := NVL(l_all_tables_rec.max_trans,255);
    out_table_rec.physical_attr_rec.storage.initial_extent := l_all_tables_rec.initial_extent;
--    out_table_rec.physical_attr_rec.storage.next_extent := l_all_tables_rec.next_extent;
    out_table_rec.physical_attr_rec.storage.min_extents := l_all_tables_rec.min_extents;
--    out_table_rec.physical_attr_rec.storage.max_extents := l_all_tables_rec.max_extents;
    out_table_rec.physical_attr_rec.storage.pct_increase := l_all_tables_rec.pct_increase;
    out_table_rec.physical_attr_rec.storage.freelists := l_all_tables_rec.freelists;
    out_table_rec.physical_attr_rec.storage.freelist_groups := l_all_tables_rec.freelist_groups;
    out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_all_tables_rec.buffer_pool,'DEFAULT');
    out_table_rec.compression_rec.compression := l_all_tables_rec.compression;
    $IF dbms_db_version.version >= 11 $THEN
    out_table_rec.compression_rec.compress_for := l_all_tables_rec.compress_for;
      $IF (dbms_db_version.version = 11 AND dbms_db_version.release >=2) or (dbms_db_version.version > 11) $THEN
      BEGIN
        SELECT read_only, result_cache
          INTO out_table_rec.read_only, out_table_rec.result_cache
          FROM all_tables
         WHERE owner = in_owner
           AND table_name = in_table_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;
      END;
      $END
    $END
    out_table_rec.parallel_rec.degree := l_all_tables_rec.degree;
    out_table_rec.parallel_rec.instances := l_all_tables_rec.instances;
    out_table_rec.logging := l_all_tables_rec.logging;
    out_table_rec.cache := l_all_tables_rec.cache;
    out_table_rec.monitoring := l_all_tables_rec.monitoring;

    out_table_rec.rename_rec := get_rename_rec(
                                  in_object_name  => out_table_rec.table_name,
                                  in_object_owner => out_table_rec.owner,
                                  in_object_type  => 'TABLE'
                                );

    -- Read IOT attributes for IOT tables
    IF out_table_rec.iot_type IS NOT NULL THEN
      BEGIN
        SELECT *
          INTO l_indexes_rec
          FROM all_indexes
         WHERE table_owner = in_owner
           AND table_name = in_table_name
           AND index_type = 'IOT - TOP';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;
      out_table_rec.iot_index_name := l_indexes_rec.index_name;
      out_table_rec.iot_index_owner := l_indexes_rec.owner;
      out_table_rec.iot_pct_threshold := l_indexes_rec.pct_threshold;
      out_table_rec.iot_include_column := l_indexes_rec.include_column;
      out_table_rec.iot_key_compression := l_indexes_rec.compression;
      out_table_rec.iot_prefix_length := case when out_table_rec.iot_key_compression = 'ENABLED' and l_indexes_rec.prefix_length > 0 then l_indexes_rec.prefix_length end;
      out_table_rec.physical_attr_rec.pct_free := l_indexes_rec.pct_free;
      out_table_rec.physical_attr_rec.ini_trans := l_indexes_rec.ini_trans;
      out_table_rec.physical_attr_rec.max_trans := l_indexes_rec.max_trans;
      out_table_rec.physical_attr_rec.storage.initial_extent := l_indexes_rec.initial_extent;
--      out_table_rec.physical_attr_rec.storage.next_extent := l_indexes_rec.next_extent;
      out_table_rec.physical_attr_rec.storage.min_extents := l_indexes_rec.min_extents;
--      out_table_rec.physical_attr_rec.storage.max_extents := l_indexes_rec.max_extents;
      out_table_rec.physical_attr_rec.storage.pct_increase := l_indexes_rec.pct_increase;
      out_table_rec.physical_attr_rec.storage.freelists := l_indexes_rec.freelists;
      out_table_rec.physical_attr_rec.storage.freelist_groups := l_indexes_rec.freelist_groups;
      out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_indexes_rec.buffer_pool,'DEFAULT');
      out_table_rec.tablespace_name := l_indexes_rec.tablespace_name;
      out_table_rec.logging := l_indexes_rec.logging;

      SELECT column_name, descend
        BULK COLLECT
        INTO out_table_rec.iot_pk_column_arr, out_table_rec.iot_pk_column_sort_type_arr
        FROM all_ind_columns
       WHERE table_owner = in_owner
         AND table_name = in_table_name
         AND index_name = out_table_rec.iot_index_name
         AND index_owner = out_table_rec.iot_index_owner
       ORDER BY column_position;

      BEGIN
        SELECT *
          INTO l_all_tables_rec
          FROM all_all_tables
         WHERE owner = in_owner
           AND iot_name = in_table_name
           AND iot_type = 'IOT_OVERFLOW';

        out_table_rec.overflow_table_name := l_all_tables_rec.table_name;
        out_table_rec.overflow_physical_attr_rec.pct_free := l_all_tables_rec.pct_free;
        out_table_rec.overflow_physical_attr_rec.pct_used := l_all_tables_rec.pct_used;
        out_table_rec.overflow_physical_attr_rec.ini_trans := l_all_tables_rec.ini_trans;
        out_table_rec.overflow_physical_attr_rec.max_trans := l_all_tables_rec.max_trans;
        out_table_rec.overflow_physical_attr_rec.storage.initial_extent := l_all_tables_rec.initial_extent;
--        out_table_rec.overflow_physical_attr_rec.storage.next_extent := l_all_tables_rec.next_extent;
        out_table_rec.overflow_physical_attr_rec.storage.min_extents := l_all_tables_rec.min_extents;
--        out_table_rec.overflow_physical_attr_rec.storage.max_extents := l_all_tables_rec.max_extents;
        out_table_rec.overflow_physical_attr_rec.storage.pct_increase := l_all_tables_rec.pct_increase;
        out_table_rec.overflow_physical_attr_rec.storage.freelists := l_all_tables_rec.freelists;
        out_table_rec.overflow_physical_attr_rec.storage.freelist_groups := l_all_tables_rec.freelist_groups;
        out_table_rec.overflow_physical_attr_rec.storage.buffer_pool := NULLIF(l_all_tables_rec.buffer_pool,'DEFAULT');
        out_table_rec.overflow_tablespace := l_indexes_rec.tablespace_name;
        out_table_rec.overflow_logging := l_indexes_rec.logging;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;
      END;

      SELECT DECODE(COUNT(*), 0, 'N', 'Y')
        INTO out_table_rec.mapping_table
        FROM all_all_tables
       WHERE owner = in_owner
         AND iot_name = in_table_name
         AND iot_type = 'IOT_MAPPING';
    END IF;

    -- Read attribures for partitioned tables (non IOT)
    IF out_table_rec.partitioned = 'YES' AND out_table_rec.iot_type IS NULL THEN
      BEGIN
        SELECT *
          INTO l_part_tables_rec
          FROM all_part_tables
         WHERE owner = in_owner
           AND table_name = in_table_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;

      out_table_rec.tablespace_name := l_part_tables_rec.def_tablespace_name;
      out_table_rec.partitioning_type := l_part_tables_rec.partitioning_type;
      out_table_rec.subpartitioning_type := l_part_tables_rec.subpartitioning_type;
      $IF dbms_db_version.version >= 11 $THEN
      out_table_rec.ref_ptn_constraint_name := l_part_tables_rec.ref_ptn_constraint_name;
      out_table_rec.interval := l_part_tables_rec.interval;
      out_table_rec.compression_rec.compress_for := l_part_tables_rec.def_compress_for;
      $END
      out_table_rec.physical_attr_rec.pct_free := l_part_tables_rec.def_pct_free;
      out_table_rec.physical_attr_rec.pct_used := l_part_tables_rec.def_pct_used;
      out_table_rec.physical_attr_rec.ini_trans := l_part_tables_rec.def_ini_trans;
      out_table_rec.physical_attr_rec.max_trans := l_part_tables_rec.def_max_trans;
      out_table_rec.physical_attr_rec.storage.initial_extent := NULLIF(l_part_tables_rec.def_initial_extent,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.next_extent := NULLIF(l_part_tables_rec.def_next_extent,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.min_extents := NULLIF(l_part_tables_rec.def_min_extents,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.max_extents := NULLIF(l_part_tables_rec.def_max_extents,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.pct_increase := NULLIF(l_part_tables_rec.def_pct_increase,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.freelists := l_part_tables_rec.def_freelists;
      out_table_rec.physical_attr_rec.storage.freelist_groups := l_part_tables_rec.def_freelist_groups;
      out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_part_tables_rec.def_buffer_pool,'DEFAULT');
      out_table_rec.compression_rec.compression := l_part_tables_rec.def_compression;
      IF l_part_tables_rec.def_logging <> 'NONE' THEN
        out_table_rec.logging := l_part_tables_rec.def_logging;
      END IF;

      SELECT column_name
        BULK COLLECT
        INTO out_table_rec.part_key_column_arr
        FROM all_part_key_columns
       WHERE owner = in_owner
         AND name = in_table_name
         AND object_type = 'TABLE'
       ORDER BY column_position;

      IF out_table_rec.subpartitioning_type <> 'NONE' THEN
        SELECT column_name
          BULK COLLECT
          INTO out_table_rec.subpart_key_column_arr
          FROM all_subpart_key_columns
         WHERE owner = in_owner
           AND name = in_table_name
           AND object_type = 'TABLE'
         ORDER BY column_position;

        -- read subpartition template
        read_subpartition_template(out_table_rec);
      END IF;

    END IF;

    -- Read attribures for partitioned tables (IOT)
    IF out_table_rec.partitioned = 'YES' AND out_table_rec.iot_type IS NOT NULL THEN
      BEGIN
        SELECT *
          INTO l_part_indexes_rec
          FROM all_part_indexes
         WHERE owner = in_owner
           AND table_name = in_table_name
           AND index_name = out_table_rec.iot_index_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;

      out_table_rec.tablespace_name := l_part_indexes_rec.def_tablespace_name;
      out_table_rec.partitioning_type := l_part_indexes_rec.partitioning_type;
      out_table_rec.subpartitioning_type := l_part_indexes_rec.subpartitioning_type;
      $IF dbms_db_version.version >= 11 $THEN
      out_table_rec.interval := l_part_indexes_rec.interval;
      $END
      out_table_rec.physical_attr_rec.pct_free := l_part_indexes_rec.def_pct_free;
      out_table_rec.physical_attr_rec.ini_trans := l_part_indexes_rec.def_ini_trans;
      out_table_rec.physical_attr_rec.max_trans := l_part_indexes_rec.def_max_trans;
      out_table_rec.physical_attr_rec.storage.initial_extent := NULLIF(l_part_indexes_rec.def_initial_extent,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.next_extent := NULLIF(l_part_indexes_rec.def_next_extent,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.min_extents := NULLIF(l_part_indexes_rec.def_min_extents,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.max_extents := NULLIF(l_part_indexes_rec.def_max_extents,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.pct_increase := NULLIF(l_part_indexes_rec.def_pct_increase,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.freelists := l_part_indexes_rec.def_freelists;
      out_table_rec.physical_attr_rec.storage.freelist_groups := l_part_indexes_rec.def_freelist_groups;
      out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_part_indexes_rec.def_buffer_pool,'DEFAULT');

      SELECT column_name
        BULK COLLECT
        INTO out_table_rec.part_key_column_arr
        FROM all_part_key_columns
       WHERE owner = out_table_rec.iot_index_owner
         AND name = out_table_rec.iot_index_name
         AND object_type = 'INDEX'
       ORDER BY column_position;

      IF out_table_rec.subpartitioning_type <> 'NONE' THEN
        SELECT column_name
          BULK COLLECT
          INTO out_table_rec.subpart_key_column_arr
          FROM all_subpart_key_columns
         WHERE owner = out_table_rec.iot_index_owner
           AND name = out_table_rec.iot_index_name
           AND object_type = 'INDEX'
         ORDER BY column_position;

        -- read subpartition template
        read_subpartition_template(out_table_rec);
      END IF;
    END IF;

    IF out_table_rec.cluster_name IS NOT NULL THEN
      IF out_table_rec.owner = SYS_CONTEXT('USERENV','CURRENT_USER') THEN
        SELECT tab_column_name
          BULK COLLECT
          INTO out_table_rec.cluster_column_arr
          FROM user_clu_columns
         WHERE cluster_name = out_table_rec.cluster_name
           AND table_name = out_table_rec.table_name;
      ELSE
        BEGIN
          EXECUTE IMMEDIATE
          'SELECT tab_column_name
            FROM dba_clu_columns
           WHERE cluster_name = :in_cluster
             AND table_name = :in_table_name'
            BULK COLLECT
            INTO out_table_rec.cluster_column_arr
           USING out_table_rec.cluster_name, out_table_rec.table_name;
        EXCEPTION
          WHEN ge_table_not_exist THEN
            raise_error('You need grant select on dba_clu_columns to perform this change');
        END;
      END IF;
    END IF;

    SELECT tablespace_name, block_size
      BULK COLLECT
      INTO l_tablespace_name_arr, l_block_size_arr
      FROM user_tablespaces;

    FOR i IN 1..l_tablespace_name_arr.COUNT LOOP
      out_table_rec.tablespace_block_size_indx(l_tablespace_name_arr(i)) := l_block_size_arr(i);
    END LOOP;

    BEGIN
      SELECT comments
        INTO out_table_rec.tab_comment
        FROM all_tab_comments
       WHERE owner = out_table_rec.owner
         AND table_name = out_table_rec.table_name
         AND table_type = 'TABLE';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;

    IF in_read_data THEN
      l_sql := 'SELECT COUNT(*) FROM "'||in_owner||'"."'||in_table_name||'" WHERE ROWNUM = 1';
      cort_log_pkg.execute(
        in_text      => l_sql
      );
      BEGIN
        EXECUTE IMMEDIATE l_sql INTO l_cnt;
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.update_exec_time;
          cort_log_pkg.error('Error in parsing/executing sql (select count)',l_sql);
      END;
      cort_log_pkg.update_exec_time;
    END IF;
    out_table_rec.is_table_empty := l_cnt = 0;
    stop_timer;
  END read_table;

  -- read table attributes from all_tables/all_part_tables
  PROCEDURE read_table_columns(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_tab_cols_arr IS TABLE OF all_tab_cols%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_all_enc_cols_arr IS TABLE OF all_encrypted_columns%ROWTYPE INDEX BY PLS_INTEGER;
    l_tab_cols_arr             t_all_tab_cols_arr;
    l_enc_cols_arr             t_all_enc_cols_arr;
    l_indx                     PLS_INTEGER;
    l_part_key_col_indx_arr    arrays.gt_int_indx; -- index by column name of partition key columns
    l_subpart_key_col_indx_arr arrays.gt_int_indx; -- index by column name of subpartition key columns
    l_iot_pk_column_indx_arr   arrays.gt_int_indx; -- index by column name of IOT primary key columns
    l_cluster_column_indx_arr  arrays.gt_int_indx; -- index by column name of cluster key columns
    l_unused_col_cnt           PLS_INTEGER;
    l_invisible_col_cnt        PLS_INTEGER;
  BEGIN
    start_timer;
    SELECT *
      BULK COLLECT
      INTO l_tab_cols_arr
      FROM all_tab_cols
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name
     ORDER BY internal_column_id;

    FOR i IN 1..io_table_rec.part_key_column_arr.COUNT LOOP
      l_part_key_col_indx_arr(io_table_rec.part_key_column_arr(i)) := i;
    END LOOP;

    FOR i IN 1..io_table_rec.subpart_key_column_arr.COUNT LOOP
      l_subpart_key_col_indx_arr(io_table_rec.subpart_key_column_arr(i)) := i;
    END LOOP;

    FOR i IN 1..io_table_rec.iot_pk_column_arr.COUNT LOOP
      l_iot_pk_column_indx_arr(io_table_rec.iot_pk_column_arr(i)) := i;
    END LOOP;

    FOR i IN 1..io_table_rec.cluster_column_arr.COUNT LOOP
      l_cluster_column_indx_arr(io_table_rec.cluster_column_arr(i)) := i;
    END LOOP;


    l_unused_col_cnt := 0;
    l_invisible_col_cnt := 0;
    FOR i IN 1..l_tab_cols_arr.COUNT LOOP
      io_table_rec.column_indx_arr(l_tab_cols_arr(i).column_name) := i;
      io_table_rec.column_qualified_indx_arr(l_tab_cols_arr(i).qualified_col_name) := i;
      io_table_rec.column_arr(i).column_name := l_tab_cols_arr(i).column_name;
      io_table_rec.column_arr(i).column_indx := i;
      io_table_rec.column_arr(i).data_type := l_tab_cols_arr(i).data_type;
      io_table_rec.column_arr(i).data_type_mod := l_tab_cols_arr(i).data_type_mod;
      io_table_rec.column_arr(i).data_type_owner := l_tab_cols_arr(i).data_type_owner;
      io_table_rec.column_arr(i).data_length := l_tab_cols_arr(i).data_length;
      io_table_rec.column_arr(i).data_precision := l_tab_cols_arr(i).data_precision;
      io_table_rec.column_arr(i).data_scale := l_tab_cols_arr(i).data_scale;
      io_table_rec.column_arr(i).nullable := l_tab_cols_arr(i).nullable;
      io_table_rec.column_arr(i).column_id := l_tab_cols_arr(i).column_id;
      io_table_rec.column_arr(i).data_default := REGEXP_REPLACE(l_tab_cols_arr(i).data_default, '\s$', '');
      io_table_rec.column_arr(i).character_set_name := l_tab_cols_arr(i).character_set_name;
      io_table_rec.column_arr(i).char_col_decl_length := l_tab_cols_arr(i).char_col_decl_length;
      io_table_rec.column_arr(i).char_length := l_tab_cols_arr(i).char_length;
      io_table_rec.column_arr(i).char_used := l_tab_cols_arr(i).char_used;
      io_table_rec.column_arr(i).internal_column_id := l_tab_cols_arr(i).internal_column_id;
      io_table_rec.column_arr(i).hidden_column := l_tab_cols_arr(i).hidden_column;
      io_table_rec.column_arr(i).qualified_col_name := l_tab_cols_arr(i).qualified_col_name;
      $IF dbms_db_version.version >= 11 $THEN
      io_table_rec.column_arr(i).virtual_column := l_tab_cols_arr(i).virtual_column;
      $END
      $IF dbms_db_version.version >= 12 $THEN
      io_table_rec.column_arr(i).user_generated := l_tab_cols_arr(i).user_generated;
      io_table_rec.column_arr(i).default_on_null := l_tab_cols_arr(i).default_on_null;
      io_table_rec.column_arr(i).identity_column := l_tab_cols_arr(i).identity_column;
      io_table_rec.column_arr(i).evaluation_edition := l_tab_cols_arr(i).evaluation_edition;
      io_table_rec.column_arr(i).unusable_before := l_tab_cols_arr(i).unusable_before;
      io_table_rec.column_arr(i).unusable_beginning := l_tab_cols_arr(i).unusable_beginning;
      $END
      $IF dbms_db_version.version >= 18 $THEN
      io_table_rec.column_arr(i).collation := l_tab_cols_arr(i).collation;
      io_table_rec.column_arr(i).collated_column_id := l_tab_cols_arr(i).collated_column_id;
      $END
      io_table_rec.column_arr(i).segment_column_id := l_tab_cols_arr(i).segment_column_id -
                                                      CASE WHEN g_params.compare.value_exists('IGNORE_UNUSED') THEN l_unused_col_cnt ELSE 0 END -
                                                      CASE WHEN g_params.compare.value_exists('IGNORE_INVISIBLE') THEN l_invisible_col_cnt ELSE 0 END;
      io_table_rec.column_arr(i).unused_segment_shift := l_unused_col_cnt;
      io_table_rec.column_arr(i).invisible_segment_shift := l_invisible_col_cnt;
      -- Special case for unused columns: increase counter and deduct from segment_id for next columns
      io_table_rec.column_arr(i).unused_flag := io_table_rec.column_arr(i).column_name = io_table_rec.column_arr(i).qualified_col_name AND
                                                io_table_rec.column_arr(i).hidden_column = 'YES' AND
                                                io_table_rec.column_arr(i).segment_column_id IS NOT NULL AND
                                                $IF dbms_db_version.version >= 12 $THEN
                                                io_table_rec.column_arr(i).user_generated = 'NO' AND
                                                $END
                                                regexp_like(io_table_rec.column_arr(i).column_name, '^SYS_C[0-9]+');
      IF io_table_rec.column_arr(i).unused_flag THEN
        -- unused column found
        -- We don't need to do anything with column itlesf but need to increase counter which is deducted
        -- from column_segment_id of columns defined below.
        l_unused_col_cnt := l_unused_col_cnt + 1;
      END IF;
      $IF dbms_db_version.version >= 12 $THEN
      IF io_table_rec.column_arr(i).hidden_column = 'YES' AND
         io_table_rec.column_arr(i).user_generated = 'YES'
        -- user-hidden column found
      THEN
        l_invisible_col_cnt := l_invisible_col_cnt + 1;
      END IF;
      $END

      -- XMLTYPE columns marked as virtual columns based on hidden lob column or scalar column(s).
      -- But XMLTYPE behaves as regular column
      IF io_table_rec.column_arr(i).data_type_owner IS NOT NULL AND
         io_table_rec.column_arr(i).data_type = 'XMLTYPE' AND
         io_table_rec.column_arr(i).virtual_column = 'YES' AND
         i < l_tab_cols_arr.COUNT
      THEN
        io_table_rec.column_arr(i).virtual_column := 'NO';
        io_table_rec.column_arr(i).segment_column_id := l_tab_cols_arr(i+1).segment_column_id -
                                                        CASE WHEN g_params.compare.value_exists('IGNORE_UNUSED') THEN l_unused_col_cnt ELSE 0 END -
                                                        CASE WHEN g_params.compare.value_exists('IGNORE_INVISIBLE') THEN l_invisible_col_cnt ELSE 0 END;
      END IF;

      io_table_rec.column_arr(i).partition_key := l_part_key_col_indx_arr.EXISTS(io_table_rec.column_arr(i).column_name) OR
                                                  l_subpart_key_col_indx_arr.EXISTS(io_table_rec.column_arr(i).column_name);
      io_table_rec.column_arr(i).iot_primary_key := l_iot_pk_column_indx_arr.EXISTS(io_table_rec.column_arr(i).column_name);
      io_table_rec.column_arr(i).cluster_key := l_cluster_column_indx_arr.EXISTS(io_table_rec.column_arr(i).column_name);

      IF io_table_rec.column_arr(i).data_type_owner IS NULL THEN
        CASE
        WHEN io_table_rec.column_arr(i).data_type LIKE 'TIMESTAMP(_)' THEN
          io_table_rec.column_arr(i).data_type := 'TIMESTAMP';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'TIMESTAMP(_) WITH TIME ZONE' THEN
          io_table_rec.column_arr(i).data_type := 'TIMESTAMP WITH TIME ZONE';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'TIMESTAMP(_) WITH LOCAL TIME ZONE' THEN
          io_table_rec.column_arr(i).data_type := 'TIMESTAMP WITH LOCAL TIME ZONE';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'INTERVAL YEAR()) TO MONTH' THEN
          io_table_rec.column_arr(i).data_type := 'INTERVAL YEAR TO MONTH';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'INTERVAL DAY(_) TO SECOND(_)' THEN
          io_table_rec.column_arr(i).data_type := 'INTERVAL DAY TO SECOND';
        ELSE
          NULL;
        END CASE;
      END IF;
      io_table_rec.column_arr(i).temp_column_name := cort_params_pkg.gc_temp_prefix||'COLUMN_'||TO_CHAR(i,'fm0XXX');
    END LOOP;

    $IF dbms_db_version.ver_le_10_2 OR dbms_db_version.version >= 11 $THEN
    SELECT *
      BULK COLLECT
      INTO l_enc_cols_arr
      FROM all_encrypted_columns
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_enc_cols_arr.COUNT LOOP
      IF io_table_rec.column_indx_arr.EXISTS(l_enc_cols_arr(i).column_name) THEN
        l_indx := io_table_rec.column_indx_arr(l_enc_cols_arr(i).column_name);
        io_table_rec.column_arr(l_indx).encryption_alg := l_enc_cols_arr(i).encryption_alg;
        io_table_rec.column_arr(l_indx).salt := l_enc_cols_arr(i).salt;
      END IF;
    END LOOP;
    $END
    stop_timer;
  END read_table_columns;

  -- read index columns/expressions
  PROCEDURE read_index_cols(
    io_index_rec   IN OUT NOCOPY gt_index_rec
  )
  AS
    l_column_arr arrays.gt_lstr_arr;
  BEGIN
    -- Read index columns
    IF io_index_rec.index_type = 'BITMAP' AND io_index_rec.join_index = 'YES' THEN
      SELECT table_owner, table_name, column_name
        BULK COLLECT
        INTO io_index_rec.column_table_owner_arr, io_index_rec.column_table_arr, io_index_rec.column_arr
        FROM all_ind_columns
       WHERE index_owner = io_index_rec.owner
         AND index_name = io_index_rec.index_name
       ORDER BY column_position;

      SELECT inner_table_owner, inner_table_name, inner_table_column,
             outer_table_owner, outer_table_name, outer_table_column
        BULK COLLECT
        INTO io_index_rec.join_inner_owner_arr,
             io_index_rec.join_inner_table_arr,
             io_index_rec.join_inner_column_arr,
             io_index_rec.join_outer_owner_arr,
             io_index_rec.join_outer_table_arr,
             io_index_rec.join_outer_column_arr
        FROM all_join_ind_columns
       WHERE index_owner = io_index_rec.owner
         AND index_name = io_index_rec.index_name;
    ELSE
      IF io_index_rec.table_object_type IS NOT NULL THEN
        SELECT c.column_name, c.descend, ex.column_expression
          BULK COLLECT
          INTO l_column_arr, io_index_rec.sort_order_arr, io_index_rec.column_expr_arr
          FROM all_ind_columns c
          LEFT JOIN all_ind_expressions ex
            ON ex.index_owner = c.index_owner
           AND ex.index_name = c.index_name
           AND ex.column_position = c.column_position
         WHERE c.index_owner = io_index_rec.owner
           AND c.index_name = io_index_rec.index_name
         ORDER BY c.column_position;
        -- Store long names for indexes on object tables in expressions
        FOR i IN 1..l_column_arr.COUNT LOOP
          IF INSTR(l_column_arr(i),'"') > 0 THEN
            io_index_rec.column_expr_arr(i) := l_column_arr(i);
          ELSE
            io_index_rec.column_arr(i) := l_column_arr(i);
          END IF;
        END LOOP;
      ELSE
        SELECT c.column_name, c.descend, ex.column_expression
          BULK COLLECT
          INTO io_index_rec.column_arr, io_index_rec.sort_order_arr, io_index_rec.column_expr_arr
          FROM all_ind_columns c
          LEFT JOIN all_ind_expressions ex
            ON ex.index_owner = c.index_owner
           AND ex.index_name = c.index_name
           AND ex.column_position = c.column_position
         WHERE c.index_owner = io_index_rec.owner
           AND c.index_name = io_index_rec.index_name
         ORDER BY c.column_position;
      END IF;
    END IF;
  END read_index_cols;


  -- read attributes for partitioned index
  PROCEDURE read_part_index(
    io_index_rec   IN OUT NOCOPY gt_index_rec
  )
  AS
    l_part_indexes_rec         gt_all_part_indexes_rec;
  BEGIN
    -- Read attribures for partitioned indexes
    IF io_index_rec.partitioned = 'YES' THEN
      BEGIN
        SELECT *
          INTO l_part_indexes_rec
          FROM all_part_indexes
         WHERE owner = io_index_rec.owner
           AND index_name = io_index_rec.index_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;

      io_index_rec.tablespace_name   := l_part_indexes_rec.def_tablespace_name;
      io_index_rec.physical_attr_rec.pct_free  := l_part_indexes_rec.def_pct_free;
      io_index_rec.physical_attr_rec.ini_trans := l_part_indexes_rec.def_ini_trans;
      io_index_rec.physical_attr_rec.max_trans := l_part_indexes_rec.def_max_trans;
      io_index_rec.physical_attr_rec.storage.initial_extent := NULLIF(l_part_indexes_rec.def_initial_extent,'DEFAULT');
  --    io_index_rec.physical_attr_rec.storage.next_extent := l_part_indexes_rec.def_next_extent;
      io_index_rec.physical_attr_rec.storage.min_extents := NULLIF(l_part_indexes_rec.def_min_extents,'DEFAULT');
  --    io_index_rec.physical_attr_rec.storage.max_extents := l_part_indexes_rec.def_max_extents;
      io_index_rec.physical_attr_rec.storage.pct_increase := NULLIF(l_part_indexes_rec.def_pct_increase,'DEFAULT');
      io_index_rec.physical_attr_rec.storage.freelists := l_part_indexes_rec.def_freelists;
      io_index_rec.physical_attr_rec.storage.freelist_groups := l_part_indexes_rec.def_freelist_groups;
      io_index_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_part_indexes_rec.def_buffer_pool,'DEFAULT');
      IF l_part_indexes_rec.def_logging <> 'NONE' THEN
        io_index_rec.logging              := l_part_indexes_rec.def_logging;
      END IF;
      io_index_rec.partitioning_type      := l_part_indexes_rec.partitioning_type;
      io_index_rec.subpartitioning_type   := l_part_indexes_rec.subpartitioning_type;
      io_index_rec.locality               := l_part_indexes_rec.locality;
      io_index_rec.alignment              := l_part_indexes_rec.alignment;
      $IF dbms_db_version.version >= 11 $THEN
      io_index_rec.interval               := l_part_indexes_rec.interval;
      $END

      SELECT column_name
        BULK COLLECT
        INTO io_index_rec.part_key_column_arr
        FROM all_part_key_columns
       WHERE owner = io_index_rec.owner
         AND name = io_index_rec.index_name
         AND object_type = 'INDEX'
       ORDER BY column_position;

      IF io_index_rec.subpartitioning_type <> 'NONE' THEN
        SELECT column_name
          BULK COLLECT
          INTO io_index_rec.subpart_key_column_arr
          FROM all_subpart_key_columns
         WHERE owner = io_index_rec.owner
           AND name = io_index_rec.index_name
           AND object_type = 'INDEX'
         ORDER BY column_position;
      END IF;
    END IF;
  END read_part_index;

  PROCEDURE assign_index_rec(
    in_all_indexes_rec   IN gt_all_indexes_rec,
    out_index_rec        OUT NOCOPY gt_index_rec
  )
  AS
  BEGIN
    out_index_rec.owner             := in_all_indexes_rec.owner;
    out_index_rec.index_name        := in_all_indexes_rec.index_name;
    out_index_rec.index_type        := in_all_indexes_rec.index_type;
    out_index_rec.table_owner       := in_all_indexes_rec.table_owner;
    out_index_rec.table_name        := in_all_indexes_rec.table_name;
    out_index_rec.table_type        := in_all_indexes_rec.table_type;
    out_index_rec.uniqueness        := in_all_indexes_rec.uniqueness;
    out_index_rec.compression       := in_all_indexes_rec.compression;
    out_index_rec.prefix_length     := case when out_index_rec.compression in ('PREFIX', 'ENABLED') and in_all_indexes_rec.prefix_length > 0 then in_all_indexes_rec.prefix_length end;
    out_index_rec.tablespace_name   := in_all_indexes_rec.tablespace_name;
    out_index_rec.physical_attr_rec.pct_free  := in_all_indexes_rec.pct_free;
    out_index_rec.physical_attr_rec.ini_trans := in_all_indexes_rec.ini_trans;
    out_index_rec.physical_attr_rec.max_trans := in_all_indexes_rec.max_trans;
    out_index_rec.physical_attr_rec.storage.initial_extent := in_all_indexes_rec.initial_extent;
--    out_index_rec.physical_attr_rec.storage.next_extent := in_all_indexes_rec.next_extent;
    out_index_rec.physical_attr_rec.storage.min_extents := in_all_indexes_rec.min_extents;
--    out_index_rec.physical_attr_rec.storage.max_extents := in_all_indexes_rec.max_extents;
    out_index_rec.physical_attr_rec.storage.pct_increase := in_all_indexes_rec.pct_increase;
    out_index_rec.physical_attr_rec.storage.freelists := in_all_indexes_rec.freelists;
    out_index_rec.physical_attr_rec.storage.freelist_groups := in_all_indexes_rec.freelist_groups;
    out_index_rec.physical_attr_rec.storage.buffer_pool := NULLIF(in_all_indexes_rec.buffer_pool,'DEFAULT');
    out_index_rec.pct_threshold          := in_all_indexes_rec.pct_threshold;
    out_index_rec.include_column         := in_all_indexes_rec.include_column;
    out_index_rec.parallel_rec.degree    := in_all_indexes_rec.degree;
    out_index_rec.parallel_rec.instances := in_all_indexes_rec.instances;
    out_index_rec.logging                := in_all_indexes_rec.logging;
    out_index_rec.partitioned            := in_all_indexes_rec.partitioned;
    out_index_rec.temporary              := in_all_indexes_rec.temporary;
    out_index_rec.duration               := in_all_indexes_rec.duration;
    out_index_rec.generated              := in_all_indexes_rec.generated;
    out_index_rec.secondary              := in_all_indexes_rec.secondary;
    out_index_rec.pct_direct_access      := in_all_indexes_rec.pct_direct_access;
    out_index_rec.ityp_owner             := in_all_indexes_rec.ityp_owner;
    out_index_rec.ityp_name              := in_all_indexes_rec.ityp_name;
    out_index_rec.parameters             := in_all_indexes_rec.parameters;
    out_index_rec.domidx_status          := in_all_indexes_rec.domidx_status;
    out_index_rec.domidx_opstatus        := in_all_indexes_rec.domidx_opstatus;
    out_index_rec.funcidx_status         := in_all_indexes_rec.funcidx_status;
    out_index_rec.join_index             := in_all_indexes_rec.join_index;
    $IF dbms_db_version.version >= 11 $THEN
    out_index_rec.visibility             := in_all_indexes_rec.visibility;
    $END
    $IF dbms_db_version.version > 12 OR (dbms_db_version.version = 12 AND dbms_db_version.release >= 2) $THEN
    out_index_rec.orphaned_entries       := in_all_indexes_rec.orphaned_entries;
    out_index_rec.indexing               := in_all_indexes_rec.indexing;
    $END
    out_index_rec.recreate_flag := FALSE;
    out_index_rec.drop_flag     := FALSE;

    out_index_rec.rename_rec := get_rename_rec(
                                  in_object_name  => out_index_rec.index_name,
                                  in_object_owner => out_index_rec.owner,
                                  in_object_type  => 'INDEX'
                                );

    read_index_cols(io_index_rec => out_index_rec);

    read_part_index(io_index_rec => out_index_rec);

  END assign_index_rec;

  --  read individual index metadata
  PROCEDURE read_index(
    in_index_name  IN VARCHAR2,
    in_owner       IN VARCHAR2,
    out_index_rec  OUT NOCOPY gt_index_rec
  )
  AS
    l_all_indexes_rec   gt_all_indexes_rec;
  BEGIN
    BEGIN
      SELECT *
        INTO l_all_indexes_rec
        FROM all_indexes i
       WHERE owner = in_owner
         AND index_name = in_index_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;

    assign_index_rec(
      in_all_indexes_rec => l_all_indexes_rec,
      out_index_rec      => out_index_rec
    );

    SELECT table_type_owner, table_type
      INTO out_index_rec.table_object_owner, out_index_rec.table_object_type
      FROM all_all_tables
     WHERE owner = out_index_rec.table_owner
       AND table_name = out_index_rec.table_name;

  END read_index;

  -- read all indexes metadata on to given table
  PROCEDURE read_table_indexes(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_index_rec                gt_index_rec;
    l_all_indexes_arr          gt_all_indexes_arr;
    l_index_full_name          VARCHAR2(65);
  BEGIN
    start_timer;
    SELECT *
      BULK COLLECT
      INTO l_all_indexes_arr
      FROM all_indexes i
     WHERE table_owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_indexes_arr.COUNT LOOP
      l_index_full_name := '"'||l_all_indexes_arr(i).owner||'"."'||l_all_indexes_arr(i).index_name||'"';
      io_table_rec.index_indx_arr(l_index_full_name) := i;
      l_index_rec := NULL;

      assign_index_rec(
        in_all_indexes_rec => l_all_indexes_arr(i),
        out_index_rec      => l_index_rec
      );

      l_index_rec.table_object_owner := io_table_rec.table_type_owner;
      l_index_rec.table_object_type := io_table_rec.table_type;

      io_table_rec.index_arr(i) := l_index_rec;
    END LOOP;
    stop_timer;
  END read_table_indexes;

  PROCEDURE read_table_join_indexes(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_index_rec                gt_index_rec;
    l_index_full_name          VARCHAR2(65);
    l_indx                     pls_integer;
  BEGIN
    start_timer;

    l_indx := 1;
    FOR x IN (SELECT *
                FROM all_join_ind_columns ic
               WHERE (ic.inner_table_owner = io_table_rec.owner
                 AND ic.inner_table_name = io_table_rec.table_name)
                  OR (ic.outer_table_owner = io_table_rec.owner
                 AND ic.outer_table_name = io_table_rec.table_name)
             )
    LOOP
      l_index_full_name := '"'||x.index_owner||'"."'||x.index_name||'"';
      io_table_rec.join_index_indx_arr(l_index_full_name) := l_indx;
      l_index_rec := NULL;

      read_index(
        in_index_name  => x.index_name,
        in_owner       => x.index_owner,
        out_index_rec  => l_index_rec
      );

      l_index_rec.table_object_owner := io_table_rec.table_type_owner;
      l_index_rec.table_object_type := io_table_rec.table_type;

      io_table_rec.join_index_arr(l_indx) := l_index_rec;
      l_indx := l_indx + 1;
    END LOOP;
    stop_timer;
  END read_table_join_indexes;

  PROCEDURE assign_constraint_rec(
    io_constraint_rec      IN OUT NOCOPY gt_constraint_rec,
    in_all_constraints_rec IN gt_all_constraints_rec
  )
  AS
  BEGIN
    io_constraint_rec.owner             := in_all_constraints_rec.owner;
    io_constraint_rec.constraint_name   := in_all_constraints_rec.constraint_name;
    io_constraint_rec.constraint_type   := in_all_constraints_rec.constraint_type;
    io_constraint_rec.table_name        := in_all_constraints_rec.table_name;
    io_constraint_rec.search_condition  := in_all_constraints_rec.search_condition;
    io_constraint_rec.r_owner           := in_all_constraints_rec.r_owner;
    io_constraint_rec.r_constraint_name := in_all_constraints_rec.r_constraint_name;
    io_constraint_rec.delete_rule       := in_all_constraints_rec.delete_rule;
    io_constraint_rec.status            := in_all_constraints_rec.status;
    io_constraint_rec.deferrable        := in_all_constraints_rec.deferrable;
    io_constraint_rec.deferred          := in_all_constraints_rec.deferred;
    io_constraint_rec.validated         := in_all_constraints_rec.validated;
    io_constraint_rec.generated         := in_all_constraints_rec.generated;
    io_constraint_rec.rely              := in_all_constraints_rec.rely;
    io_constraint_rec.index_owner       := in_all_constraints_rec.index_owner;  -- Oracle bug in all_constraints
    io_constraint_rec.index_name        := in_all_constraints_rec.index_name;
    io_constraint_rec.has_references    := FALSE;
    io_constraint_rec.drop_flag         := FALSE;
  END assign_constraint_rec;

  PROCEDURE update_constraint_index_owner(
    in_table_rec           IN gt_table_rec,
    io_constraint_rec      IN OUT NOCOPY gt_constraint_rec
  )
  AS
  BEGIN
    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      IF in_table_rec.index_arr(i).index_name = io_constraint_rec.index_name AND
         cort_comp_pkg.comp_array(in_table_rec.index_arr(i).column_arr, io_constraint_rec.column_arr) = 0
      THEN
        io_constraint_rec.index_owner := in_table_rec.index_arr(i).owner;
        EXIT;
      END IF;
    END LOOP;
  END update_constraint_index_owner;

  -- read constraint details from cort_all_constraints
  PROCEDURE read_table_constraints(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_constraint_rec           gt_constraint_rec;
    l_constraints_arr          gt_all_constraints_arr;
    l_col_cons_name_arr        arrays.gt_name_arr;
    l_col_name_arr             arrays.gt_name_arr;
    l_col_position_arr         arrays.gt_num_arr;
    l_indx                     PLS_INTEGER;
    l_index_full_name          VARCHAR2(65);
  BEGIN
    start_timer;
    SELECT c.*
      BULK COLLECT
      INTO l_constraints_arr
      FROM all_constraints c
     WHERE c.owner = io_table_rec.owner
       AND c.table_name = io_table_rec.table_name
       AND c.constraint_type IN ('P','U','R','C','F')
       AND c.view_related IS NULL;

    FOR i IN 1..l_constraints_arr.COUNT LOOP
      io_table_rec.constraint_indx_arr(l_constraints_arr(i).constraint_name) := i;
      l_constraint_rec := NULL;

      assign_constraint_rec(
        io_constraint_rec      => l_constraint_rec,
        in_all_constraints_rec => l_constraints_arr(i)
      );

      io_table_rec.constraint_arr(i) := l_constraint_rec;

      io_table_rec.constraint_arr(i).rename_rec := get_rename_rec(
                                                     in_table_name_rec => io_table_rec.rename_rec,
                                                     in_object_name    => l_constraint_rec.constraint_name,
                                                     in_object_owner   => l_constraint_rec.owner,
                                                     in_object_type    => 'CONSTRAINT',
                                                     in_index          => i,
                                                     in_generated      => CASE l_constraint_rec.generated WHEN 'USER NAME' THEN 'N' ELSE 'Y' END
                                                   );
/*
      IF io_table_rec.constraint_arr(i).constraint_type = 'C' THEN
        cort_parse_pkg.normalize_search_condition(io_table_rec.constraint_arr(i));
      END IF;
*/
      IF io_table_rec.constraint_arr(i).constraint_type = 'R' THEN
        BEGIN
          SELECT r.table_name
            INTO io_table_rec.constraint_arr(i).r_table_name
            FROM all_constraints r
           WHERE r.owner = io_table_rec.constraint_arr(i).r_owner
             AND r.constraint_name = io_table_rec.constraint_arr(i).r_constraint_name;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            RETURN;
        END;
        -- Read ref table columns
        SELECT column_name
          BULK COLLECT
          INTO io_table_rec.constraint_arr(i).r_column_arr
          FROM all_cons_columns
         WHERE owner = io_table_rec.constraint_arr(i).r_owner
           AND constraint_name = io_table_rec.constraint_arr(i).r_constraint_name
         ORDER BY NVL(position,1);
/*
        -- for self-referencing foreign keys
        IF io_table_rec.constraint_arr(i).r_table_name = io_table_rec.table_name THEN
          FOR j IN 1..io_table_rec.constraint_arr(i).r_column_arr.COUNT LOOP
            io_table_rec.constraint_arr(i).column_id_arr(j) := get_column_indx(io_table_rec, io_table_rec.constraint_arr(i).r_column_arr(j));
          END LOOP;
        END IF;
*/
      END IF;

    END LOOP;

    -- read constraint columns
    IF l_constraints_arr.COUNT > 0 THEN
      SELECT constraint_name, column_name, NVL(position,1)
        BULK COLLECT
        INTO l_col_cons_name_arr, l_col_name_arr, l_col_position_arr
        FROM all_cons_columns
       WHERE owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name;

      FOR i IN 1..l_col_name_arr.COUNT LOOP
        l_indx := io_table_rec.constraint_indx_arr(l_col_cons_name_arr(i));
        io_table_rec.constraint_arr(l_indx).column_arr(l_col_position_arr(i)) := l_col_name_arr(i);
--        io_table_rec.constraint_arr(l_indx).column_id_arr(l_col_position_arr(i)) := get_column_indx(io_table_rec, l_col_name_arr(i));
      END LOOP;
    END IF;

    FOR i IN 1..io_table_rec.constraint_arr.COUNT LOOP
      IF io_table_rec.constraint_arr(i).constraint_type IN ('P', 'U') THEN

        -- find index owner. A workaround for oracle ALL_CONSTRAINS bug (index owner is NULL)
        update_constraint_index_owner(
          in_table_rec      => io_table_rec,
          io_constraint_rec => io_table_rec.constraint_arr(i)
        );

        l_index_full_name := '"'||io_table_rec.constraint_arr(i).index_owner||'"."'||io_table_rec.constraint_arr(i).index_name||'"';
        IF io_table_rec.index_indx_arr.EXISTS(l_index_full_name) THEN
          l_indx := io_table_rec.index_indx_arr(l_index_full_name);
          io_table_rec.index_arr(l_indx).constraint_name := io_table_rec.constraint_arr(i).constraint_name;
          IF io_table_rec.index_arr(l_indx).rename_rec.object_name = io_table_rec.constraint_arr(i).constraint_name AND
             io_table_rec.constraint_arr(i).constraint_name <> io_table_rec.constraint_arr(i).rename_rec.object_name
          THEN
            io_table_rec.index_arr(l_indx).rename_rec.object_name := io_table_rec.constraint_arr(i).rename_rec.object_name;
          END IF;

        END IF;
      END IF;

    END LOOP;

    stop_timer;
  END read_table_constraints;


  PROCEDURE read_table_references(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_all_constraints_arr      gt_all_constraints_arr;
    l_ref_table_rename_rec     gt_rename_rec;
  BEGIN
    start_timer;
    FOR j IN 1..io_table_rec.constraint_arr.COUNT LOOP
      IF io_table_rec.constraint_arr(j).constraint_type IN ('P','U') THEN
        SELECT r.*
          BULK COLLECT
          INTO l_all_constraints_arr
          FROM all_constraints r
         WHERE r.r_owner = io_table_rec.constraint_arr(j).owner
           AND r.r_constraint_name = io_table_rec.constraint_arr(j).constraint_name
           AND r.constraint_type = 'R'
           AND '"'||r.owner||'"."'||r.table_name||'"' <> '"'||io_table_rec.owner||'"."'||io_table_rec.table_name||'"'
           -- self referenced FK
        ;

        FOR i IN 1..l_all_constraints_arr.COUNT LOOP
          io_table_rec.constraint_arr(j).has_references := TRUE;

          io_table_rec.ref_constraint_indx_arr(l_all_constraints_arr(i).constraint_name) := i;

          io_table_rec.ref_constraint_arr(i).r_table_name := io_table_rec.table_name;
          io_table_rec.ref_constraint_arr(i).r_column_arr := io_table_rec.constraint_arr(j).column_arr;

          assign_constraint_rec(
            io_constraint_rec      => io_table_rec.ref_constraint_arr(i),
            in_all_constraints_rec => l_all_constraints_arr(i)
          );
          l_ref_table_rename_rec := get_rename_rec(
                                      in_object_name  => io_table_rec.ref_constraint_arr(i).table_name,
                                      in_object_owner => io_table_rec.ref_constraint_arr(i).owner,
                                      in_object_type  => 'TABLE'
                                    );

          SELECT column_name
            BULK COLLECT
            INTO io_table_rec.ref_constraint_arr(i).column_arr
            FROM all_cons_columns
           WHERE owner = l_all_constraints_arr(i).owner
             AND table_name = l_all_constraints_arr(i).table_name
             AND constraint_name = l_all_constraints_arr(i).constraint_name
           ORDER BY position;

          io_table_rec.ref_constraint_arr(i).rename_rec := get_rename_rec(
                                                             in_table_name_rec => l_ref_table_rename_rec,
                                                             in_object_name    => l_all_constraints_arr(i).constraint_name,
                                                             in_object_owner   => l_all_constraints_arr(i).owner,
                                                             in_object_type    => 'REFERENCE',
                                                             in_index          => i,
                                                             in_generated      => CASE l_all_constraints_arr(i).generated WHEN 'USER NAME' THEN 'N' ELSE 'Y' END
                                                           );
        END LOOP;
      END IF;
    END LOOP;
    stop_timer;
  END read_table_references;


  -- read log groups details from all_log_groups
  PROCEDURE read_table_log_groups(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE gt_all_log_groups_arr IS TABLE OF all_log_groups%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_log_groups_arr       gt_all_log_groups_arr;
    l_col_log_name_arr         arrays.gt_name_arr;
    l_col_name_arr             arrays.gt_lstr_arr;
    l_col_log_arr              arrays.gt_name_arr;
    l_col_position_arr         arrays.gt_num_arr;
    l_indx                     PLS_INTEGER;
  BEGIN
    start_timer;
    SELECT *
      BULK COLLECT
      INTO l_all_log_groups_arr
      FROM all_log_groups
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_log_groups_arr.COUNT LOOP
      io_table_rec.log_group_indx_arr(l_all_log_groups_arr(i).log_group_name) := i;
      io_table_rec.log_group_arr(i).owner             := l_all_log_groups_arr(i).owner;
      io_table_rec.log_group_arr(i).log_group_name    := l_all_log_groups_arr(i).log_group_name;
      io_table_rec.log_group_arr(i).log_group_type    := l_all_log_groups_arr(i).log_group_type;
      io_table_rec.log_group_arr(i).table_name        := l_all_log_groups_arr(i).table_name;
      io_table_rec.log_group_arr(i).always            := l_all_log_groups_arr(i).always;
      io_table_rec.log_group_arr(i).generated         := l_all_log_groups_arr(i).generated;
      io_table_rec.log_group_arr(i).drop_flag         := FALSE;

      io_table_rec.log_group_arr(i).rename_rec := get_rename_rec(
                                                    in_table_name_rec => io_table_rec.rename_rec,
                                                    in_object_name    => l_all_log_groups_arr(i).log_group_name,
                                                    in_object_owner   => l_all_log_groups_arr(i).owner,
                                                    in_object_type    => 'LOG GROUP',
                                                    in_index          => i,
                                                    in_generated      => CASE l_all_log_groups_arr(i).generated WHEN 'USER NAME' THEN 'N' ELSE 'Y' END
                                                  );
    END LOOP;

    IF l_all_log_groups_arr.COUNT > 0 THEN
      SELECT log_group_name, column_name, logging_property, NVL(position,1)
        BULK COLLECT
        INTO l_col_log_name_arr, l_col_name_arr, l_col_log_arr, l_col_position_arr
        FROM all_log_group_columns
       WHERE owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name;

      FOR i IN 1..l_col_name_arr.COUNT LOOP
        l_indx := io_table_rec.log_group_indx_arr(l_col_log_name_arr(i));
        io_table_rec.log_group_arr(l_indx).column_arr(l_col_position_arr(i)) := l_col_name_arr(i);
        io_table_rec.log_group_arr(l_indx).column_log_arr(l_col_position_arr(i)) := l_col_log_arr(i);
      END LOOP;
    END IF;

    stop_timer;
  END read_table_log_groups;

  -- reads table lobs
  PROCEDURE read_table_lobs(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_lobs     IS TABLE OF all_lobs%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_part_lob_arr IS TABLE OF all_part_lobs%ROWTYPE INDEX BY PLS_INTEGER;
    l_lob_arr           t_all_lobs;
    l_part_lob_arr      t_part_lob_arr;
    l_column_indx       PLS_INTEGER;
  BEGIN
    start_timer;

    IF io_table_rec.partitioned = 'YES' THEN
      SELECT *
        BULK COLLECT
        INTO l_part_lob_arr
        FROM all_part_lobs
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
         AND INSTR(column_name, '"') = 0; -- ignore LOB columns of object data types

      FOR i IN 1..l_part_lob_arr.COUNT LOOP
        l_column_indx := get_column_indx(io_table_rec, l_part_lob_arr(i).column_name);
        io_table_rec.lob_indx_arr(l_part_lob_arr(i).column_name) := l_column_indx;
        io_table_rec.lob_arr(l_column_indx).column_indx     := l_column_indx;
        io_table_rec.lob_arr(l_column_indx).owner           := l_part_lob_arr(i).table_owner;
        io_table_rec.lob_arr(l_column_indx).table_name      := l_part_lob_arr(i).table_name;
        io_table_rec.lob_arr(l_column_indx).column_name     := l_part_lob_arr(i).column_name;
        io_table_rec.lob_arr(l_column_indx).lob_name        := l_part_lob_arr(i).lob_name;
        io_table_rec.lob_arr(l_column_indx).lob_index_name  := l_part_lob_arr(i).lob_index_name;
        io_table_rec.lob_arr(l_column_indx).tablespace_name := l_part_lob_arr(i).def_tablespace_name;
        -- Oracle bug workaround
        IF l_part_lob_arr(i).def_tablespace_name IS NOT NULL AND
           io_table_rec.tablespace_block_size_indx.EXISTS(l_part_lob_arr(i).def_tablespace_name)
        THEN
          io_table_rec.lob_arr(l_column_indx).chunk         := l_part_lob_arr(i).def_chunk * io_table_rec.tablespace_block_size_indx(l_part_lob_arr(i).def_tablespace_name);
        ELSE
          io_table_rec.lob_arr(l_column_indx).chunk         := l_part_lob_arr(i).def_chunk;
        END IF;
        io_table_rec.lob_arr(l_column_indx).pctversion      := l_part_lob_arr(i).def_pctversion;
        io_table_rec.lob_arr(l_column_indx).retention       := NVL(l_part_lob_arr(i).def_retention,'NONE');
        io_table_rec.lob_arr(l_column_indx).min_retention   := NULLIF(NULLIF(l_part_lob_arr(i).def_minret,'DEFAULT'),0);
        io_table_rec.lob_arr(l_column_indx).cache           := l_part_lob_arr(i).def_cache;
        IF l_part_lob_arr(i).def_logging <> 'NONE' THEN
          io_table_rec.lob_arr(l_column_indx).logging       := l_part_lob_arr(i).def_logging;
        END IF;
        io_table_rec.lob_arr(l_column_indx).in_row          := l_part_lob_arr(i).def_in_row;

        io_table_rec.lob_arr(l_column_indx).storage.initial_extent  := NULLIF(l_part_lob_arr(i).def_initial_extent,'DEFAULT');
--        io_table_rec.lob_arr(l_column_indx).storage.next_extent     := l_part_lob_arr(i).def_next_extent;
        io_table_rec.lob_arr(l_column_indx).storage.min_extents     := NULLIF(l_part_lob_arr(i).def_min_extents,'DEFAULT');
--        io_table_rec.lob_arr(l_column_indx).storage.max_extents     := l_part_lob_arr(i).def_max_extents;
        io_table_rec.lob_arr(l_column_indx).storage.max_size        := NULLIF(l_part_lob_arr(i).def_max_size,'DEFAULT');
        io_table_rec.lob_arr(l_column_indx).storage.pct_increase    := NULLIF(l_part_lob_arr(i).def_pct_increase,'DEFAULT');
        io_table_rec.lob_arr(l_column_indx).storage.freelists       := l_part_lob_arr(i).def_freelists;
        io_table_rec.lob_arr(l_column_indx).storage.freelist_groups := l_part_lob_arr(i).def_freelist_groups;
        io_table_rec.lob_arr(l_column_indx).storage.buffer_pool     := NULLIF(l_part_lob_arr(i).def_buffer_pool,'DEFAULT');


        $IF dbms_db_version.version >= 11 $THEN
        io_table_rec.lob_arr(l_column_indx).encrypt         := l_part_lob_arr(i).def_encrypt;
        io_table_rec.lob_arr(l_column_indx).compression     := l_part_lob_arr(i).def_compress;
        io_table_rec.lob_arr(l_column_indx).deduplication   := l_part_lob_arr(i).def_deduplicate;
        io_table_rec.lob_arr(l_column_indx).securefile      := l_part_lob_arr(i).def_securefile;
        $IF dbms_db_version.ver_le_11_2 OR dbms_db_version.version >= 12 $THEN
        io_table_rec.lob_arr(l_column_indx).flash_cache      := l_part_lob_arr(i).def_flash_cache;
        io_table_rec.lob_arr(l_column_indx).cell_flash_cache := l_part_lob_arr(i).def_cell_flash_cache;
        $END
        $END

        io_table_rec.lob_arr(l_column_indx).rename_rec := get_rename_rec(
                                                            in_object_name  => io_table_rec.lob_arr(l_column_indx).lob_name,
                                                            in_object_owner => io_table_rec.lob_arr(l_column_indx).owner,
                                                            in_object_type  => 'LOB'
                                                          );
      END LOOP;

    ELSE
      SELECT *
        BULK COLLECT
        INTO l_lob_arr
        FROM all_lobs
       WHERE owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
         AND INSTR(column_name, '"') = 0; -- ignore LOB columns of object data types

      FOR i IN 1..l_lob_arr.COUNT LOOP
        l_column_indx := get_column_indx(io_table_rec, l_lob_arr(i).column_name);
        io_table_rec.lob_arr(l_column_indx).column_indx     := l_column_indx;
        io_table_rec.lob_indx_arr(l_lob_arr(i).column_name) := l_column_indx;
        io_table_rec.lob_arr(l_column_indx).owner           := l_lob_arr(i).owner;
        io_table_rec.lob_arr(l_column_indx).table_name      := l_lob_arr(i).table_name;
        io_table_rec.lob_arr(l_column_indx).column_name     := l_lob_arr(i).column_name;
        io_table_rec.lob_arr(l_column_indx).lob_name        := l_lob_arr(i).segment_name;
        io_table_rec.lob_arr(l_column_indx).lob_index_name  := l_lob_arr(i).index_name;
        io_table_rec.lob_arr(l_column_indx).tablespace_name := l_lob_arr(i).tablespace_name;
        io_table_rec.lob_arr(l_column_indx).chunk           := l_lob_arr(i).chunk;
        io_table_rec.lob_arr(l_column_indx).pctversion      := l_lob_arr(i).pctversion;
        io_table_rec.lob_arr(l_column_indx).freepools       := l_lob_arr(i).freepools;
        io_table_rec.lob_arr(l_column_indx).cache           := l_lob_arr(i).cache;
        io_table_rec.lob_arr(l_column_indx).logging         := l_lob_arr(i).logging;
        io_table_rec.lob_arr(l_column_indx).in_row          := l_lob_arr(i).in_row;
        io_table_rec.lob_arr(l_column_indx).partitioned     := l_lob_arr(i).partitioned;
        $IF dbms_db_version.version >= 11 $THEN
        io_table_rec.lob_arr(l_column_indx).encrypt         := l_lob_arr(i).encrypt;
        io_table_rec.lob_arr(l_column_indx).compression     := l_lob_arr(i).compression;
        io_table_rec.lob_arr(l_column_indx).deduplication   := l_lob_arr(i).deduplication;
        io_table_rec.lob_arr(l_column_indx).securefile      := l_lob_arr(i).securefile;
        $END

        $IF dbms_db_version.version >= 12 $THEN
        io_table_rec.lob_arr(l_column_indx).retention  := l_lob_arr(i).retention_type;
        io_table_rec.lob_arr(l_column_indx).min_retention := l_lob_arr(i).retention_value;
        $ELSE
        IF l_lob_arr(i).owner = SYS_CONTEXT('USERENV','CURRENT_USER') THEN
          BEGIN
            SELECT NVL(retention,'NONE'), NULLIF(minretention,0)
              INTO io_table_rec.lob_arr(l_column_indx).retention,
                   io_table_rec.lob_arr(l_column_indx).min_retention
              FROM user_segments
             WHERE segment_name = l_lob_arr(i).segment_name
               AND segment_type = 'LOBSEGMENT';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              io_table_rec.lob_arr(l_column_indx).retention := 'NONE';
              io_table_rec.lob_arr(l_column_indx).min_retention := 0;
          END;
        ELSE
          BEGIN
            EXECUTE IMMEDIATE
         q'{SELECT NVL(retention,'NONE'), NULLIF(minretention,0)
              FROM dba_segments
             WHERE segment_name = :in_segment_name
               AND owner = :in_segment_owner
               AND segment_type = 'LOBSEGMENT'}'
              INTO io_table_rec.lob_arr(l_column_indx).retention, io_table_rec.lob_arr(l_column_indx).min_retention
             USING l_lob_arr(i).segment_name,
                   l_lob_arr(i).owner;
          EXCEPTION
            WHEN ge_table_not_exist THEN
              cort_log_pkg.echo('WARNING!!! You need grant select on dba_segments to perform this change correctly');
              io_table_rec.lob_arr(l_column_indx).retention := 'NONE';
              io_table_rec.lob_arr(l_column_indx).min_retention := 0;
            WHEN NO_DATA_FOUND THEN
              io_table_rec.lob_arr(l_column_indx).retention := 'NONE';
              io_table_rec.lob_arr(l_column_indx).min_retention := 0;
          END;
        END IF;
        $END

        io_table_rec.lob_arr(l_column_indx).rename_rec := get_rename_rec(
                                                            in_object_name  => io_table_rec.lob_arr(l_column_indx).lob_name,
                                                            in_object_owner => io_table_rec.lob_arr(l_column_indx).owner,
                                                            in_object_type  => 'LOB'
                                                          );
        io_table_rec.lob_arr(l_column_indx).rename_rec.parent_object_name := l_lob_arr(i).table_name;
        io_table_rec.lob_arr(l_column_indx).rename_rec.lob_column_name := l_lob_arr(i).column_name;
      END LOOP;
    END IF;

    stop_timer;
  END read_table_lobs;

  -- read xml columns
  PROCEDURE read_table_xml_cols(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_xml_cols IS TABLE OF all_xml_tab_cols%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_xml_cols      t_all_xml_cols;
    l_column_indx       PLS_INTEGER;
    l_lob_column_indx   PLS_INTEGER;
    l_column_id         PLS_INTEGER;
  BEGIN
    start_timer;

    SELECT *
      BULK COLLECT
      INTO l_all_xml_cols
      FROM all_xml_tab_cols
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_xml_cols.COUNT LOOP
      l_column_indx := get_column_indx(io_table_rec, l_all_xml_cols(i).column_name);
      l_column_id := io_table_rec.column_arr(l_column_indx).column_id;
      io_table_rec.xml_col_arr(l_column_indx).column_indx    := l_column_indx;
      io_table_rec.xml_col_arr(l_column_indx).owner          := l_all_xml_cols(i).owner;
      io_table_rec.xml_col_arr(l_column_indx).table_name     := l_all_xml_cols(i).table_name;
      io_table_rec.xml_col_arr(l_column_indx).column_name    := l_all_xml_cols(i).column_name;
      io_table_rec.xml_col_arr(l_column_indx).xmlschema      := l_all_xml_cols(i).xmlschema;
      io_table_rec.xml_col_arr(l_column_indx).schema_owner   := l_all_xml_cols(i).schema_owner;
      io_table_rec.xml_col_arr(l_column_indx).element_name   := l_all_xml_cols(i).element_name;
      io_table_rec.xml_col_arr(l_column_indx).storage_type   := l_all_xml_cols(i).storage_type;
      io_table_rec.xml_col_arr(l_column_indx).anyschema      := l_all_xml_cols(i).anyschema;
      io_table_rec.xml_col_arr(l_column_indx).nonschema      := l_all_xml_cols(i).nonschema;
      CASE io_table_rec.xml_col_arr(l_column_indx).storage_type
      WHEN 'BINARY' THEN
        -- next column after XML should be hidden BLOB with the same column_id
        l_lob_column_indx := l_column_indx + 1;
        IF io_table_rec.column_arr.EXISTS(l_lob_column_indx) AND
           io_table_rec.column_arr(l_lob_column_indx).column_id = l_column_id AND
           io_table_rec.column_arr(l_lob_column_indx).data_type = 'BLOB' AND
           io_table_rec.column_arr(l_lob_column_indx).hidden_column = 'YES'
        THEN
          io_table_rec.xml_col_arr(l_column_indx).lob_column_indx := l_lob_column_indx;
          io_table_rec.lob_arr(l_lob_column_indx).xml_column_indx := l_column_indx;
        END IF;
      WHEN 'CLOB' THEN
        -- next column after XML should be hidden CLOB with the same column_id
        l_lob_column_indx := l_column_indx + 1;
        IF io_table_rec.column_arr.EXISTS(l_lob_column_indx) AND
           io_table_rec.column_arr(l_lob_column_indx).column_id = l_column_id AND
           io_table_rec.column_arr(l_lob_column_indx).data_type = 'CLOB' AND
           io_table_rec.column_arr(l_lob_column_indx).hidden_column = 'YES'
        THEN
          io_table_rec.xml_col_arr(l_column_indx).lob_column_indx := l_lob_column_indx;
          io_table_rec.lob_arr(l_lob_column_indx).xml_column_indx := l_column_indx;
        END IF;
      ELSE
        NULL;
      END CASE;
    END LOOP;

    stop_timer;
  END read_table_xml_cols;

  -- read varray columns
  PROCEDURE read_table_varrays(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_varrays IS TABLE OF all_varrays%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_varrays       t_all_varrays;
    l_column_indx       PLS_INTEGER;
  BEGIN
    start_timer;
    SELECT *
      BULK COLLECT
      INTO l_all_varrays
      FROM all_varrays
     WHERE owner = io_table_rec.owner
       AND parent_table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_varrays.COUNT LOOP
      l_column_indx := get_column_indx(io_table_rec, l_all_varrays(i).parent_table_column);
      io_table_rec.varray_arr(l_column_indx).owner                 := l_all_varrays(i).owner;
      io_table_rec.varray_arr(l_column_indx).table_name            := l_all_varrays(i).parent_table_name;
      io_table_rec.varray_arr(l_column_indx).column_name           := l_all_varrays(i).parent_table_column;
      io_table_rec.varray_arr(l_column_indx).column_indx           := l_column_indx;
      io_table_rec.varray_arr(l_column_indx).type_owner            := l_all_varrays(i).type_owner;
      io_table_rec.varray_arr(l_column_indx).type_name             := l_all_varrays(i).type_name;
      io_table_rec.varray_arr(l_column_indx).lob_name              := l_all_varrays(i).lob_name;
      io_table_rec.varray_arr(l_column_indx).storage_spec          := TRIM(l_all_varrays(i).storage_spec);
      io_table_rec.varray_arr(l_column_indx).return_type           := TRIM(l_all_varrays(i).return_type);
      io_table_rec.varray_arr(l_column_indx).element_substitutable := TRIM(l_all_varrays(i).element_substitutable);
      IF io_table_rec.lob_arr.EXISTS(l_column_indx) THEN
        io_table_rec.lob_arr(l_column_indx).varray_column_indx := l_column_indx;
        io_table_rec.varray_arr(l_column_indx).lob_column_indx := l_column_indx;
        io_table_rec.lob_arr(l_column_indx).rename_rec.object_type := 'VARRAY';
      END IF;
    END LOOP;

    stop_timer;
  END read_table_varrays;

  PROCEDURE read_partition_rowcount(
    io_partition_arr   IN OUT NOCOPY gt_partition_arr,
    in_sql             IN VARCHAR2
  )
  AS
    l_indx_arr                 arrays.gt_int_arr;
    l_cnt_arr                  arrays.gt_num_arr;
  BEGIN
    cort_log_pkg.execute(
      in_text      => in_sql
    );
    BEGIN
      EXECUTE IMMEDIATE in_sql
         BULK COLLECT
         INTO l_indx_arr, l_cnt_arr;
    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.update_exec_time;
        cort_log_pkg.error('Error in parsing/executing sql read partition rowcount',in_sql);
    END;
    cort_log_pkg.update_exec_time;

    FOR i IN 1..l_indx_arr.COUNT LOOP
      io_partition_arr(l_indx_arr(i)).is_partition_empty := l_cnt_arr(i) = 0;
    END LOOP;
  END read_partition_rowcount;

  -- reads table partitions
  PROCEDURE read_partitions(
    io_table_rec IN OUT NOCOPY gt_table_rec,
    in_read_data IN BOOLEAN
  )
  AS
    TYPE gt_tab_partitions_arr IS TABLE OF all_tab_partitions%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE gt_lob_partitions_arr IS TABLE OF all_lob_partitions%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE gt_ind_partitions_arr IS TABLE OF all_ind_partitions%ROWTYPE INDEX BY PLS_INTEGER;
    l_tab_partitions_arr       gt_tab_partitions_arr;
    l_ind_partitions_arr       gt_ind_partitions_arr;
    l_lob_partitions_arr       gt_lob_partitions_arr;
    l_indx                     PLS_INTEGER;
    l_lob_indx                 PLS_INTEGER;
    l_lob_rec                  gt_lob_rec;
    l_block_size               NUMBER;
    l_part_sql                 CLOB;
    l_sql                      CLOB;
    l_segment_created          BOOLEAN;
  BEGIN
    start_timer;
    IF io_table_rec.partitioned = 'YES' THEN
      SELECT *
        BULK COLLECT
        INTO l_tab_partitions_arr
        FROM all_tab_partitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY partition_position;

      SELECT *
        BULK COLLECT
        INTO l_lob_partitions_arr
        FROM all_lob_partitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY partition_position, column_name;
    END IF;

    FOR i IN 1..l_tab_partitions_arr.COUNT LOOP
      io_table_rec.partition_indx_arr(l_tab_partitions_arr(i).partition_name) := i;
      io_table_rec.partition_arr(i).indx               := i;
      io_table_rec.partition_arr(i).table_owner        := l_tab_partitions_arr(i).table_owner;
      io_table_rec.partition_arr(i).table_name         := l_tab_partitions_arr(i).table_name;
      io_table_rec.partition_arr(i).partition_level    := 'PARTITION';
      io_table_rec.partition_arr(i).partition_type     := io_table_rec.partitioning_type;
      io_table_rec.partition_arr(i).partition_name     := l_tab_partitions_arr(i).partition_name;
      io_table_rec.partition_arr(i).composite          := l_tab_partitions_arr(i).composite;
      io_table_rec.partition_arr(i).high_value         := l_tab_partitions_arr(i).high_value;
      io_table_rec.partition_arr(i).position           := l_tab_partitions_arr(i).partition_position;
      io_table_rec.partition_arr(i).physical_attr_rec.pct_free := l_tab_partitions_arr(i).pct_free;
      io_table_rec.partition_arr(i).physical_attr_rec.pct_used := l_tab_partitions_arr(i).pct_used;
      io_table_rec.partition_arr(i).physical_attr_rec.ini_trans := l_tab_partitions_arr(i).ini_trans;
      io_table_rec.partition_arr(i).physical_attr_rec.max_trans := l_tab_partitions_arr(i).max_trans;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.initial_extent := l_tab_partitions_arr(i).initial_extent;
--      io_table_rec.partition_arr(i).physical_attr_rec.storage.next_extent := l_tab_partitions_arr(i).next_extent;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.min_extents := l_tab_partitions_arr(i).min_extent;
--      io_table_rec.partition_arr(i).physical_attr_rec.storage.max_extents := l_tab_partitions_arr(i).max_extent;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.pct_increase := l_tab_partitions_arr(i).pct_increase;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.freelists := l_tab_partitions_arr(i).freelists;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.freelist_groups := l_tab_partitions_arr(i).freelist_groups;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.buffer_pool := NULLIF(l_tab_partitions_arr(i).buffer_pool,'DEFAULT');
      io_table_rec.partition_arr(i).compression_rec.compression := l_tab_partitions_arr(i).compression;
      io_table_rec.compressed_partitions := io_table_rec.compressed_partitions OR (l_tab_partitions_arr(i).compression = 'ENABLED');
      io_table_rec.partition_arr(i).logging := l_tab_partitions_arr(i).logging;
      io_table_rec.partition_arr(i).tablespace_name := l_tab_partitions_arr(i).tablespace_name;
      $IF dbms_db_version.version >= 11 $THEN
      io_table_rec.partition_arr(i).compression_rec.compress_for := l_tab_partitions_arr(i).compress_for;
      l_segment_created := l_tab_partitions_arr(i).segment_created = 'YES';
      $ELSE
      l_segment_created := TRUE;
      $END

      IF in_read_data THEN
        IF l_segment_created AND io_table_rec.subpartitioning_type = 'NONE' THEN
          l_part_sql := 'SELECT '||TO_CHAR(i)||' AS indx, COUNT(*) AS cnt FROM "'||io_table_rec.owner||'"."'||io_table_rec.table_name||'" PARTITION ("'||io_table_rec.partition_arr(i).partition_name||'") WHERE ROWNUM = 1';
          IF LENGTH(l_sql) + LENGTH(l_part_sql) + 13 > 32767 THEN
            read_partition_rowcount(
              io_partition_arr => io_table_rec.partition_arr,
              in_sql           => l_sql
            );
            l_sql := null;
          END IF;

          IF l_sql IS NULL THEN
            l_sql := l_part_sql;
          ELSE
             l_sql := l_sql ||chr(10)||' UNION ALL '||chr(10)||l_part_sql;
          END IF;
        ELSE
          io_table_rec.partition_arr(i).is_partition_empty := TRUE;
        END IF;
      END IF;
    END LOOP;

    IF in_read_data AND l_sql IS NOT NULL THEN
      read_partition_rowcount(
        io_partition_arr => io_table_rec.partition_arr,
        in_sql           => l_sql
      );
    END IF;


    FOR i IN 1..l_lob_partitions_arr.COUNT LOOP
      l_lob_rec.owner              := l_lob_partitions_arr(i).table_owner;
      l_lob_rec.table_name         := l_lob_partitions_arr(i).table_name;
      l_lob_rec.column_name        := l_lob_partitions_arr(i).column_name;
      l_lob_rec.lob_name           := l_lob_partitions_arr(i).lob_name;
      l_lob_rec.partition_name     := l_lob_partitions_arr(i).partition_name;
      l_lob_rec.partition_level    := 'PARTITION';
      l_lob_rec.lob_partition_name := l_lob_partitions_arr(i).lob_partition_name;
      l_lob_rec.lob_indpart_name   := l_lob_partitions_arr(i).lob_indpart_name;
      l_lob_rec.partition_position := l_lob_partitions_arr(i).partition_position;
      l_lob_rec.tablespace_name    := l_lob_partitions_arr(i).tablespace_name;
      -- Oracle bug workaround
      IF l_lob_partitions_arr(i).tablespace_name IS NOT NULL AND
        io_table_rec.tablespace_block_size_indx.EXISTS(l_lob_partitions_arr(i).tablespace_name) AND
        l_lob_partitions_arr(i).composite = 'NO'
      THEN
        l_block_size := io_table_rec.tablespace_block_size_indx(l_lob_partitions_arr(i).tablespace_name);
      ELSE
        l_block_size := 1;
      END IF;
      l_lob_rec.chunk              := l_lob_partitions_arr(i).chunk * l_block_size;
      --
      l_lob_rec.pctversion         := l_lob_partitions_arr(i).pctversion;
      l_lob_rec.cache              := l_lob_partitions_arr(i).cache;
      l_lob_rec.logging            := l_lob_partitions_arr(i).logging;
      l_lob_rec.in_row             := l_lob_partitions_arr(i).in_row;

      l_lob_rec.storage.initial_extent  := l_lob_partitions_arr(i).initial_extent;
--      l_lob_rec.storage.next_extent     := l_lob_partitions_arr(i).next_extent;
      l_lob_rec.storage.min_extents     := l_lob_partitions_arr(i).min_extents;
--      l_lob_rec.storage.max_extents     := l_lob_partitions_arr(i).max_extents;
      l_lob_rec.storage.max_size        := l_lob_partitions_arr(i).max_size;
      l_lob_rec.storage.pct_increase    := l_lob_partitions_arr(i).pct_increase;
      l_lob_rec.storage.freelists       := l_lob_partitions_arr(i).freelists;
      l_lob_rec.storage.freelist_groups := l_lob_partitions_arr(i).freelist_groups;
      l_lob_rec.storage.buffer_pool     := NULLIF(l_lob_partitions_arr(i).buffer_pool,'DEFAULT');

      $IF dbms_db_version.version >= 11  $THEN
      l_lob_rec.encrypt            := l_lob_partitions_arr(i).encrypt;
      l_lob_rec.compression        := l_lob_partitions_arr(i).compression;
      l_lob_rec.deduplication      := l_lob_partitions_arr(i).deduplication;
      l_lob_rec.securefile         := l_lob_partitions_arr(i).securefile;
      $IF dbms_db_version.version >= 12 OR dbms_db_version.ver_le_11_2 $THEN
      l_lob_rec.flash_cache        := l_lob_partitions_arr(i).flash_cache;
      l_lob_rec.cell_flash_cache   := l_lob_partitions_arr(i).cell_flash_cache;
      $END
      $END
      l_lob_rec.column_indx := get_column_indx(io_table_rec, l_lob_rec.column_name);
      l_indx := io_table_rec.partition_indx_arr(l_lob_partitions_arr(i).partition_name);
      l_lob_indx := io_table_rec.partition_arr(l_indx).lob_arr.COUNT+1;
      io_table_rec.partition_arr(l_indx).lob_arr(l_lob_indx) := l_lob_rec;
      io_table_rec.partition_arr(l_indx).lob_indx_arr(l_lob_rec.column_indx) := l_lob_indx;
    END LOOP;

    IF io_table_rec.iot_type IS NOT NULL AND
       io_table_rec.overflow_table_name IS NOT NULL AND
       io_table_rec.partitioned = 'YES'
    THEN
      SELECT *
        BULK COLLECT
        INTO l_ind_partitions_arr
        FROM all_ind_partitions
       WHERE index_owner = io_table_rec.iot_index_owner
         AND index_name = io_table_rec.iot_index_name
       ORDER BY partition_position;

      FOR i IN 1..l_ind_partitions_arr.COUNT LOOP
        io_table_rec.partition_indx_arr(l_ind_partitions_arr(i).partition_name) := i;
        io_table_rec.partition_arr(i).iot_key_compression := l_ind_partitions_arr(i).compression;
        io_table_rec.partition_arr(i).physical_attr_rec.pct_free := l_ind_partitions_arr(i).pct_free;
        io_table_rec.partition_arr(i).physical_attr_rec.ini_trans := l_ind_partitions_arr(i).ini_trans;
        io_table_rec.partition_arr(i).physical_attr_rec.max_trans := l_ind_partitions_arr(i).max_trans;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.initial_extent := l_ind_partitions_arr(i).initial_extent;
        --io_table_rec.partition_arr(i).physical_attr_rec.storage.next_extent := l_ind_partitions_arr(i).next_extent;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.min_extents := l_ind_partitions_arr(i).min_extent;
        --io_table_rec.partition_arr(i).physical_attr_rec.storage.max_extents := l_ind_partitions_arr(i).max_extent;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.pct_increase := l_ind_partitions_arr(i).pct_increase;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.freelists := l_ind_partitions_arr(i).freelists;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.freelist_groups := l_ind_partitions_arr(i).freelist_groups;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.buffer_pool := NULLIF(l_ind_partitions_arr(i).buffer_pool,'DEFAULT');
        io_table_rec.partition_arr(i).logging := l_ind_partitions_arr(i).logging;
        io_table_rec.partition_arr(i).tablespace_name := l_ind_partitions_arr(i).tablespace_name;
      END LOOP;

      SELECT *
        BULK COLLECT
        INTO l_tab_partitions_arr
        FROM all_tab_partitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.overflow_table_name
       ORDER BY partition_position;

      FOR i IN 1..l_tab_partitions_arr.COUNT LOOP
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.pct_free := l_tab_partitions_arr(i).pct_free;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.pct_used := l_tab_partitions_arr(i).pct_used;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.ini_trans := l_tab_partitions_arr(i).ini_trans;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.max_trans := l_tab_partitions_arr(i).max_trans;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.initial_extent := l_tab_partitions_arr(i).initial_extent;
        --io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.next_extent := l_tab_partitions_arr(i).next_extent;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.min_extents := l_tab_partitions_arr(i).min_extent;
        --io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.max_extents := l_tab_partitions_arr(i).max_extent;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.pct_increase := l_tab_partitions_arr(i).pct_increase;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.freelists := l_tab_partitions_arr(i).freelists;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.freelist_groups := l_tab_partitions_arr(i).freelist_groups;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.buffer_pool := NULLIF(l_tab_partitions_arr(i).buffer_pool,'DEFAULT');
        io_table_rec.partition_arr(i).overflow_logging := l_tab_partitions_arr(i).logging;
        io_table_rec.partition_arr(i).overflow_tablespace := l_tab_partitions_arr(i).tablespace_name;
      END LOOP;
    END IF;

    stop_timer;
  END read_partitions;

  -- reads table subpartitions
  PROCEDURE read_subpartitions(
    io_table_rec IN OUT NOCOPY gt_table_rec,
    in_read_data IN BOOLEAN
  )
  AS
    TYPE gt_tab_subpartitions_arr IS TABLE OF all_tab_subpartitions%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE gt_lob_subpartitions_arr IS TABLE OF all_lob_subpartitions%ROWTYPE INDEX BY PLS_INTEGER;
    l_tab_subpartitions_arr       gt_tab_subpartitions_arr;
    l_lob_subpartitions_arr       gt_lob_subpartitions_arr;
    l_last_partition_name         arrays.gt_name;
    l_partition_name              arrays.gt_name;
    l_partition_indx              PLS_INTEGER;
    l_indx                        PLS_INTEGER;
    l_lob_indx                    PLS_INTEGER;
    l_lob_rec                     gt_lob_rec;
    l_part_sql                    CLOB;
    l_sql                         CLOB;
    l_segment_created             BOOLEAN;
    l_parent_name                 arrays.gt_name;
  BEGIN
    start_timer;
    IF io_table_rec.subpartitioning_type IS NOT NULL THEN
      SELECT *
        BULK COLLECT
        INTO l_tab_subpartitions_arr
        FROM all_tab_subpartitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY partition_name, subpartition_position;

      SELECT *
        BULK COLLECT
        INTO l_lob_subpartitions_arr
        FROM all_lob_subpartitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY subpartition_position, column_name, lob_partition_name;

    END IF;

    l_last_partition_name := NULL;
    FOR i IN 1..l_tab_subpartitions_arr.COUNT LOOP
      io_table_rec.subpartition_indx_arr(l_tab_subpartitions_arr(i).subpartition_name) := i;
      l_partition_name := l_tab_subpartitions_arr(i).partition_name;
      IF io_table_rec.partition_indx_arr.EXISTS(l_partition_name) THEN
        l_partition_indx := io_table_rec.partition_indx_arr(l_partition_name);
        IF l_last_partition_name = l_partition_name THEN
          io_table_rec.partition_arr(l_partition_indx).subpartition_to_indx := i;
        ELSE
          l_last_partition_name := l_partition_name;
          io_table_rec.partition_arr(l_partition_indx).subpartition_from_indx := i;
          io_table_rec.partition_arr(l_partition_indx).subpartition_to_indx := i;
        END IF;
      END IF;
      io_table_rec.subpartition_arr(i).indx               := i;
      io_table_rec.subpartition_arr(i).table_owner        := l_tab_subpartitions_arr(i).table_owner;
      io_table_rec.subpartition_arr(i).table_name         := l_tab_subpartitions_arr(i).table_name;
      io_table_rec.subpartition_arr(i).partition_level    := 'SUBPARTITION';
      io_table_rec.subpartition_arr(i).partition_type     := io_table_rec.subpartitioning_type;
      io_table_rec.subpartition_arr(i).partition_name     := l_tab_subpartitions_arr(i).subpartition_name;
      io_table_rec.subpartition_arr(i).parent_partition_name := l_tab_subpartitions_arr(i).partition_name;
      io_table_rec.subpartition_arr(i).high_value         := l_tab_subpartitions_arr(i).high_value;
      io_table_rec.subpartition_arr(i).position           := l_tab_subpartitions_arr(i).subpartition_position;
      io_table_rec.subpartition_arr(i).physical_attr_rec.pct_free := l_tab_subpartitions_arr(i).pct_free;
      io_table_rec.subpartition_arr(i).physical_attr_rec.pct_used := l_tab_subpartitions_arr(i).pct_used;
      io_table_rec.subpartition_arr(i).physical_attr_rec.ini_trans := l_tab_subpartitions_arr(i).ini_trans;
      io_table_rec.subpartition_arr(i).physical_attr_rec.max_trans := l_tab_subpartitions_arr(i).max_trans;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.initial_extent := l_tab_subpartitions_arr(i).initial_extent;
      --io_table_rec.subpartition_arr(i).physical_attr_rec.storage.next_extent := l_tab_subpartitions_arr(i).next_extent;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.min_extents := l_tab_subpartitions_arr(i).min_extent;
      --io_table_rec.subpartition_arr(i).physical_attr_rec.storage.max_extents := l_tab_subpartitions_arr(i).max_extent;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.pct_increase := l_tab_subpartitions_arr(i).pct_increase;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.freelists := l_tab_subpartitions_arr(i).freelists;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.freelist_groups := l_tab_subpartitions_arr(i).freelist_groups;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.buffer_pool := NULLIF(l_tab_subpartitions_arr(i).buffer_pool,'DEFAULT');
      io_table_rec.subpartition_arr(i).compression_rec.compression := l_tab_subpartitions_arr(i).compression;
      io_table_rec.compressed_partitions := io_table_rec.compressed_partitions OR (l_tab_subpartitions_arr(i).compression = 'ENABLED');
      io_table_rec.subpartition_arr(i).logging := l_tab_subpartitions_arr(i).logging;
      io_table_rec.subpartition_arr(i).tablespace_name := l_tab_subpartitions_arr(i).tablespace_name;
      $IF dbms_db_version.version >= 11 $THEN
      io_table_rec.subpartition_arr(i).compression_rec.compress_for := l_tab_subpartitions_arr(i).compress_for;
      l_segment_created := l_tab_subpartitions_arr(i).segment_created = 'YES';
      $ELSE
      l_segment_created := TRUE;
      $END
      IF in_read_data THEN
        IF l_segment_created THEN
          l_part_sql := 'SELECT '||TO_CHAR(i)||' AS indx, COUNT(*) AS cnt FROM "'||io_table_rec.owner||'"."'||io_table_rec.table_name||'" SUBPARTITION ("'||io_table_rec.subpartition_arr(i).partition_name||'") WHERE ROWNUM = 1';
          IF LENGTH(l_sql) + LENGTH(l_part_sql) + 13 > 32767 THEN
            read_partition_rowcount(
              io_partition_arr => io_table_rec.subpartition_arr,
              in_sql           => l_sql
            );
            l_sql := null;
          END IF;
          IF l_sql IS NULL THEN
            l_sql := l_part_sql;
          ELSE
            l_sql := l_sql ||chr(10)||' UNION ALL '||chr(10)||l_part_sql;
          END IF;
        ELSE
          io_table_rec.subpartition_arr(i).is_partition_empty := TRUE;
        END IF;
      END IF;
    END LOOP;

    IF in_read_data and l_sql IS NOT NULL THEN
      read_partition_rowcount(
        io_partition_arr => io_table_rec.subpartition_arr,
        in_sql           => l_sql
      );

      FOR i IN 1..io_table_rec.subpartition_arr.COUNT LOOP
        IF NOT io_table_rec.subpartition_arr(i).is_partition_empty THEN
          l_parent_name := io_table_rec.subpartition_arr(i).parent_partition_name;
          IF io_table_rec.partition_indx_arr.EXISTS(l_parent_name) THEN
            l_indx := io_table_rec.partition_indx_arr(l_parent_name);
            io_table_rec.partition_arr(l_indx).is_partition_empty := FALSE;
          END IF;
        END IF;
      END LOOP;
    END IF;


    FOR i IN 1..l_lob_subpartitions_arr.COUNT LOOP
      l_lob_rec.owner                := l_lob_subpartitions_arr(i).table_owner;
      l_lob_rec.table_name           := l_lob_subpartitions_arr(i).table_name;
      l_lob_rec.column_name          := l_lob_subpartitions_arr(i).column_name;
      l_lob_rec.lob_name             := l_lob_subpartitions_arr(i).lob_name;
      l_lob_rec.parent_lob_part_name := l_lob_subpartitions_arr(i).lob_partition_name;
      l_lob_rec.partition_name       := l_lob_subpartitions_arr(i).subpartition_name;
      l_lob_rec.partition_level      := 'SUBPARTITION';
      l_lob_rec.lob_partition_name   := l_lob_subpartitions_arr(i).lob_subpartition_name;
      l_lob_rec.lob_indpart_name     := l_lob_subpartitions_arr(i).lob_indsubpart_name;
      l_lob_rec.partition_position   := l_lob_subpartitions_arr(i).subpartition_position;
      l_lob_rec.tablespace_name      := l_lob_subpartitions_arr(i).tablespace_name;
      l_lob_rec.chunk                := l_lob_subpartitions_arr(i).chunk;
      l_lob_rec.pctversion           := l_lob_subpartitions_arr(i).pctversion;
      l_lob_rec.cache                := l_lob_subpartitions_arr(i).cache;
      l_lob_rec.logging              := l_lob_subpartitions_arr(i).logging;
      l_lob_rec.in_row               := l_lob_subpartitions_arr(i).in_row;
      l_lob_rec.retention            := NVL(l_lob_subpartitions_arr(i).retention,'NONE');
      l_lob_rec.min_retention        := NULLIF(l_lob_subpartitions_arr(i).minretention,'0');

      l_lob_rec.storage.initial_extent  := l_lob_subpartitions_arr(i).initial_extent;
      --l_lob_rec.storage.next_extent     := l_lob_subpartitions_arr(i).next_extent;
      l_lob_rec.storage.min_extents     := l_lob_subpartitions_arr(i).min_extents;
      --l_lob_rec.storage.max_extents     := l_lob_subpartitions_arr(i).max_extents;
      l_lob_rec.storage.max_size        := l_lob_subpartitions_arr(i).max_size;
      l_lob_rec.storage.pct_increase    := l_lob_subpartitions_arr(i).pct_increase;
      l_lob_rec.storage.freelists       := l_lob_subpartitions_arr(i).freelists;
      l_lob_rec.storage.freelist_groups := l_lob_subpartitions_arr(i).freelist_groups;
      l_lob_rec.storage.buffer_pool     := NULLIF(l_lob_subpartitions_arr(i).buffer_pool,'DEFAULT');

      $IF dbms_db_version.version >= 11 $THEN
      l_lob_rec.encrypt              := l_lob_subpartitions_arr(i).encrypt;
      l_lob_rec.compression          := l_lob_subpartitions_arr(i).compression;
      l_lob_rec.deduplication        := l_lob_subpartitions_arr(i).deduplication;
      l_lob_rec.securefile           := l_lob_subpartitions_arr(i).securefile;
      $IF dbms_db_version.version >= 12 OR dbms_db_version.ver_le_11_2 $THEN
      l_lob_rec.flash_cache          := l_lob_subpartitions_arr(i).flash_cache;
      l_lob_rec.cell_flash_cache     := l_lob_subpartitions_arr(i).cell_flash_cache;
      $END
      $END
      l_lob_rec.column_indx := get_column_indx(io_table_rec, l_lob_rec.column_name);
      l_indx := io_table_rec.subpartition_indx_arr(l_lob_subpartitions_arr(i).subpartition_name);
      l_lob_indx := io_table_rec.subpartition_arr(l_indx).lob_arr.COUNT+1;
      io_table_rec.subpartition_arr(l_indx).lob_arr(l_lob_indx) := l_lob_rec;
      io_table_rec.subpartition_arr(l_indx).lob_indx_arr(l_lob_rec.column_indx) := l_lob_indx;
    END LOOP;

    stop_timer;
  END read_subpartitions;

  -- read table privileges
  PROCEDURE read_privileges(
    in_owner         IN VARCHAR2,
    in_table_name    IN VARCHAR2,
    io_privilege_arr IN OUT NOCOPY gt_privilege_arr
  )
  AS
    TYPE t_all_tab_privs_arr IS TABLE OF all_tab_privs%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_all_col_privs_arr IS TABLE OF all_col_privs%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_tab_privs_arr      t_all_tab_privs_arr;
    l_all_col_privs_arr      t_all_col_privs_arr;
    l_indx                   PLS_INTEGER;
  BEGIN
    start_timer;
    io_privilege_arr.DELETE;
    SELECT *
      BULK COLLECT
      INTO l_all_tab_privs_arr
      FROM all_tab_privs
     WHERE table_name = in_table_name
       AND table_schema = in_owner
       AND grantor = user;

    FOR i IN 1..l_all_tab_privs_arr.COUNT LOOP
      io_privilege_arr(i).grantor      := l_all_tab_privs_arr(i).grantor;
      io_privilege_arr(i).grantee      := l_all_tab_privs_arr(i).grantee;
      io_privilege_arr(i).table_schema := l_all_tab_privs_arr(i).table_schema;
      io_privilege_arr(i).table_name   := l_all_tab_privs_arr(i).table_name;
      io_privilege_arr(i).privilege    := l_all_tab_privs_arr(i).privilege;
      io_privilege_arr(i).grantable    := l_all_tab_privs_arr(i).grantable;
      io_privilege_arr(i).hierarchy    := l_all_tab_privs_arr(i).hierarchy;
    END LOOP;

    SELECT *
      BULK COLLECT
      INTO l_all_col_privs_arr
      FROM all_col_privs
     WHERE table_name = in_table_name
       AND table_schema = in_owner;

    FOR i IN 1..l_all_col_privs_arr.COUNT LOOP
      l_indx := io_privilege_arr.COUNT + i;
      io_privilege_arr(l_indx).grantor      := l_all_col_privs_arr(i).grantor;
      io_privilege_arr(l_indx).grantee      := l_all_col_privs_arr(i).grantee;
      io_privilege_arr(l_indx).table_schema := l_all_col_privs_arr(i).table_schema;
      io_privilege_arr(l_indx).table_name   := l_all_col_privs_arr(i).table_name;
      io_privilege_arr(l_indx).column_name  := l_all_col_privs_arr(i).column_name;
      io_privilege_arr(l_indx).privilege    := l_all_col_privs_arr(i).privilege;
      io_privilege_arr(l_indx).grantable    := l_all_col_privs_arr(i).grantable;
    END LOOP;
    stop_timer;
  END read_privileges;

  PROCEDURE read_table_triggers(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_triggers_arr         IS TABLE OF all_triggers%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_triggers_arr              t_all_triggers_arr;
    l_full_name                     VARCHAR2(65);
    l_indx                          PLS_INTEGER;
    l_owner_arr                     arrays.gt_name_arr;
    l_trigger_name_arr              arrays.gt_name_arr;
    l_referenced_trigger_owner_arr  arrays.gt_name_arr;
    l_referenced_trigger_name_arr   arrays.gt_name_arr;
    l_ref_indx_arr                  arrays.gt_int_indx;
  BEGIN
    start_timer;
    $IF dbms_db_version.version >= 11 $THEN
    SELECT t.*
      BULK COLLECT
      INTO l_owner_arr, l_trigger_name_arr, l_referenced_trigger_owner_arr, l_referenced_trigger_name_arr
      FROM (SELECT t.owner, t.trigger_name, t1.referenced_trigger_owner, t1.referenced_trigger_name
              FROM all_triggers t
              LEFT JOIN all_trigger_ordering t1
                ON t1.trigger_name = t.trigger_name
               AND t1.ordering_type = 'FOLLOWS'
             WHERE table_name = io_table_rec.table_name
               AND table_owner = io_table_rec.owner
           ) t
    CONNECT BY referenced_trigger_name = PRIOR trigger_name
           AND referenced_trigger_owner = PRIOR owner
    START WITH referenced_trigger_name IS NULL;

    FOR i IN 1..l_trigger_name_arr.COUNT LOOP
      l_full_name := '"'||l_owner_arr(i)||'"."'||l_trigger_name_arr(i)||'"';
      l_ref_indx_arr(l_full_name) := i;
    END LOOP;
    $END

    SELECT *
      BULK COLLECT
      INTO l_all_triggers_arr
      FROM all_triggers
     WHERE table_name = io_table_rec.table_name
       AND table_owner = io_table_rec.owner;

    FOR i IN 1..l_all_triggers_arr.COUNT LOOP
      l_full_name := '"'||l_all_triggers_arr(i).owner||'"."'||l_all_triggers_arr(i).trigger_name||'"';
      $IF dbms_db_version.version >= 11 $THEN
        l_indx := l_ref_indx_arr(l_full_name);
      $ELSE
        l_indx := i;
      $END
      io_table_rec.trigger_indx_arr(l_full_name) := l_indx;
      io_table_rec.trigger_arr(l_indx).owner             := l_all_triggers_arr(i).owner;
      io_table_rec.trigger_arr(l_indx).trigger_name      := l_all_triggers_arr(i).trigger_name;
      io_table_rec.trigger_arr(l_indx).trigger_type      := l_all_triggers_arr(i).trigger_type;
      io_table_rec.trigger_arr(l_indx).triggering_event  := l_all_triggers_arr(i).triggering_event;
      io_table_rec.trigger_arr(l_indx).table_owner       := l_all_triggers_arr(i).table_owner;
      io_table_rec.trigger_arr(l_indx).base_object_type  := l_all_triggers_arr(i).base_object_type;
      io_table_rec.trigger_arr(l_indx).table_name        := l_all_triggers_arr(i).table_name;
      io_table_rec.trigger_arr(l_indx).column_name       := l_all_triggers_arr(i).column_name;
      io_table_rec.trigger_arr(l_indx).referencing_names := l_all_triggers_arr(i).referencing_names;
      io_table_rec.trigger_arr(l_indx).when_clause       := l_all_triggers_arr(i).when_clause;
      io_table_rec.trigger_arr(l_indx).status            := l_all_triggers_arr(i).status;
      io_table_rec.trigger_arr(l_indx).description       := l_all_triggers_arr(i).description;
      io_table_rec.trigger_arr(l_indx).action_type       := l_all_triggers_arr(i).action_type;
      io_table_rec.trigger_arr(l_indx).trigger_body      := l_all_triggers_arr(i).trigger_body;

      io_table_rec.trigger_arr(l_indx).rename_rec := get_rename_rec(
                                                       in_object_name  => l_all_triggers_arr(i).trigger_name,
                                                       in_object_owner => l_all_triggers_arr(i).owner,
                                                       in_object_type  => 'TRIGGER'
                                                     );

      $IF dbms_db_version.version >= 11 $THEN
        IF l_referenced_trigger_name_arr(l_indx) IS NOT NULL THEN
          l_full_name := '"'||l_referenced_trigger_owner_arr(l_indx)||'"."'||l_referenced_trigger_name_arr(l_indx)||'"';
          io_table_rec.trigger_arr(l_indx).referenced_trigger_indx := l_ref_indx_arr(l_full_name);
        END IF;
      $END

    END LOOP;
    stop_timer;
  END read_table_triggers;

  PROCEDURE read_table_policies(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_policy_arr     OUT NOCOPY gt_policy_arr
  )
  AS
    TYPE t_all_policies_arr         IS TABLE OF all_policies%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_policies_arr              t_all_policies_arr;
    l_column_option_arr             arrays.gt_str_arr;
  BEGIN
    start_timer;
    SELECT *
      BULK COLLECT
      INTO l_all_policies_arr
      FROM all_policies
     WHERE object_name = in_table_name
       AND object_owner = in_owner;

    FOR i IN 1..l_all_policies_arr.COUNT LOOP
      out_policy_arr(i).object_owner   := l_all_policies_arr(i).object_owner;
      out_policy_arr(i).object_name    := l_all_policies_arr(i).object_name;
      out_policy_arr(i).policy_group   := l_all_policies_arr(i).policy_group;
      out_policy_arr(i).policy_name    := l_all_policies_arr(i).policy_name;
      out_policy_arr(i).pf_owner       := l_all_policies_arr(i).pf_owner;
      out_policy_arr(i).package        := l_all_policies_arr(i).package;
      out_policy_arr(i).function       := l_all_policies_arr(i).function;
      out_policy_arr(i).sel            := l_all_policies_arr(i).sel;
      out_policy_arr(i).ins            := l_all_policies_arr(i).ins;
      out_policy_arr(i).upd            := l_all_policies_arr(i).upd;
      out_policy_arr(i).del            := l_all_policies_arr(i).del;
      out_policy_arr(i).idx            := l_all_policies_arr(i).idx;
      out_policy_arr(i).chk_option     := l_all_policies_arr(i).chk_option;
      out_policy_arr(i).enable         := l_all_policies_arr(i).enable;
      out_policy_arr(i).static_policy  := l_all_policies_arr(i).static_policy;
      out_policy_arr(i).policy_type    := l_all_policies_arr(i).policy_type;
      out_policy_arr(i).long_predicate := l_all_policies_arr(i).long_predicate;

      SELECT sec_rel_column, column_option
        BULK COLLECT
        INTO out_policy_arr(i).sec_rel_col_arr, l_column_option_arr
        FROM all_sec_relevant_cols
       WHERE object_owner = out_policy_arr(i).object_owner
         AND object_name = out_policy_arr(i).object_name
         AND policy_group = out_policy_arr(i).policy_group
         AND policy_name = out_policy_arr(i).policy_name;

      IF l_column_option_arr.COUNT > 0 THEN
        out_policy_arr(i).column_option := l_column_option_arr(1);
      END IF;
    END LOOP;

    stop_timer;
  END read_table_policies;


  PROCEDURE enable_policies(
    in_policy_arr     IN gt_policy_arr,
    in_enable         IN BOOLEAN DEFAULT TRUE
  )
  AS
    l_sql VARCHAR2(1000); 
  BEGIN
    FOR i IN 1..in_policy_arr.COUNT LOOP
      IF in_enable THEN 
        l_sql := '
begin
  dbms_rls.enable_policy(
    object_schema => :in_object_owner,
    object_name   => :in_object_name,
    policy_name   => :in_policy_name,
    enable        => TRUE
  );
end;';     
      ELSE
        l_sql := '
begin
  dbms_rls.enable_policy(
    object_schema => :in_object_owner,
    object_name   => :in_object_name,
    policy_name   => :in_policy_name,
    enable        => FALSE
  );
end;';
     END IF;
     EXECUTE IMMEDIATE l_sql USING in_policy_arr(i).object_owner, in_policy_arr(i).object_name,in_policy_arr(i).policy_name;    
    END LOOP;
  END enable_policies;


  PROCEDURE read_nested_tables(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_nested_tables_arr   IS TABLE OF all_nested_tables%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_nested_tables_arr        t_all_nested_tables_arr;
  BEGIN
    start_timer;
    SELECT *
      BULK COLLECT
      INTO l_all_nested_tables_arr
      FROM all_nested_tables
     WHERE owner = io_table_rec.owner
       AND parent_table_name = io_table_rec.table_name;

    io_table_rec.nested_tables_arr.DELETE;
    FOR i IN 1..l_all_nested_tables_arr.COUNT LOOP
      io_table_rec.nested_tables_arr(i).owner                 := l_all_nested_tables_arr(i).owner;
      io_table_rec.nested_tables_arr(i).table_name            := l_all_nested_tables_arr(i).table_name;
      io_table_rec.nested_tables_arr(i).table_type_owner      := l_all_nested_tables_arr(i).table_type_owner;
      io_table_rec.nested_tables_arr(i).table_type_name       := l_all_nested_tables_arr(i).table_type_name;
      io_table_rec.nested_tables_arr(i).parent_table_name     := l_all_nested_tables_arr(i).parent_table_name;
      io_table_rec.nested_tables_arr(i).parent_table_column   := l_all_nested_tables_arr(i).parent_table_column;
      io_table_rec.nested_tables_arr(i).storage_spec          := l_all_nested_tables_arr(i).storage_spec;
      io_table_rec.nested_tables_arr(i).return_type           := l_all_nested_tables_arr(i).return_type;
      io_table_rec.nested_tables_arr(i).element_substitutable := trim(l_all_nested_tables_arr(i).element_substitutable);

      io_table_rec.nested_tables_arr(i).rename_rec := get_rename_rec(
                                                        in_object_name  => io_table_rec.nested_tables_arr(i).table_name,
                                                        in_object_owner => io_table_rec.nested_tables_arr(i).owner,
                                                        in_object_type  => 'TABLE'
                                                      );
    END LOOP;

    stop_timer;
  END read_nested_tables;

  -- read table properties and all attributes/dependant objects
  PROCEDURE read_table_cascade(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    in_read_data       IN BOOLEAN,
    out_table_rec      OUT NOCOPY gt_table_rec
  )
  AS
    l_table_rec  gt_table_rec;
  BEGIN
    start_timer;
    -- read table attributes
    read_table(
      in_table_name => in_table_name,
      in_owner      => in_owner,
      in_read_data  => in_read_data,
      out_table_rec => l_table_rec
    );
    -- read columns of existed table
    read_table_columns(l_table_rec);
    -- read all indexes for existing table
    read_table_indexes(l_table_rec);
    -- read all constraints for existing table
    read_table_constraints(l_table_rec);
    -- read all log groups for existing table
    read_table_log_groups(l_table_rec);
    -- read all lobs for existing table
    read_table_lobs(l_table_rec);
    -- read all XML columns for existing table
    read_table_xml_cols(l_table_rec);
    -- read all varray columns for existing table
    read_table_varrays(l_table_rec);
    -- read all reference constraints referencing from other tables to existed table
    read_table_references(l_table_rec);
    -- read all join indexes on other tables joined with existing table
    read_table_join_indexes(l_table_rec);
    -- find all columns included into join indexes
    read_table_triggers(l_table_rec);
    -- read partitions
    read_partitions(l_table_rec, in_read_data);
    -- read subpartitions
    read_subpartitions(l_table_rec, in_read_data);
    -- read nested tables
    read_nested_tables(l_table_rec);

    out_table_rec := l_table_rec;
    stop_timer;
  END read_table_cascade;

  -- read sequence metadata
  PROCEDURE read_sequence(
    in_sequence_name IN VARCHAR2,
    in_owner         IN VARCHAR2,
    out_sequence_rec IN OUT NOCOPY gt_sequence_rec
  )
  AS
    l_all_sequence_rec  all_sequences%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_all_sequence_rec
        FROM all_sequences
       WHERE sequence_owner = in_owner
         AND sequence_name = in_sequence_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;

    out_sequence_rec.owner           := l_all_sequence_rec.sequence_owner;
    out_sequence_rec.sequence_name   := l_all_sequence_rec.sequence_name;
    out_sequence_rec.min_value       := l_all_sequence_rec.min_value;
    out_sequence_rec.max_value       := l_all_sequence_rec.max_value;
    out_sequence_rec.increment_by    := l_all_sequence_rec.increment_by;
    out_sequence_rec.cycle_flag      := l_all_sequence_rec.cycle_flag;
    out_sequence_rec.order_flag      := l_all_sequence_rec.order_flag;
    out_sequence_rec.cache_size      := l_all_sequence_rec.cache_size;
    out_sequence_rec.last_number     := l_all_sequence_rec.last_number;


    out_sequence_rec.rename_rec := get_rename_rec(
                                     in_object_name  => out_sequence_rec.sequence_name,
                                     in_object_owner => out_sequence_rec.owner,
                                     in_object_type  => 'SEQUENCE'
                                   );

  END read_sequence;

  -- read object type metadata
  PROCEDURE read_type(
    in_type_name IN VARCHAR2,
    in_owner     IN VARCHAR2,
    out_type_rec IN OUT NOCOPY gt_type_rec
  )
  AS
    TYPE t_all_type_attr_arr    IS TABLE OF all_type_attrs%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_all_type_method_arr  IS TABLE OF all_type_methods%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_all_method_param_arr IS TABLE OF all_method_params%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_all_dependencies_arr IS TABLE OF all_dependencies%ROWTYPE INDEX BY PLS_INTEGER;

    l_all_type_rec          all_types%ROWTYPE;
    l_all_type_attrs_arr    t_all_type_attr_arr;
    l_all_type_method_arr   t_all_type_method_arr;
    l_all_method_param_arr  t_all_method_param_arr;
    l_indx_arr              arrays.gt_int_arr;
    l_last_indx             PLS_INTEGER;
    l_cnt                   PLS_INTEGER;
    l_indx                  PLS_INTEGER;
    l_table_rec             gt_table_rec;
  BEGIN
    start_timer;
    BEGIN
      SELECT *
        INTO l_all_type_rec
        FROM all_types
       WHERE owner = in_owner
         AND type_name = in_type_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;

    out_type_rec.owner             := l_all_type_rec.owner;
    out_type_rec.type_name         := l_all_type_rec.type_name;
    out_type_rec.type_oid          := l_all_type_rec.type_oid;
    out_type_rec.typecode          := l_all_type_rec.typecode;
    out_type_rec.predefined        := l_all_type_rec.predefined;
    out_type_rec.incomplete        := l_all_type_rec.incomplete;
    out_type_rec.final             := l_all_type_rec.final;
    out_type_rec.instantiable      := l_all_type_rec.instantiable;
    out_type_rec.supertype_owner   := l_all_type_rec.supertype_owner;
    out_type_rec.supertype_name    := l_all_type_rec.supertype_name;
    out_type_rec.local_attributes  := l_all_type_rec.local_attributes;
    out_type_rec.local_methods     := l_all_type_rec.local_methods;
    out_type_rec.typeid            := l_all_type_rec.typeid;
/*
    SELECT text
      BULK COLLECT
      INTO l_text_arr
      FROM all_source
     WHERE type = 'TYPE'
       AND owner = in_owner
       AND name = in_type_name
     ORDER BY line;

    out_type_rec.sql_text := null;
    FOR i in 1..l_text_arr.COUNT LOOP
      out_type_rec.sql_text := out_type_rec.sql_text || l_text_arr(i) || chr(10);
    END LOOP;
    out_type_rec.sql_text := TRIM(chr(10) FROM out_type_rec.sql_text);
*/

    -- read attributes
    SELECT *
      BULK COLLECT
      INTO l_all_type_attrs_arr
      FROM all_type_attrs
     WHERE owner = in_owner
       AND type_name = in_type_name
       AND inherited = 'NO'
     ORDER BY attr_no;

    FOR i IN 1..l_all_type_attrs_arr.COUNT LOOP
      out_type_rec.attribute_ind_arr(l_all_type_attrs_arr(i).attr_name) := i;
      out_type_rec.attribute_arr(i).owner              := l_all_type_attrs_arr(i).owner;
      out_type_rec.attribute_arr(i).type_name          := l_all_type_attrs_arr(i).type_name;
      out_type_rec.attribute_arr(i).attr_name          := l_all_type_attrs_arr(i).attr_name;
      out_type_rec.attribute_arr(i).attr_type_mod      := l_all_type_attrs_arr(i).attr_type_mod;
      out_type_rec.attribute_arr(i).attr_type_owner    := l_all_type_attrs_arr(i).attr_type_owner;
      out_type_rec.attribute_arr(i).attr_type_name     := l_all_type_attrs_arr(i).attr_type_name;
      out_type_rec.attribute_arr(i).length             := l_all_type_attrs_arr(i).length;
      out_type_rec.attribute_arr(i).precision          := l_all_type_attrs_arr(i).precision;
      out_type_rec.attribute_arr(i).scale              := l_all_type_attrs_arr(i).scale;
      out_type_rec.attribute_arr(i).character_set_name := l_all_type_attrs_arr(i).character_set_name;
      out_type_rec.attribute_arr(i).attr_no            := l_all_type_attrs_arr(i).attr_no;
      out_type_rec.attribute_arr(i).inherited          := l_all_type_attrs_arr(i).inherited;
      $IF dbms_db_version.version >= 11 $THEN
      out_type_rec.attribute_arr(i).char_used          := CASE l_all_type_attrs_arr(i).char_used WHEN 'B' THEN 'BYTE' WHEN 'C' THEN 'CHAR' END;
      $END
    END LOOP;

    -- read methods
    SELECT *
      BULK COLLECT
      INTO l_all_type_method_arr
      FROM all_type_methods
     WHERE owner = in_owner
       AND type_name = in_type_name
       AND inherited = 'NO'
     ORDER BY method_no;

    -- read method params
    SELECT *
      BULK COLLECT
      INTO l_all_method_param_arr
      FROM all_method_params
     WHERE owner = in_owner
       AND type_name = in_type_name
     ORDER BY method_no, param_no;

    l_last_indx := 0;

    FOR i IN 1..l_all_type_method_arr.COUNT LOOP
      out_type_rec.method_arr(i).owner         := l_all_type_method_arr(i).owner;
      out_type_rec.method_arr(i).type_name     := l_all_type_method_arr(i).type_name;
      out_type_rec.method_arr(i).method_name   := cort_parse_pkg.get_original_name(
                                                    in_object_type => 'METHOD',
                                                    in_object_name => l_all_type_method_arr(i).method_name
                                                  );
      out_type_rec.method_arr(i).method_no    := l_all_type_method_arr(i).method_no;
      out_type_rec.method_arr(i).method_type  := cort_parse_pkg.get_original_name(
                                                    in_object_type => 'TYPE',
                                                    in_object_name => l_all_type_method_arr(i).method_type
                                                  );
      out_type_rec.method_arr(i).final        := l_all_type_method_arr(i).final;
      out_type_rec.method_arr(i).instantiable := l_all_type_method_arr(i).instantiable;
      out_type_rec.method_arr(i).overriding   := l_all_type_method_arr(i).overriding;
      out_type_rec.method_arr(i).inherited    := l_all_type_method_arr(i).inherited;
      out_type_rec.method_arr(i).constructor  := CASE WHEN l_all_type_method_arr(i).method_name = l_all_type_method_arr(i).type_name THEN 'YES' ELSE 'NO' END;
      out_type_rec.method_arr(i).static       := CASE WHEN l_all_type_method_arr(i).parameters = 0 THEN 'YES' ELSE 'NO' END;

      IF l_all_method_param_arr.COUNT > 0 THEN
        FOR j in l_last_indx+1..l_all_method_param_arr.COUNT LOOP
          IF l_all_method_param_arr(j).method_name = l_all_type_method_arr(i).method_name AND
             l_all_method_param_arr(j).method_no = l_all_type_method_arr(i).method_no
          THEN
            l_indx := l_all_method_param_arr(j).param_no;
            out_type_rec.method_arr(i).parameter_arr(l_indx).owner              := l_all_method_param_arr(j).owner;
            out_type_rec.method_arr(i).parameter_arr(l_indx).type_name          := cort_parse_pkg.get_original_name(
                                                                                     in_object_type => 'TYPE',
                                                                                     in_object_name => l_all_method_param_arr(j).type_name
                                                                                   );
            out_type_rec.method_arr(i).parameter_arr(l_indx).method_name        := cort_parse_pkg.get_original_name(
                                                                                     in_object_type => 'METHOD',
                                                                                     in_object_name => l_all_method_param_arr(j).method_name
                                                                                   );
            out_type_rec.method_arr(i).parameter_arr(l_indx).method_no          := l_all_method_param_arr(j).method_no;
            out_type_rec.method_arr(i).parameter_arr(l_indx).param_name         := l_all_method_param_arr(j).param_name;
            out_type_rec.method_arr(i).parameter_arr(l_indx).param_no           := l_all_method_param_arr(j).param_no;
            out_type_rec.method_arr(i).parameter_arr(l_indx).param_mode         := l_all_method_param_arr(j).param_mode;
            out_type_rec.method_arr(i).parameter_arr(l_indx).param_type_mod     := l_all_method_param_arr(j).param_type_mod;
            out_type_rec.method_arr(i).parameter_arr(l_indx).param_type_owner   := l_all_method_param_arr(j).param_type_owner;
            out_type_rec.method_arr(i).parameter_arr(l_indx).param_type_name    := l_all_method_param_arr(j).param_type_name;
            out_type_rec.method_arr(i).parameter_arr(l_indx).character_set_name := l_all_method_param_arr(j).character_set_name;

            out_type_rec.method_arr(i).parameter_arr(l_indx).param_type_name :=
              CASE out_type_rec.method_arr(i).parameter_arr(l_indx).param_type_name
                WHEN 'PL/SQL PLS INTEGER'    THEN 'PLS_INTEGER'
                WHEN 'PL/SQL BINARY INTEGER' THEN 'BINARY_INTEGER'
                WHEN 'PL/SQL BOOLEAN'        THEN 'BOOLEAN'
                WHEN 'PL/SQL REF CURSOR'     THEN 'SYS_REFCURSOR'
                WHEN 'PL/SQL LONG RAW'       THEN 'LONG RAW'
                ELSE cort_parse_pkg.get_original_name(
                       in_object_type => 'TYPE',
                       in_object_name => out_type_rec.method_arr(i).parameter_arr(l_indx).param_type_name
                     )
              END;

            IF l_all_method_param_arr(j).param_no = 1 THEN
              IF l_all_method_param_arr(j).param_name = 'SELF' AND
                 l_all_method_param_arr(j).param_type_owner = l_all_method_param_arr(j).owner AND
                 l_all_method_param_arr(j).param_type_name = l_all_method_param_arr(j).type_name
              THEN
                out_type_rec.method_arr(i).static := 'NO';
              ELSE
                out_type_rec.method_arr(i).static := 'YES';
              END IF;
            END IF;

            l_last_indx := j;
          ELSIF l_all_method_param_arr(j).method_no < out_type_rec.method_arr(i).method_no THEN
            l_last_indx := j;
          ELSE
            EXIT;
          END IF;
        END LOOP;
      END IF;

      IF l_all_type_method_arr(i).results = 1 THEN
        -- read method result
        SELECT owner, type_name, method_name, method_no, null as param_name, null as param_no, null as param_mode,
               result_type_mod as param_type_mod, result_type_owner as param_type_owner, result_type_name as param_type_name, character_set_name
          INTO out_type_rec.method_arr(i).result_rec
          FROM all_method_results
         WHERE owner = in_owner
           AND type_name = in_type_name
           AND method_name = l_all_type_method_arr(i).method_name
           AND method_no = l_all_type_method_arr(i).method_no;
        out_type_rec.method_arr(i).result_rec.method_name := cort_parse_pkg.get_original_name(
                                                               in_object_type => 'TYPE',
                                                               in_object_name => out_type_rec.method_arr(i).result_rec.type_name
                                                             );
        out_type_rec.method_arr(i).result_rec.method_name := cort_parse_pkg.get_original_name(
                                                               in_object_type => 'METHOD',
                                                               in_object_name => out_type_rec.method_arr(i).result_rec.method_name
                                                             );
        out_type_rec.method_arr(i).result_rec.param_type_name :=
          CASE out_type_rec.method_arr(i).result_rec.param_type_name
            WHEN 'PL/SQL PLS INTEGER'    THEN 'PLS_INTEGER'
            WHEN 'PL/SQL BINARY INTEGER' THEN 'BINARY_INTEGER'
            WHEN 'PL/SQL BOOLEAN'        THEN 'BOOLEAN'
            WHEN 'PL/SQL REF CURSOR'     THEN 'SYS_REFCURSOR'
            WHEN 'PL/SQL LONG RAW'       THEN 'LONG RAW'
            ELSE cort_parse_pkg.get_original_name(
                   in_object_type => 'TYPE',
                   in_object_name => out_type_rec.method_arr(i).result_rec.param_type_name
                 )
          END;
      END IF;

      IF out_type_rec.method_ind_arr.EXISTS(out_type_rec.method_arr(i).method_name) THEN
        l_indx_arr := out_type_rec.method_ind_arr(out_type_rec.method_arr(i).method_name);
      ELSE
        l_indx_arr.DELETE;
      END IF;
      l_indx_arr(l_indx_arr.COUNT + 1) := i;
      out_type_rec.method_ind_arr(out_type_rec.method_arr(i).method_name) := l_indx_arr;
    END LOOP;

    -- read dependencies

    SELECT *
      BULK COLLECT
      INTO out_type_rec.dependency_arr
      FROM (SELECT DISTINCT d.owner, d.name, d.type,
                   (SELECT max(object_id) FROM all_objects a
                     WHERE a.object_type = d.type
                       AND a.owner = d.owner
                       AND a.object_name = d.name
                       AND status = 'VALID') as object_id
              FROM all_dependencies d
             WHERE type = 'TABLE'
             CONNECT BY referenced_owner = PRIOR d.owner
                    AND referenced_name = PRIOR d.name
                    AND referenced_type = PRIOR d.type
             START WITH referenced_owner = in_owner
                    AND referenced_name =  in_type_name
                    AND referenced_type = 'TYPE'
            )
      WHERE OBJECT_ID IS NOT NULL;

    out_type_rec.table_dependency := FALSE;
    FOR i IN 1..out_type_rec.dependency_arr.COUNT LOOP
      IF out_type_rec.dependency_arr(i).type = 'TABLE' THEN
        out_type_rec.table_dependency := TRUE;

        read_table(
          in_table_name      => out_type_rec.dependency_arr(i).name,
          in_owner           => out_type_rec.dependency_arr(i).owner,
          in_read_data       => FALSE,
          out_table_rec      => l_table_rec
        );

        -- find part key columns
        FOR j IN 1..l_table_rec.part_key_column_arr.COUNT LOOP
          IF out_type_rec.attribute_ind_arr.EXISTS(l_table_rec.part_key_column_arr(j)) THEN
            l_indx := out_type_rec.attribute_ind_arr(l_table_rec.part_key_column_arr(j));
            out_type_rec.attribute_arr(l_indx).partition_key := TRUE;
          END IF;
        END LOOP;

        -- find part key columns
        FOR j IN 1..l_table_rec.subpart_key_column_arr.COUNT LOOP
          IF out_type_rec.attribute_ind_arr.EXISTS(l_table_rec.subpart_key_column_arr(j)) THEN
            l_indx := out_type_rec.attribute_ind_arr(l_table_rec.subpart_key_column_arr(j));
            out_type_rec.attribute_arr(l_indx).partition_key := TRUE;
          END IF;
        END LOOP;

        -- find IOT key columns
        FOR j IN 1..l_table_rec.iot_pk_column_arr.COUNT LOOP
          IF out_type_rec.attribute_ind_arr.EXISTS(l_table_rec.iot_pk_column_arr(j)) THEN
            l_indx := out_type_rec.attribute_ind_arr(l_table_rec.iot_pk_column_arr(j));
            out_type_rec.attribute_arr(l_indx).iot_primary_key := TRUE;
          END IF;
        END LOOP;

        -- find cluster key columns
        FOR j IN 1..l_table_rec.cluster_column_arr.COUNT LOOP
          IF out_type_rec.attribute_ind_arr.EXISTS(l_table_rec.cluster_column_arr(j)) THEN
            l_indx := out_type_rec.attribute_ind_arr(l_table_rec.cluster_column_arr(j));
            out_type_rec.attribute_arr(l_indx).cluster_key := TRUE;
          END IF;
        END LOOP;

      END IF;
    END LOOP;


/*
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_dependencies d
     INNER JOIN all_objects a
        ON a.object_type = d.type
       AND a.owner = d.owner
       AND a.object_name = d.name
     WHERE referenced_owner = in_owner
       AND referenced_name = in_type_name
       AND referenced_type = 'TYPE'
       AND TYPE = 'TABLE';
    out_type_rec.table_dependency := l_cnt > 0;
*/

    BEGIN
      SELECT 1
        INTO l_cnt
        FROM all_types
       WHERE supertype_owner = in_owner
         AND supertype_name = in_type_name
         AND ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;
    out_type_rec.subtype_dependency := l_cnt = 1;

    out_type_rec.rename_rec := get_rename_rec(
                                     in_object_name  => out_type_rec.type_name,
                                     in_object_owner => out_type_rec.owner,
                                     in_object_type  => 'TYPE'
                                   );

    stop_timer;
  END read_type;

  -- read view metadata
  PROCEDURE read_view(
    in_view_name       IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_view_rec       OUT NOCOPY gt_view_rec
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_all_view_rec        all_views%ROWTYPE;
    l_view_text           CLOB;
  BEGIN
    BEGIN
      SELECT *
        INTO l_all_view_rec
        FROM all_views
       WHERE owner = in_owner
         AND view_name = in_view_name;

      -- convert LONG into LOB to get full view text
      -- cort_lob will be deleted on commit
      INSERT INTO cort_lob
      SELECT to_lob(text)
        FROM all_views
       WHERE owner = in_owner
         AND view_name = in_view_name;

      -- cort_lob has only 1 row as it is temporary and in autonomous transaction
      SELECT text
        INTO l_view_text
        FROM cort_lob;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;


    out_view_rec.owner             := l_all_view_rec.owner;
    out_view_rec.view_name         := l_all_view_rec.view_name;
    out_view_rec.type_text         := l_all_view_rec.type_text;
    out_view_rec.oid_text          := l_all_view_rec.oid_text;
    out_view_rec.view_type_owner   := l_all_view_rec.view_type_owner;
    out_view_rec.view_type         := l_all_view_rec.view_type;
    out_view_rec.superview_name    := l_all_view_rec.superview_name;
    $IF dbms_db_version.version >= 11 $THEN
    out_view_rec.editioning_view   := l_all_view_rec.editioning_view;
    out_view_rec.read_only         := l_all_view_rec.read_only;
    $END

    dbms_lob.createtemporary(out_view_rec.view_text, TRUE, dbms_lob.transaction);
    dbms_lob.copy(out_view_rec.view_text, l_view_text, l_all_view_rec.text_length);

    -- read_columns
    SELECT column_name
      BULK COLLECT
      INTO out_view_rec.columns_arr
      FROM all_tab_cols
     WHERE owner = in_owner
       AND table_name = in_view_name
     ORDER BY internal_column_id;


    COMMIT; -- to end  autonomous transaction
  END read_view;

  -- renames table and table's indexes, named constraints, named log groups, triggers and external references
  PROCEDURE rename_table_cascade(
    io_table_rec     IN OUT NOCOPY gt_table_rec,
    in_rename_mode   IN VARCHAR2,
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_indx PLS_INTEGER;
  BEGIN
    FOR i IN 1..io_table_rec.index_arr.COUNT LOOP
      IF io_table_rec.index_arr(i).rename_rec.generated = 'N' THEN
        IF object_exists(
             in_rename_rec        => io_table_rec.index_arr(i).rename_rec
           )
        THEN
          cort_comp_pkg.rename_object(
            in_rename_mode   => in_rename_mode,
            io_rename_rec    => io_table_rec.index_arr(i).rename_rec,
            io_frwd_stmt_arr => io_frwd_stmt_arr,
            io_rlbk_stmt_arr => io_rlbk_stmt_arr
          );
        END IF;
      END IF;
    END LOOP;
    FOR i IN 1..io_table_rec.constraint_arr.COUNT LOOP
      IF io_table_rec.constraint_arr(i).generated = 'USER NAME' THEN
        IF constraint_exists(
             in_cons_name   => io_table_rec.constraint_arr(i).rename_rec.current_name,
             in_owner       => io_table_rec.constraint_arr(i).owner
           )
        THEN
          cort_comp_pkg.rename_object(
            in_rename_mode   => in_rename_mode,
            io_rename_rec    => io_table_rec.constraint_arr(i).rename_rec,
            io_frwd_stmt_arr => io_frwd_stmt_arr,
            io_rlbk_stmt_arr => io_rlbk_stmt_arr
          );
        END IF;
      END IF;
    END LOOP;
    FOR i IN 1..io_table_rec.log_group_arr.COUNT LOOP
      IF io_table_rec.log_group_arr(i).generated = 'USER NAME' THEN
        IF constraint_exists(
             in_cons_name   => io_table_rec.log_group_arr(i).rename_rec.current_name,
             in_owner       => io_table_rec.log_group_arr(i).owner
           )
        THEN
          cort_comp_pkg.rename_object(
            in_rename_mode   => in_rename_mode,
            io_rename_rec    => io_table_rec.log_group_arr(i).rename_rec,
            io_frwd_stmt_arr => io_frwd_stmt_arr,
            io_rlbk_stmt_arr => io_rlbk_stmt_arr
          );
        END IF;
      END IF;
    END LOOP;

    l_indx := io_table_rec.lob_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF io_table_rec.lob_arr(l_indx).rename_rec.generated = 'N' THEN
        cort_comp_pkg.rename_object(
          in_rename_mode   => in_rename_mode,
          io_rename_rec    => io_table_rec.lob_arr(l_indx).rename_rec,
          io_frwd_stmt_arr => io_frwd_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_stmt_arr
        );
      END IF;
      l_indx := io_table_rec.lob_arr.NEXT(l_indx);
    END LOOP;

    FOR i IN 1..io_table_rec.nested_tables_arr.COUNT LOOP
      IF object_exists(
           in_rename_rec        => io_table_rec.nested_tables_arr(i).rename_rec
         )
      THEN
        cort_comp_pkg.rename_object(
          in_rename_mode   => in_rename_mode,
          io_rename_rec    => io_table_rec.nested_tables_arr(i).rename_rec,
          io_frwd_stmt_arr => io_frwd_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_stmt_arr
        );
      END IF;
    END LOOP;

    IF object_exists(
         in_object_type  => 'TABLE',
         in_object_name  => io_table_rec.rename_rec.current_name,
         in_object_owner => io_table_rec.owner
       )
    THEN
      cort_comp_pkg.rename_object(
        in_rename_mode   => in_rename_mode,
        io_rename_rec    => io_table_rec.rename_rec,
        io_frwd_stmt_arr => io_frwd_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_stmt_arr
      );
    END IF;
  END rename_table_cascade;

  -- finds all check constraint that was implicitly created by NOT NULL column constraint and mark their type as 'N'
  PROCEDURE mark_notnull_constraints(
    io_constraint_arr IN OUT NOCOPY gt_constraint_arr,
    io_column_arr     IN OUT NOCOPY gt_column_arr
  )
  AS
    l_column_indx   arrays.gt_int_indx;
    l_indx          PLS_INTEGER;
  BEGIN
    -- bild index
    FOR i IN 1..io_column_arr.COUNT LOOP
      l_column_indx(io_column_arr(i).column_name) := i;
    END LOOP;

    -- find all check constraints
    FOR i IN 1..io_constraint_arr.COUNT LOOP
      IF io_constraint_arr(i).constraint_type = 'C' AND
         io_constraint_arr(i).column_arr.COUNT = 1 AND
         io_constraint_arr(i).search_condition = '"'||io_constraint_arr(i).column_arr(1)||'" IS NOT NULL' AND
         l_column_indx.EXISTS(io_constraint_arr(i).column_arr(1))
      THEN
        l_indx := l_column_indx(io_constraint_arr(i).column_arr(1));
        IF io_column_arr(l_indx).nullable = 'N' THEN
          io_constraint_arr(i).constraint_type := 'N';
          debug(io_constraint_arr(i).constraint_name||' ('||io_constraint_arr(i).column_arr(1)||') is NOT NULL constraint');
          IF io_constraint_arr(i).generated = 'USER NAME' THEN
            io_column_arr(l_indx).notnull_constraint_name := io_constraint_arr(i).constraint_name;
          END IF;
        END IF;
      END IF;

      IF io_constraint_arr(i).constraint_type = 'F' AND
         io_constraint_arr(i).column_arr.COUNT > 1
      THEN
        FOR j IN 1..io_constraint_arr(i).column_arr.COUNT LOOP
          IF io_constraint_arr(i).search_condition = '"'||io_constraint_arr(i).column_arr(j)||'" IS NOT NULL' AND
             l_column_indx.EXISTS(io_constraint_arr(i).column_arr(j))
          THEN
            l_indx := l_column_indx(io_constraint_arr(i).column_arr(j));
            IF io_column_arr(l_indx).nullable = 'N' THEN
              io_constraint_arr(i).constraint_type := 'N';
              debug(io_constraint_arr(i).constraint_name||' is NOT NULL constraint');
              IF io_constraint_arr(i).generated = 'USER NAME' THEN
                io_column_arr(l_indx).notnull_constraint_name := io_constraint_arr(i).constraint_name;
              END IF;
            END IF;
          END IF;
        END LOOP;
      END IF;
    END LOOP;
  END mark_notnull_constraints;

  -- update triggers definition - include table name in double quotes - workaround for Oracle bug
  PROCEDURE update_triggers(
    in_table_rec IN gt_table_rec
  )
  AS
    l_frwd_alter_stmt_arr      arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr      arrays.gt_clob_arr;
  BEGIN
    FOR i IN 1..in_table_rec.trigger_arr.COUNT LOOP
      cort_comp_pkg.update_trigger(
        in_table_rec           => in_table_rec,
        in_trigger_rec         => in_table_rec.trigger_arr(i),
        io_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
        io_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr
      );
    END LOOP;
    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr
    );
  END update_triggers;

  -- copy table's and columns' comments
  PROCEDURE copy_comments(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_col_arr         arrays.gt_name_arr;
    l_col_comment_arr arrays.gt_lstr_arr;
    l_ddl             VARCHAR2(4200);
    l_col_name        arrays.gt_name;
    l_indx            PLS_INTEGER;
  BEGIN
    IF in_source_table_rec.tab_comment IS NOT NULL THEN
      l_ddl := 'COMMENT ON TABLE "'||in_target_table_rec.owner||'"."'||in_target_table_rec.table_name||'" IS Q''{'||in_source_table_rec.tab_comment||'}''';
      cort_comp_pkg.add_stmt(
        io_frwd_stmt_arr => io_frwd_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_stmt_arr,
        in_frwd_stmt     => l_ddl,
        in_rlbk_stmt     => NULL
      );
    END IF;
    SELECT column_name, comments
      BULK COLLECT
      INTO l_col_arr, l_col_comment_arr
      FROM all_col_comments
     WHERE owner = in_source_table_rec.owner
       AND table_name = in_source_table_rec.table_name
       AND comments IS NOT NULL;
     FOR i IN 1..l_col_comment_arr.COUNT LOOP
       l_col_name := l_col_arr(i);
       IF in_source_table_rec.column_indx_arr.EXISTS(l_col_name) THEN
         l_indx := in_source_table_rec.column_indx_arr(l_col_name);
         l_col_name := NVL(in_source_table_rec.column_arr(l_indx).new_column_name, in_source_table_rec.column_arr(l_indx).column_name);
         IF in_target_table_rec.column_indx_arr.EXISTS(l_col_name) THEN
           l_ddl := 'COMMENT ON COLUMN "'||in_target_table_rec.owner||'"."'||in_target_table_rec.table_name||'"."'||l_col_name||'" IS Q''{'||l_col_comment_arr(i)||'}''';
           cort_comp_pkg.add_stmt(
             io_frwd_stmt_arr => io_frwd_stmt_arr,
             io_rlbk_stmt_arr => io_rlbk_stmt_arr,
             in_frwd_stmt     => l_ddl,
             in_rlbk_stmt     => NULL
           );
         END IF;
       END IF;
     END LOOP;
  END copy_comments;

  -- change comment on source table
  PROCEDURE comment_table(
    in_table_rec     IN gt_table_rec,
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    cort_comp_pkg.add_stmt(
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr,
      in_frwd_stmt     => 'COMMENT ON TABLE "'||in_table_rec.owner||'"."'||in_table_rec.table_name||'" IS Q''{'||in_table_rec.table_name||'}''',
      in_rlbk_stmt     => 'COMMENT ON TABLE "'||in_table_rec.owner||'"."'||in_table_rec.table_name||'" IS Q''{'||in_table_rec.tab_comment||'}'''
    );
  END comment_table;

  -- copy table's privileges
  PROCEDURE copy_privileges(
    io_source_table_rec IN OUT NOCOPY gt_table_rec,
    io_target_table_rec IN OUT NOCOPY gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    read_privileges(
      in_owner         => io_source_table_rec.owner,
      in_table_name    => io_source_table_rec.table_name,
      io_privilege_arr => io_source_table_rec.privilege_arr
    );
    cort_comp_pkg.copy_privileges(
      in_source_table_rec      => io_source_table_rec,
      io_target_table_rec      => io_target_table_rec,
      io_frwd_alter_stmt_arr   => io_frwd_stmt_arr,
      io_rlbk_alter_stmt_arr   => io_rlbk_stmt_arr
    );
  END copy_privileges;

  -- copy sequence's privileges
  PROCEDURE copy_privileges(
    io_source_sequence_rec IN OUT NOCOPY gt_sequence_rec,
    io_target_sequence_rec IN OUT NOCOPY gt_sequence_rec,
    io_frwd_stmt_arr       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr       IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    read_privileges(
      in_owner         => io_source_sequence_rec.owner,
      in_table_name    => io_source_sequence_rec.sequence_name,
      io_privilege_arr => io_source_sequence_rec.privilege_arr
    );
    io_target_sequence_rec.privilege_arr := io_source_sequence_rec.privilege_arr;

    FOR i IN 1..io_target_sequence_rec.privilege_arr.COUNT LOOP
      io_target_sequence_rec.privilege_arr(i).table_name := io_target_sequence_rec.sequence_name;
    END LOOP;

    cort_comp_pkg.get_privileges_stmt(
      in_privilege_arr         => io_target_sequence_rec.privilege_arr,
      io_frwd_alter_stmt_arr   => io_frwd_stmt_arr,
      io_rlbk_alter_stmt_arr   => io_rlbk_stmt_arr
    );

  END copy_privileges;


  -- Disable/Enable all FK before/after data load
  PROCEDURE change_status_for_all_fk(
    in_table_rec     IN gt_table_rec,
    in_action        IN VARCHAR2,  -- DISABLE/ENABLE
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_validate VARCHAR2(10);
    l_sql      VARCHAR2(1000);
  BEGIN
    IF in_action NOT IN ('ENABLE','DISABLE') THEN
      RETURN;
    END IF;

    FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
      IF in_table_rec.constraint_arr(i).constraint_type = 'R' AND
         in_table_rec.constraint_arr(i).status = 'ENABLED'
      THEN
        IF in_action = 'ENABLE' THEN
          IF NOT g_params.validate.get_bool_value THEN
            l_validate := 'NOVALIDATE';
          ELSE
            l_validate := NULL;
          END IF;
        END IF;
        l_sql := 'ALTER TABLE "'||in_table_rec.owner||'"."'||in_table_rec.table_name||'" '||
                 in_action||' '||l_validate||' CONSTRAINT "'||in_table_rec.constraint_arr(i).constraint_name||'"';
        cort_comp_pkg.add_stmt(
          io_frwd_stmt_arr => io_frwd_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_stmt_arr,
          in_frwd_stmt     => l_sql,
          in_rlbk_stmt     => NULL
        );
      END IF;
    END LOOP;
  END change_status_for_all_fk;

  PROCEDURE change_status_for_policies(
    in_table_rec     IN gt_table_rec,
    in_action        IN VARCHAR2,  -- DISABLE/ENABLE
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_frwd_sql VARCHAR2(1000);
    l_rlbk_sql VARCHAR2(1000);
    l_enable   VARCHAR2(10);
  BEGIN
    CASE in_action
      WHEN 'ENABLE' THEN l_enable := 'TRUE';
      WHEN 'DISABLE' THEN l_enable := 'FALSE';
      ELSE RETURN;
    END CASE;

    FOR i IN 1..in_table_rec.policy_arr.COUNT LOOP
      l_frwd_sql := '
begin
  dbms_rls.enable_policy(
    object_schema => ''"'||in_table_rec.policy_arr(i).object_owner||'"'',
    object_name   => ''"'||in_table_rec.policy_arr(i).object_name||'"'',
    policy_name   => ''"'||in_table_rec.policy_arr(i).policy_name||'"'',
    enable        => '||l_enable||'
  );
end;';
      l_rlbk_sql := '
begin
  dbms_rls.enable_policy(
    object_schema => ''"'||in_table_rec.policy_arr(i).object_owner||'"'',
    object_name   => ''"'||in_table_rec.policy_arr(i).object_name||'"'',
    policy_name   => ''"'||in_table_rec.policy_arr(i).policy_name||'"'',
    enable        => NOT '||l_enable||'
  );
end;';
      cort_comp_pkg.add_stmt(
        io_frwd_stmt_arr => io_frwd_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_stmt_arr,
        in_frwd_stmt     => l_frwd_sql,
        in_rlbk_stmt     => l_rlbk_sql
      );
    END LOOP;
  END change_status_for_policies;

  PROCEDURE get_copy_data_sql(
    in_source_table_rec     IN gt_table_rec,
    in_target_table_rec     IN gt_table_rec,
    in_source_partition_rec IN gt_partition_rec,
    in_target_partition_rec IN gt_partition_rec,
    in_column_list          IN CLOB,
    in_values_list          IN CLOB,
    in_filter_sql           IN CLOB,
    io_sql_arr              IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_source_partition_clause  VARCHAR2(100);
    l_target_partition_clause  VARCHAR2(100);
    l_sql                      CLOB;
    l_append                   VARCHAR2(100);
    l_parallel                 VARCHAR2(100);
  BEGIN
    -- partitioned table
    IF in_source_partition_rec.partition_name IS NOT NULL THEN
      l_source_partition_clause := in_source_partition_rec.partition_level||' ("'||in_source_partition_rec.partition_name||'")';
      IF in_target_partition_rec.partition_name IS NOT NULL THEN
        l_target_partition_clause := in_target_partition_rec.partition_level||' ("'||in_target_partition_rec.partition_name||'")';
        l_append := 'APPEND';
      END IF;
      $IF cort_options_pkg.gc_threading $THEN
        -- in case of threading we can use append only when data are copied from partition to partition to prevent blocking
        IF g_params.threading.get_value = 'NONE' THEN
          l_append := 'APPEND';
        END IF;
      $ELSE
        l_append := 'APPEND';
      $END
    ELSE
      -- non partitioned table
      l_source_partition_clause := NULL;
      l_target_partition_clause := NULL;
      l_append := 'APPEND';
    END IF;

    IF g_params.parallel.get_num_value > 1 THEN
      l_parallel := 'PARALLEL ('||g_params.parallel.get_value||')';
    END IF;

    IF g_params.test.get_bool_value AND NOT g_params.debug.get_bool_value AND io_sql_arr.count > 1 THEN
      -- short version of SQL for output only
      l_sql :=
      'INSERT /*+ '||l_append||' '||l_parallel||' */'||chr(10)||
      '  INTO "'||in_target_table_rec.owner||'"."'||in_target_table_rec.rename_rec.current_name||'" '||l_target_partition_clause||' (...)'||chr(10)||
      'SELECT /*+ '||l_parallel||' full('||g_params.alias.get_value||') */ ...'||chr(10)||
      '  FROM "'||in_source_table_rec.owner||'"."'||in_source_table_rec.rename_rec.current_name||'" '||l_source_partition_clause||' '||g_params.alias.get_value||chr(10)||
      case when length(in_filter_sql) > 100 then substr(in_filter_sql, 1, 97)||'...' else in_filter_sql end;
    ELSE
      -- copy data
      l_sql :=
      'INSERT /*+ '||l_append||' '||l_parallel||' */'||chr(10)||
      '  INTO "'||in_target_table_rec.owner||'"."'||in_target_table_rec.rename_rec.current_name||'" '||l_target_partition_clause||chr(10)||
      '      ('||in_column_list||')'||chr(10)||
      'SELECT /*+ '||l_parallel||' full('||g_params.alias.get_value||') */'||chr(10)||
              in_values_list||chr(10)||
      '  FROM "'||in_source_table_rec.owner||'"."'||in_source_table_rec.rename_rec.current_name||'" '||l_source_partition_clause||' '||g_params.alias.get_value||chr(10)||
      in_filter_sql;
    END IF;

    $IF cort_options_pkg.gc_threading $THEN
    IF in_source_partition_rec.partition_name IS NOT NULL THEN
      cort_thread_exec_pkg.make_sql_threadable(
        io_sql => l_sql
      );
    END IF;
    $END
    io_sql_arr(io_sql_arr.count+1) := l_sql;

  END get_copy_data_sql;

  PROCEDURE get_online_copy_data_sql(
    in_source_table_rec     IN gt_table_rec,
    in_target_table_rec     IN gt_table_rec,
    in_source_partition_rec IN gt_partition_rec,
    in_target_partition_rec IN gt_partition_rec,
    in_column_list          IN CLOB,
    in_filter_sql           IN CLOB,
    io_sql_arr              IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_source_partition_clause  VARCHAR2(100);
    l_target_partition_clause  VARCHAR2(100);
    l_sql                      CLOB;
    l_append                   VARCHAR2(100);
    l_parallel                 VARCHAR2(100);
  BEGIN

    l_sql := q'{
    DECLARE
      CURSOR l_cur IS
      SELECT * FROM

    BEGIN
    END;}';

  END get_online_copy_data_sql;

  PROCEDURE get_stats_sql(
    in_source_table_rec     IN gt_table_rec,
    in_target_table_rec     IN gt_table_rec,
    in_source_partition_rec IN gt_partition_rec,
    in_target_partition_rec IN gt_partition_rec,
    io_sql_arr              IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_sql      CLOB;
  BEGIN
    IF g_params.stats.get_value = 'COPY' AND in_target_partition_rec.partition_name IS NOT NULL AND in_source_partition_rec.partition_name IS NOT NULL THEN
      l_sql := '
begin
  cort_exec_pkg.copy_stats(
    in_owner        => '''||in_source_table_rec.owner||''',
    in_source_table => '''||in_source_table_rec.rename_rec.current_name||''',
    in_target_table => '''||in_target_table_rec.rename_rec.current_name||''',
    in_source_part  => '''||in_source_partition_rec.partition_name||''',
    in_target_part  => '''||in_target_partition_rec.partition_name||''',
    in_part_level   => '''||in_target_partition_rec.partition_level||'''
  );
end;';
      $IF cort_options_pkg.gc_threading $THEN
        cort_thread_exec_pkg.make_sql_threadable(
          io_sql => l_sql
        );
      $END
      io_sql_arr(io_sql_arr.count+1) := l_sql;
    ELSIF g_params.stats.get_value = 'COPY' AND in_target_partition_rec.partition_name IS NULL AND in_source_partition_rec.partition_name IS NULL THEN
      l_sql := '
begin
  cort_exec_pkg.copy_stats(
    in_owner        => '''||in_source_table_rec.owner||''',
    in_source_table => '''||in_source_table_rec.rename_rec.current_name||''',
    in_target_table => '''||in_target_table_rec.rename_rec.current_name||'''
  );
end;';
    ELSIF g_params.stats.get_value IN ('APPROX_GLOBAL AND PARTITION',in_target_partition_rec.partition_level) AND in_target_partition_rec.partition_name IS NOT NULL THEN
      l_sql := '
begin
  dbms_stats.gather_table_stats(
    ownname          => ''"'||in_target_table_rec.owner||'"'',
    tabname          => ''"'||in_target_table_rec.rename_rec.current_name||'"'',
    partname         => ''"'||in_target_partition_rec.partition_name||'"'',
    granularity      => '''||g_params.stats.get_value||''',
    estimate_percent => '||NVL(g_params.stats_estimate_pct.get_num_value, dbms_stats.default_estimate_percent)||',
    method_opt       => '||NVL(SUBSTR(g_params.stats_method_opt.get_value, 2, LENGTH(g_params.stats_method_opt.get_value)-2), 'dbms_stats.default_method_opt')||',
    degree           => '||NVL(g_params.parallel.get_num_value, dbms_stats.default_degree_value)||',
    cascade          => true,
    no_invalidate    => false
  );
end;';
      $IF cort_options_pkg.gc_threading $THEN
        cort_thread_exec_pkg.make_sql_threadable(
          io_sql => l_sql
        );
      $END
      io_sql_arr(io_sql_arr.count+1) := l_sql;
    ELSIF g_params.stats.get_value IN ('ALL','AUTO','GLOBAL','GLOBAL AND PARTITION') AND in_target_partition_rec.partition_name IS NULL THEN
      l_sql := '
begin
  dbms_stats.gather_table_stats(
    ownname          => ''"'||in_target_table_rec.owner||'"'',
    tabname          => ''"'||in_target_table_rec.rename_rec.current_name||'"'',
    granularity      => '''||g_params.stats.get_value||''',
    estimate_percent => '||NVL(g_params.stats_estimate_pct.get_num_value, dbms_stats.default_estimate_percent)||',
    method_opt       => '||NVL(SUBSTR(g_params.stats_method_opt.get_value, 2, LENGTH(g_params.stats_method_opt.get_value)-2), 'dbms_stats.default_method_opt')||',
    degree           => '||NVL(g_params.parallel.get_num_value, dbms_stats.default_degree_value)||',
    cascade          => true,
    no_invalidate    => false
  );
end;';
    ELSE
      l_sql := null;
    END IF;
  END get_stats_sql;

  PROCEDURE build_partition_loop_sql(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    in_column_list      IN CLOB,
    in_values_list      IN CLOB,
    in_filter_sql       IN CLOB,
    in_recreate_mode    IN PLS_INTEGER,
    io_sql_arr          IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_source_partition_rec     gt_partition_rec;
    l_target_partition_rec     gt_partition_rec;
    l_cnt                      PLS_INTEGER;
  BEGIN
    start_timer;

    IF in_source_table_rec.partitioning_type IS NULL THEN
      raise_error('Copy data by partitions is not supported. Source table is not partitioned');
    END IF;

    l_cnt := 1;

    FOR i IN REVERSE 1..in_source_table_rec.partition_arr.COUNT LOOP
      l_source_partition_rec := in_source_table_rec.partition_arr(i);

      IF l_source_partition_rec.is_partition_empty THEN
        debug('partition '||l_source_partition_rec.partition_name||' is empty');
      END IF;

      IF (l_source_partition_rec.matching_indx IS NOT NULL AND in_recreate_mode = cort_comp_pkg.gc_result_part_exchange) OR
          l_source_partition_rec.is_partition_empty
      THEN
        -- move data
        NULL;
      ELSE
        IF l_source_partition_rec.matching_indx = -1 THEN
          NULL; -- skip partition
        ELSE
          IF l_source_partition_rec.matching_indx > 0 THEN
            l_target_partition_rec := in_target_table_rec.partition_arr(l_source_partition_rec.matching_indx);
          ELSE
            l_target_partition_rec := NULL;
          END IF;

          get_copy_data_sql(
            in_source_table_rec     => in_source_table_rec,
            in_target_table_rec     => in_target_table_rec,
            in_source_partition_rec => l_source_partition_rec,
            in_target_partition_rec => l_target_partition_rec,
            in_column_list          => in_column_list,
            in_values_list          => in_values_list,
            in_filter_sql           => in_filter_sql,
            io_sql_arr              => io_sql_arr
          );

          l_cnt := l_cnt + 1;
        END IF;
      END IF;
    END LOOP;

    stop_timer;
  END build_partition_loop_sql;

  PROCEDURE build_partition_loop_stats(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    io_sql_arr          IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_source_partition_rec     gt_partition_rec;
    l_target_partition_rec     gt_partition_rec;
  BEGIN
    start_timer;

    IF in_target_table_rec.partitioning_type IS NULL THEN
      raise_error('Gather stats by partitions is not supported. Target table is not partitioned');
    END IF;


    FOR i IN REVERSE 1..in_target_table_rec.partition_arr.COUNT LOOP
      l_target_partition_rec := in_target_table_rec.partition_arr(i);

      IF l_target_partition_rec.matching_indx > 0 THEN
        l_source_partition_rec := in_source_table_rec.partition_arr(l_target_partition_rec.matching_indx);
      ELSE
        l_source_partition_rec := NULL;
      END IF;

      get_stats_sql(
        in_source_table_rec     => in_source_table_rec,
        in_target_table_rec     => in_target_table_rec,
        in_source_partition_rec => l_source_partition_rec,
        in_target_partition_rec => l_target_partition_rec,
        io_sql_arr              => io_sql_arr
      );

    END LOOP;

    stop_timer;
  END build_partition_loop_stats;

  PROCEDURE build_subpartition_loop_sql(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    in_column_list      IN CLOB,
    in_values_list      IN CLOB,
    in_filter_sql       IN CLOB,
    in_recreate_mode    IN PLS_INTEGER,
    io_sql_arr          IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_source_partition_rec     gt_partition_rec;
    l_source_subpartition_rec  gt_partition_rec;
    l_target_subpartition_rec  gt_partition_rec;
    l_cnt                      PLS_INTEGER;
  BEGIN
    start_timer;

    IF in_source_table_rec.subpartitioning_type IS NULL THEN
      raise_error('Copy data by subpartitions is not supported. Source table is not subpartitioned');
    END IF;

    l_cnt := 1;

    FOR i IN REVERSE 1..in_source_table_rec.partition_arr.COUNT LOOP
      l_source_partition_rec := in_source_table_rec.partition_arr(i);
      FOR j IN l_source_partition_rec.subpartition_from_indx..l_source_partition_rec.subpartition_to_indx LOOP
        l_source_subpartition_rec := in_source_table_rec.subpartition_arr(j);
        IF l_source_partition_rec.matching_indx > 0 AND l_source_subpartition_rec.matching_indx > 0 THEN
          l_target_subpartition_rec := in_target_table_rec.subpartition_arr(l_source_subpartition_rec.matching_indx);
        ELSE
          l_target_subpartition_rec := NULL;
        END IF;

        IF (l_source_partition_rec.matching_indx IS NOT NULL AND
            l_source_subpartition_rec.matching_indx IS NOT NULL AND
            in_recreate_mode = cort_comp_pkg.gc_result_part_exchange) OR
           l_source_partition_rec.is_partition_empty
        THEN
          -- move data
          NULL;
        ELSE
          get_copy_data_sql(
            in_source_table_rec     => in_source_table_rec,
            in_target_table_rec     => in_target_table_rec,
            in_source_partition_rec => l_source_subpartition_rec,
            in_target_partition_rec => l_target_subpartition_rec,
            in_column_list          => in_column_list,
            in_values_list          => in_values_list,
            in_filter_sql           => in_filter_sql,
            io_sql_arr              => io_sql_arr
          );
          l_cnt := l_cnt + 1;
        END IF;
      END LOOP;

    END LOOP;

    stop_timer;
  END build_subpartition_loop_sql;

  PROCEDURE build_subpartition_loop_stats(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    io_sql_arr          IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_target_partition_rec     gt_partition_rec;
    l_target_subpartition_rec  gt_partition_rec;
    l_source_subpartition_rec  gt_partition_rec;
  BEGIN
    start_timer;

    IF in_target_table_rec.subpartitioning_type IS NULL THEN
      raise_error('Gather stats by subpartitions is not supported. Target table is not subpartitioned');
    END IF;

    FOR i IN REVERSE 1..in_target_table_rec.partition_arr.COUNT LOOP
      l_target_partition_rec := in_target_table_rec.partition_arr(i);
      FOR j IN l_target_partition_rec.subpartition_from_indx..l_target_partition_rec.subpartition_to_indx LOOP
        l_target_subpartition_rec := in_target_table_rec.subpartition_arr(j);
        IF l_target_partition_rec.matching_indx > 0 AND l_target_subpartition_rec.matching_indx > 0 THEN
          l_source_subpartition_rec := in_source_table_rec.subpartition_arr(l_target_subpartition_rec.matching_indx);
        ELSE
          l_source_subpartition_rec := NULL;
        END IF;

        get_stats_sql(
          in_target_table_rec     => in_target_table_rec,
          in_source_table_rec     => in_source_table_rec,
          in_target_partition_rec => l_target_subpartition_rec,
          in_source_partition_rec => l_source_subpartition_rec,
          io_sql_arr              => io_sql_arr
        );
      END LOOP;

    END LOOP;

    stop_timer;
  END build_subpartition_loop_stats;

  -- copying data from old tale to new one
  PROCEDURE copy_data(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    in_recreate_mode    IN PLS_INTEGER,
    in_filter_sql       IN CLOB,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_column_list   CLOB;
    l_values_list   CLOB;
    l_last_indx     PLS_INTEGER;
  BEGIN
    start_timer;

    cort_comp_pkg.get_column_values_list(
      in_source_table_rec => in_source_table_rec,
      in_target_table_rec => in_target_table_rec,
      out_columns_list    => l_column_list,
      out_values_list     => l_values_list
    );

    --check for non-empty strings
    IF l_column_list IS NULL OR
       l_values_list IS NULL
    THEN
      RETURN;
    END IF;

    -- disable all foreign keys
    change_status_for_all_fk(
      in_table_rec     => in_target_table_rec,
      in_action        => 'DISABLE',
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );
    change_status_for_policies(
      in_table_rec     => in_source_table_rec,
      in_action        => 'DISABLE',
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );

    l_last_indx := io_frwd_stmt_arr.COUNT;

    IF in_source_table_rec.partitioned = 'NO' THEN

      get_copy_data_sql(
        in_source_table_rec     => in_source_table_rec,
        in_target_table_rec     => in_target_table_rec,
        in_source_partition_rec => null,
        in_target_partition_rec => null,
        in_column_list          => l_column_list,
        in_values_list          => l_values_list,
        in_filter_sql           => in_filter_sql,
        io_sql_arr              => io_frwd_stmt_arr
      );

      IF in_target_table_rec.partitioned = 'YES' AND in_target_table_rec.interval IS NULL THEN
        -- build loop by partitions only if new partitions are hardcoded and not interval
        -- because interval partitions are created when data are copied and not defined yet at this moment
        build_partition_loop_stats(
          in_source_table_rec => in_source_table_rec,
          in_target_table_rec => in_target_table_rec,
          io_sql_arr          => io_frwd_stmt_arr
        );
        $IF cort_options_pkg.gc_threading $THEN
        cort_thread_exec_pkg.add_threading_sql(
          io_sql_arr          => io_frwd_stmt_arr
        );
        $END
      ELSE
        get_stats_sql(
          in_target_table_rec     => in_target_table_rec,
          in_source_table_rec     => in_source_table_rec,
          in_target_partition_rec => null,
          in_source_partition_rec => null,
          io_sql_arr              => io_frwd_stmt_arr
        );
      END IF;

    ELSE
      IF in_source_table_rec.subpartitioning_type <> 'NONE' AND in_source_table_rec.subpartitioning_type = in_target_table_rec.subpartitioning_type THEN
        build_subpartition_loop_sql(
          in_source_table_rec => in_source_table_rec,
          in_target_table_rec => in_target_table_rec,
          in_column_list      => l_column_list,
          in_values_list      => l_values_list,
          in_filter_sql       => in_filter_sql,
          in_recreate_mode    => in_recreate_mode,
          io_sql_arr          => io_frwd_stmt_arr
        );
        build_subpartition_loop_stats(
          in_source_table_rec => in_source_table_rec,
          in_target_table_rec => in_target_table_rec,
          io_sql_arr          => io_frwd_stmt_arr
        );
      ELSE
        build_partition_loop_sql(
          in_source_table_rec => in_source_table_rec,
          in_target_table_rec => in_target_table_rec,
          in_column_list      => l_column_list,
          in_values_list      => l_values_list,
          in_filter_sql       => in_filter_sql,
          in_recreate_mode    => in_recreate_mode,
          io_sql_arr          => io_frwd_stmt_arr
        );
        IF in_target_table_rec.partitioned = 'YES' THEN
          build_partition_loop_stats(
            in_source_table_rec => in_source_table_rec,
            in_target_table_rec => in_target_table_rec,
            io_sql_arr          => io_frwd_stmt_arr
          );
        END IF;
      END IF;
      $IF cort_options_pkg.gc_threading $THEN
      cort_thread_exec_pkg.add_threading_sql(
        io_sql_arr          => io_frwd_stmt_arr
      );
      $END
    END IF;

    -- Gather global stats (if needed)
    get_stats_sql(
      in_target_table_rec     => in_target_table_rec,
      in_source_table_rec     => in_source_table_rec,
      in_target_partition_rec => null,
      in_source_partition_rec => null,
      io_sql_arr              => io_frwd_stmt_arr
    );


    -- add nulls to rollback ddl
    FOR i IN l_last_indx+1..io_frwd_stmt_arr.COUNT LOOP
      io_rlbk_stmt_arr(i) := NULL;
    END LOOP;

    -- enable all foreign keys
    change_status_for_all_fk(
      in_table_rec     => in_target_table_rec,
      in_action        => 'ENABLE',
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );
    change_status_for_policies(
      in_table_rec     => in_source_table_rec,
      in_action        => 'ENABLE',
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );

    stop_timer;
  END copy_data;

  PROCEDURE exchange_partitions(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_swap_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_source_part_rec  IN cort_exec_pkg.gt_partition_rec,
    in_target_part_rec  IN cort_exec_pkg.gt_partition_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_stmt_arr          arrays.gt_clob_arr;
    l_rlbk_stmt_arr          arrays.gt_clob_arr;
  BEGIN
    -- create swap table for given partition
    cort_comp_pkg.create_swap_table_sql(
      in_table_rec      => in_target_table_rec,
      in_swap_table_rec => in_swap_table_rec,
      io_frwd_stmt_arr  => l_frwd_stmt_arr,
      io_rlbk_stmt_arr  => l_rlbk_stmt_arr
    );
    -- forward changes:
    FOR j IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(j);
    END LOOP;
    FOR j IN 1..l_rlbk_stmt_arr.COUNT LOOP
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(j);
    END LOOP;

    -- Exchange partition from source table with swap table
    cort_comp_pkg.exchange_partition(
      in_source_table_rec => in_source_table_rec,
      in_target_table_rec => in_swap_table_rec,
      in_partition_rec    => in_source_part_rec,
      io_frwd_stmt_arr    => io_frwd_stmt_arr,
      io_rlbk_stmt_arr    => io_rlbk_stmt_arr
    );
    -- Exchange partition from target table with swap table
    cort_comp_pkg.exchange_partition(
      in_source_table_rec => in_target_table_rec,
      in_target_table_rec => in_swap_table_rec,
      in_partition_rec    => in_target_part_rec,
      io_frwd_stmt_arr    => io_frwd_stmt_arr,
      io_rlbk_stmt_arr    => io_rlbk_stmt_arr
    );

    -- drop swap table for given partition (switch forward and rollback statements). Change becomes absolutely symmetric
    FOR j IN 1..l_rlbk_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(j);
    END LOOP;
    FOR j IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_frwd_stmt_arr(j);
    END LOOP;

  END exchange_partitions;

  PROCEDURE exchange_partitions(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_swap_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_source_part_arr  IN cort_exec_pkg.gt_partition_arr,
    in_target_part_arr  IN cort_exec_pkg.gt_partition_arr,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_stmt_arr          arrays.gt_clob_arr;
    l_rlbk_stmt_arr          arrays.gt_clob_arr;
  BEGIN
    -- create swap table for given partition
    cort_comp_pkg.create_swap_table_sql(
      in_table_rec      => in_target_table_rec,
      in_swap_table_rec => in_swap_table_rec,
      io_frwd_stmt_arr  => l_frwd_stmt_arr,
      io_rlbk_stmt_arr  => l_rlbk_stmt_arr
    );
    -- forward changes:
    FOR j IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(j);
    END LOOP;
    FOR j IN 1..l_rlbk_stmt_arr.COUNT LOOP
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(j);
    END LOOP;

    FOR i IN 1..in_source_part_arr.COUNT LOOP
      -- Exchange partition from source table with swap table
      cort_comp_pkg.exchange_partition(
        in_source_table_rec => in_source_table_rec,
        in_target_table_rec => in_swap_table_rec,
        in_partition_rec    => in_source_part_arr(i),
        io_frwd_stmt_arr    => io_frwd_stmt_arr,
        io_rlbk_stmt_arr    => io_rlbk_stmt_arr
      );
      -- Exchange partition from target table with swap table
      cort_comp_pkg.exchange_partition(
        in_source_table_rec => in_target_table_rec,
        in_target_table_rec => in_swap_table_rec,
        in_partition_rec    => in_target_part_arr(i),
        io_frwd_stmt_arr    => io_frwd_stmt_arr,
        io_rlbk_stmt_arr    => io_rlbk_stmt_arr
      );
     END LOOP;

    -- drop swap table for given partition (switch forward and rollback statements). Change becomes absolutely symmetric
    FOR j IN 1..l_rlbk_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(j);
    END LOOP;
    FOR j IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_frwd_stmt_arr(j);
    END LOOP;

  END exchange_partitions;

  PROCEDURE exchange_partition(
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec,
    in_swap_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_swap_part_rec    IN cort_exec_pkg.gt_partition_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_frwd_stmt_arr          arrays.gt_clob_arr;
    l_rlbk_stmt_arr          arrays.gt_clob_arr;
  BEGIN
    -- create swap table for given partition
    cort_comp_pkg.create_swap_table_sql(
      in_table_rec      => in_target_table_rec,
      in_swap_table_rec => in_swap_table_rec,
      io_frwd_stmt_arr  => l_frwd_stmt_arr,
      io_rlbk_stmt_arr  => l_rlbk_stmt_arr
    );
    -- forward changes:
    FOR j IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(j);
    END LOOP;
    FOR j IN 1..l_rlbk_stmt_arr.COUNT LOOP
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(j);
    END LOOP;

    -- Exchange partition from source table with swap table
    cort_comp_pkg.exchange_partition(
      in_source_table_rec => in_swap_table_rec,
      in_target_table_rec => in_source_table_rec,
      in_partition_rec    => in_swap_part_rec,
      io_frwd_stmt_arr    => io_frwd_stmt_arr,
      io_rlbk_stmt_arr    => io_rlbk_stmt_arr
    );
    -- Exchange partition from target table with swap table
    cort_comp_pkg.exchange_partition(
      in_source_table_rec => in_swap_table_rec,
      in_target_table_rec => in_target_table_rec,
      in_partition_rec    => in_swap_part_rec,
      io_frwd_stmt_arr    => io_frwd_stmt_arr,
      io_rlbk_stmt_arr    => io_rlbk_stmt_arr
    );

    -- drop swap table for given partition (switch forward and rollback statements). Change becomes absolutely symmetric
    FOR j IN 1..l_rlbk_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(j);
    END LOOP;
    FOR j IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_frwd_stmt_arr(j);
    END LOOP;

  END exchange_partition;

  -- move data using exchange partition mechanism
  PROCEDURE move_data(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    in_recreate_mode    IN PLS_INTEGER,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_swap_table_name        arrays.gt_name;
    l_swap_table_rec         gt_table_rec;
    l_swap_part_rec          gt_partition_rec;
    l_source_part_rec        gt_partition_rec;
    l_target_part_rec        gt_partition_rec;
    l_source_subpart_rec     gt_partition_rec;
    l_target_subpart_rec     gt_partition_rec;
    l_source_part_arr        gt_partition_arr;
    l_target_part_arr        gt_partition_arr;
  BEGIN
    start_timer;
    l_swap_table_name := get_object_temp_name(
                           in_object_id   => in_source_table_rec.rename_rec.object_id,
                           in_owner       => in_source_table_rec.owner,
                           in_prefix      => cort_params_pkg.gc_swap_prefix
                         );
    l_swap_table_rec.table_name := l_swap_table_name;
    l_swap_table_rec.owner := in_source_table_rec.owner;
    l_swap_table_rec.rename_rec.object_name := l_swap_table_name;
    l_swap_table_rec.rename_rec.object_owner := in_source_table_rec.owner;
    l_swap_table_rec.rename_rec.current_name := l_swap_table_name;

    IF in_source_table_rec.partitioned = 'NO' THEN
      -- create swap table with 1 list or system partition
      l_swap_table_rec.partitioned := 'YES';
      l_swap_table_rec.partitioning_type := 'LIST';
      l_swap_table_rec.subpartitioning_type := 'NONE';
      l_swap_table_rec.part_key_column_arr(1) := cort_comp_pkg.get_column_for_partitioning(in_source_table_rec);

      l_swap_part_rec.table_owner := l_swap_table_rec.owner;
      l_swap_part_rec.table_name := l_swap_table_rec.table_name;
      l_swap_part_rec.partition_name := 'P_DEF';
      l_swap_part_rec.partition_level := 'PARTITION';
      l_swap_part_rec.partition_type := 'LIST';
      l_swap_part_rec.high_value := 'DEFAULT';
      l_swap_part_rec.composite := 'NO';
      l_swap_part_rec.position := 1;
      l_swap_part_rec.physical_attr_rec  := in_source_table_rec.physical_attr_rec;
      l_swap_part_rec.subpartition_from_indx := null;
      l_swap_part_rec.subpartition_to_indx   := null;

      l_swap_table_rec.partition_arr(1) := l_swap_part_rec;
      l_swap_table_rec.partition_indx_arr('P_DEF') := 1;

      exchange_partition(
        in_source_table_rec => in_source_table_rec,
        in_target_table_rec => in_target_table_rec,
        in_swap_table_rec   => l_swap_table_rec,
        in_swap_part_rec    => l_swap_part_rec,
        io_frwd_stmt_arr    => io_frwd_stmt_arr,
        io_rlbk_stmt_arr    => io_rlbk_stmt_arr
      );

    ELSIF in_source_table_rec.partitioned = 'YES' AND in_source_table_rec.subpartitioning_type = 'NONE' THEN

        IF cort_comp_pkg.is_subpartitioning_available(in_target_table_rec) AND in_recreate_mode = cort_comp_pkg.gc_result_exchange THEN

          l_swap_table_rec.partitioned := 'YES';
          l_swap_table_rec.partitioning_type := 'LIST';
          l_swap_table_rec.subpartitioning_type := in_source_table_rec.subpartitioning_type;
          l_swap_table_rec.part_key_column_arr(1) := cort_comp_pkg.get_column_for_partitioning(in_source_table_rec);
          l_swap_table_rec.subpart_key_column_arr := in_source_table_rec.part_key_column_arr;

          l_swap_part_rec.table_owner := l_swap_table_rec.owner;
          l_swap_part_rec.table_name := l_swap_table_rec.table_name;
          l_swap_part_rec.partition_name := 'P_DEF';
          l_swap_part_rec.partition_level := 'PARTITION';
          l_swap_part_rec.partition_type := 'LIST';
          l_swap_part_rec.high_value := 'DEFAULT';
          l_swap_part_rec.composite := 'YES';
          l_swap_part_rec.position := 1;
          l_swap_part_rec.physical_attr_rec  := in_source_table_rec.partition_arr(1).physical_attr_rec;
          l_swap_part_rec.subpartition_from_indx := 1;
          l_swap_part_rec.subpartition_to_indx := in_source_table_rec.partition_arr.COUNT;

          l_swap_table_rec.partition_arr(1) := l_swap_part_rec;
          l_swap_table_rec.partition_indx_arr('P_DEF') := 1;
          l_swap_table_rec.subpartition_arr := in_source_table_rec.partition_arr;

          exchange_partitions(
            in_source_table_rec => in_source_table_rec,
            in_target_table_rec => in_target_table_rec,
            in_swap_table_rec   => l_swap_table_rec,
            in_source_part_rec  => l_source_part_rec,
            in_target_part_rec  => l_target_part_rec,
            io_frwd_stmt_arr    => io_frwd_stmt_arr,
            io_rlbk_stmt_arr    => io_rlbk_stmt_arr
          );
        ELSE
           l_swap_table_rec.partitioned := 'NO';
           l_swap_table_rec.partitioning_type := NULL;
           l_swap_table_rec.subpartitioning_type := NULL;
           l_swap_table_rec.part_key_column_arr.DELETE;
           l_swap_table_rec.partition_arr.DELETE;
           l_target_part_arr.DELETE;
           l_source_part_arr.DELETE;

           -- repeat for every partition
           FOR i IN 1..in_target_table_rec.partition_arr.COUNT LOOP

             --for partition which exists in both tables
             IF in_target_table_rec.partition_arr(i).matching_indx > 0 THEN

               l_target_part_rec := in_target_table_rec.partition_arr(i);
               l_source_part_rec := in_source_table_rec.partition_arr(l_target_part_rec.matching_indx);

               debug('Move data from partition '||l_source_part_rec.partition_name||' into partition '||l_target_part_rec.partition_name);

               IF NOT l_source_part_rec.is_partition_empty THEN
                 l_target_part_arr(l_target_part_arr.COUNT+1) := l_target_part_rec;
                 l_source_part_arr(l_source_part_arr.COUNT+1) := l_source_part_rec;
               END IF;
             END IF;
           END LOOP;

           exchange_partitions(
             in_source_table_rec => in_source_table_rec,
             in_target_table_rec => in_target_table_rec,
             in_swap_table_rec   => l_swap_table_rec,
             in_source_part_arr  => l_source_part_arr,
             in_target_part_arr  => l_target_part_arr,
             io_frwd_stmt_arr    => io_frwd_stmt_arr,
             io_rlbk_stmt_arr    => io_rlbk_stmt_arr
           );
        END IF;

    ELSIF in_source_table_rec.partitioned = 'YES' AND in_source_table_rec.subpartitioning_type <> 'NONE' THEN

      -- repeat for every partition
      FOR i IN 1..in_target_table_rec.partition_arr.COUNT LOOP

        --for partition which exists in both tables
        IF in_target_table_rec.partition_arr(i).matching_indx > 0 THEN

          l_target_part_rec := in_target_table_rec.partition_arr(i);
          l_source_part_rec := in_source_table_rec.partition_arr(l_target_part_rec.matching_indx);


          IF in_target_table_rec.partition_arr(i).subpart_comp_result = cort_comp_pkg.gc_result_nochange THEN

            debug('Move data from partition '||l_source_part_rec.partition_name||' into partition '||l_target_part_rec.partition_name||' for all subpartitions');
            l_swap_table_rec.partitioned := 'YES';
            l_swap_table_rec.partitioning_type := 'LIST';
            l_swap_table_rec.subpartitioning_type := 'NONE';
            l_swap_table_rec.part_key_column_arr := in_target_table_rec.subpart_key_column_arr;
            l_swap_table_rec.partition_arr.DELETE;
            l_target_part_arr.DELETE;
            l_source_part_arr.DELETE;

            FOR j IN in_target_table_rec.partition_arr(i).subpartition_from_indx..in_target_table_rec.partition_arr(i).subpartition_to_indx LOOP
              l_swap_part_rec.table_owner := l_swap_table_rec.owner;
              l_swap_part_rec.table_name := l_swap_table_rec.table_name;
              l_swap_part_rec.partition_name := in_target_table_rec.subpartition_arr(j).partition_name;
              l_swap_part_rec.partition_level := 'PARTITION';
              l_swap_part_rec.partition_type := in_target_table_rec.subpartition_arr(j).partition_type;
              l_swap_part_rec.high_value := in_target_table_rec.subpartition_arr(j).high_value;
              l_swap_part_rec.composite := 'NO';
              l_swap_part_rec.position := in_target_table_rec.subpartition_arr(j).position;
              l_swap_part_rec.physical_attr_rec  := in_target_table_rec.subpartition_arr(j).physical_attr_rec;
              l_swap_table_rec.partition_arr(l_swap_table_rec.partition_arr.COUNT+1) := l_swap_part_rec;
              l_swap_table_rec.partition_indx_arr(l_swap_part_rec.partition_name) := l_swap_table_rec.partition_arr.COUNT;
            END LOOP;

            exchange_partitions(
              in_source_table_rec => in_source_table_rec,
              in_target_table_rec => in_target_table_rec,
              in_swap_table_rec   => l_swap_table_rec,
              in_source_part_rec  => l_source_part_rec,
              in_target_part_rec  => l_target_part_rec,
              io_frwd_stmt_arr    => io_frwd_stmt_arr,
              io_rlbk_stmt_arr    => io_rlbk_stmt_arr
            );
          ELSE
            -- if subpartitions don't match
            -- exchange every subpartition individually

            debug('Move data from partition '||l_source_part_rec.partition_name||' into partition '||l_target_part_rec.partition_name||' for all each subsubpartition individually');
            l_swap_table_rec.partitioned := 'NO';
            l_swap_table_rec.partitioning_type := NULL;
            l_swap_table_rec.subpartitioning_type := NULL;
            l_swap_table_rec.part_key_column_arr.DELETE;
            l_swap_table_rec.partition_arr.DELETE;
            l_target_part_arr.DELETE;
            l_source_part_arr.DELETE;

            FOR j IN l_target_part_rec.subpartition_from_indx..l_target_part_rec.subpartition_to_indx LOOP
              l_target_subpart_rec := in_target_table_rec.subpartition_arr(j);

              IF l_target_subpart_rec.matching_indx > 0 THEN
                l_source_subpart_rec := in_source_table_rec.subpartition_arr(l_target_subpart_rec.matching_indx);
                IF NOT l_source_subpart_rec.is_partition_empty THEN
                  debug('Move data from subpartition '||l_source_subpart_rec.partition_name||' into subpartition '||l_target_subpart_rec.partition_name);
                  l_target_part_arr(l_target_part_arr.COUNT+1) := l_target_subpart_rec;
                  l_source_part_arr(l_source_part_arr.COUNT+1) := l_source_subpart_rec;
                END IF;
              END IF;
            END LOOP;

            exchange_partitions(
              in_source_table_rec => in_source_table_rec,
              in_target_table_rec => in_target_table_rec,
              in_swap_table_rec   => l_swap_table_rec,
              in_source_part_arr  => l_source_part_arr,
              in_target_part_arr  => l_target_part_arr,
              io_frwd_stmt_arr    => io_frwd_stmt_arr,
              io_rlbk_stmt_arr    => io_rlbk_stmt_arr
            );

          END IF;

        END IF;

      END LOOP;

    END IF;
    stop_timer;
  END move_data;


  -- copy all table's triggers
  PROCEDURE copy_triggers(
    io_source_table_rec IN OUT NOCOPY gt_table_rec,
    io_target_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_frwd_alter_stmt_arr      arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr      arrays.gt_clob_arr;
  BEGIN
    cort_comp_pkg.copy_triggers(
      in_source_table_rec      => io_source_table_rec,
      io_target_table_rec      => io_target_table_rec,
      io_frwd_alter_stmt_arr   => l_frwd_alter_stmt_arr,
      io_rlbk_alter_stmt_arr   => l_rlbk_alter_stmt_arr
    );

    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr,
      in_test                => g_params.test.get_bool_value
    );

  END copy_triggers;

  -- copy all table's policies
  PROCEDURE copy_policies(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec
  )
  AS
    l_frwd_alter_stmt_arr      arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr      arrays.gt_clob_arr;
  BEGIN
    cort_comp_pkg.copy_policies(
      in_source_table_rec      => in_source_table_rec,
      in_target_table_rec      => in_target_table_rec,
      io_frwd_alter_stmt_arr   => l_frwd_alter_stmt_arr,
      io_rlbk_alter_stmt_arr   => l_rlbk_alter_stmt_arr
    );

    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr,
      in_test                => g_params.test.get_bool_value
    );

  END copy_policies;

  PROCEDURE copy_stats(
    in_owner        IN VARCHAR2,
    in_source_table IN VARCHAR2,
    in_target_table IN VARCHAR2,
    in_source_part  IN VARCHAR2 DEFAULT NULL,
    in_target_part  IN VARCHAR2 DEFAULT NULL,
    in_part_level   IN VARCHAR2 DEFAULT NULL
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_stats_table arrays.gt_name;
  BEGIN
    start_timer;
    dbms_stats.export_table_stats(
      ownname      => '"'||in_owner||'"',
      tabname      => '"'||in_source_table||'"',
      partname     => case when in_source_part is not null then '"'||in_source_part||'"' end,
      stattab      => '"'||cort_params_pkg.gc_stat_table||'"',
      statown      => '"'||cort_params_pkg.gc_schema||'"',
      cascade      => TRUE
    );

    IF cort_options_pkg.gc_use_context AND dbms_db_version.version < 12 THEN
      cort_aux_pkg.set_context('TARGET_TABLE_NAME', in_target_table);
      cort_aux_pkg.set_context('TARGET_PARTITION_NAME', in_target_part);
      cort_aux_pkg.set_context('TARGET_PARTITION_LEVEL', in_part_level);
      l_stats_table := cort_params_pkg.gc_import_stat_table;
    ELSE
      cort_aux_pkg.copy_stats(
        in_source_table_name => in_source_table,
        in_target_table_name => in_target_table,
        in_source_part       => in_source_part,
        in_target_part       => in_target_part,
        in_part_level        => in_part_level
      );
      l_stats_table := cort_params_pkg.gc_stat_table;
    END IF;

    dbms_stats.import_table_stats(
      ownname       => '"'||in_owner||'"',
      tabname       => '"'||in_target_table||'"',
      partname      => case when in_target_part is not null then '"'||in_target_part||'"' end,
      stattab       => l_stats_table,
      statown       => '"'||cort_params_pkg.gc_schema||'"',
      no_invalidate => TRUE,
      cascade       => TRUE
    );
    stop_timer;
    COMMIT;
  END copy_stats;

  -- rename tables, copy data, move references, copy comments, copy grants, reassign synonyms
  PROCEDURE recreate_table(
    io_source_table_rec IN OUT NOCOPY gt_table_rec,
    io_target_table_rec IN OUT NOCOPY gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    in_recreate_mode    IN PLS_INTEGER,
    in_data_filter      IN CLOB,
    in_job_rec          IN cort_jobs%ROWTYPE,
    out_last_ddl_index  OUT PLS_INTEGER
  )
  AS
    l_frwd_stmt_arr    arrays.gt_clob_arr;
    l_rlbk_stmt_arr    arrays.gt_clob_arr;
    l_indx             PLS_INTEGER;
  BEGIN
    start_timer;

    BEGIN
/*
      IF g_params.data.get_value = 'PRELIMINARY_COPY' AND
         NOT io_source_table_rec.is_table_empty AND
         in_recreate_mode in (cort_comp_pkg.gc_result_recreate) THEN
        create_sync_objects(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          in_recreate_mode    => in_recreate_mode,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );

        copy_data(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          in_recreate_mode    => in_recreate_mode,
          in_filter_sql       => in_data_filter,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );
      END IF;
*/

      -- following changes are added to the queue
      IF g_params.data.get_value = 'COPY' AND
         NOT io_source_table_rec.is_table_empty AND
         in_recreate_mode in (cort_comp_pkg.gc_result_recreate, cort_comp_pkg.gc_result_part_exchange)
      THEN
        debug ('in_sql_rec.data_filter = '||in_data_filter);

        copy_data(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          in_recreate_mode    => in_recreate_mode,
          in_filter_sql       => in_data_filter,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );
      END IF;

      IF NOT io_source_table_rec.is_table_empty AND
         in_recreate_mode in (cort_comp_pkg.gc_result_exchange, cort_comp_pkg.gc_result_part_exchange)
      THEN
        -- add create swap table exchange partition and drop swap table statements into the queue
        move_data(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          in_recreate_mode    => in_recreate_mode,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );
      END IF;

      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
        in_test                => g_params.test.get_bool_value
      );

    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.error('Error. Rolling back...');
        exec_drop_table(
          in_table_name => io_target_table_rec.owner,
          in_owner      => io_target_table_rec.rename_rec.current_name,
          in_echo       => FALSE
        );
        RAISE;
    END;

    FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
    END LOOP;

    l_frwd_stmt_arr.DELETE;
    l_rlbk_stmt_arr.DELETE;

    BEGIN
      IF g_params.keep_objects.value_exists('COMMENTS') THEN
        copy_comments(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );
      END IF;
      IF g_params.keep_objects.value_exists('POLICIES') THEN
        cort_comp_pkg.copy_policies(
          in_source_table_rec      => io_source_table_rec,
          in_target_table_rec      => io_target_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
      END IF;
      IF g_params.keep_objects.value_exists('REFERENCES') THEN
        cort_comp_pkg.copy_references(
          in_source_table_rec      => io_source_table_rec,
          io_target_table_rec      => io_target_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
      ELSE
        cort_comp_pkg.drop_references(
          in_source_table_rec      => io_source_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
      END IF;
      cort_comp_pkg.drop_triggers(
        in_table_rec             => io_source_table_rec,
        io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
        io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
      );
      IF g_params.keep_objects.value_exists('TRIGGERS') THEN
        cort_comp_pkg.copy_triggers(
          in_source_table_rec      => io_source_table_rec,
          io_target_table_rec      => io_target_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
      END IF;

      -- move depending objects ignoring errors
      apply_changes_ignore_errors(
        io_stmt_arr => l_frwd_stmt_arr,
        in_test     => g_params.test.get_bool_value
      );
    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.error('Error. Rolling back...');
        exec_drop_table(
          in_table_name => io_source_table_rec.owner,
          in_owner      => io_source_table_rec.rename_rec.temp_name,
          in_echo       => FALSE
        );
        RAISE;
    END;

    l_indx := l_frwd_stmt_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(l_indx);
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(l_indx);
      l_indx := l_frwd_stmt_arr.NEXT(l_indx);
    END LOOP;

    l_frwd_stmt_arr.DELETE;
    l_rlbk_stmt_arr.DELETE;

    -- if in build
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') IS NOT NULL THEN
      cort_comp_pkg.drop_triggers(
        in_table_rec             => io_source_table_rec,
        io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
        io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
      );
      -- register state for all existing triggers on current table
      FOR i IN 1..io_source_table_rec.trigger_arr.COUNT LOOP
        cort_aux_pkg.register_change(
          in_object_owner   => io_source_table_rec.trigger_arr(i).owner,
          in_object_name    => io_source_table_rec.trigger_arr(i).trigger_name,
          in_object_type    => 'TRIGGER',
          in_job_rec        => in_job_rec,
          in_sql            => NULL,
          in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                 in_object_owner   => io_source_table_rec.trigger_arr(i).owner,
                                 in_object_name    => io_source_table_rec.trigger_arr(i).trigger_name,
                                 in_object_type    => 'TRIGGER'
                               ),
          in_change_type    => cort_comp_pkg.gc_result_recreate,
          in_revert_name    => NULL,
          in_last_ddl_index => NULL,
          in_frwd_stmt_arr  => l_frwd_stmt_arr,
          in_rlbk_stmt_arr  => l_rlbk_stmt_arr
        );
      END LOOP;
    END IF;

    IF g_params.data.get_value = 'PRELIMINARY_COPY' AND
       NOT io_source_table_rec.is_table_empty AND
       in_recreate_mode in (cort_comp_pkg.gc_result_recreate, cort_comp_pkg.gc_result_part_exchange)
    THEN
      l_frwd_stmt_arr.DELETE;
      l_rlbk_stmt_arr.DELETE;
      FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
      END LOOP;


      -- moving renaming sql into second part of the job.
      out_last_ddl_index := io_frwd_stmt_arr.COUNT;
    END IF;


    -- Final 2 steps - renaming

    l_frwd_stmt_arr.DELETE;
    l_rlbk_stmt_arr.DELETE;

    comment_table(
      in_table_rec     => io_source_table_rec,
      io_frwd_stmt_arr => l_frwd_stmt_arr,
      io_rlbk_stmt_arr => l_rlbk_stmt_arr
    );

    -- rename existing (old) table and all depending objects and log groups to cort_name
    rename_table_cascade(
      io_table_rec     => io_source_table_rec,
      in_rename_mode   => 'TO_CORT',
      io_frwd_stmt_arr => l_frwd_stmt_arr,
      io_rlbk_stmt_arr => l_rlbk_stmt_arr
    );

    -- rename new table and all depending objects to actual names
    rename_table_cascade(
      io_table_rec     => io_target_table_rec,
      in_rename_mode   => 'TO_ORIGINAL',
      io_frwd_stmt_arr => l_frwd_stmt_arr,
      io_rlbk_stmt_arr => l_rlbk_stmt_arr
    );

    FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
    END LOOP;


    -- for CREATE AS SELECT FROM itself
    -- check if selecting from recreating table
    -- if yes source session will hold shared lock on this table and will not allow to rename the table.
    -- so last 2 steps need to be done after release trigger's lock.
    IF (in_recreate_mode = cort_comp_pkg.gc_result_cas_from_itself) AND NOT g_params.test.get_bool_value THEN
      debug('CREATE AS SELECT FROM itself');

      IF g_params.data.get_value != 'PRELIMINARY_COPY' THEN
        -- Just output DDL commands without execution
        apply_changes(
          in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
          in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
          in_test                => TRUE
        );
      END IF;

      -- Terminate the waiting loop and set status to pending
      IF NOT g_params.test.get_bool_value THEN
        cort_job_pkg.suspend_job(
          in_rec => in_job_rec
        );
        g_job_rec.sid := null;
      END IF;
      -- Continue execution in the backgrouond
    END IF;

    enable_policies(
      in_policy_arr => io_source_table_rec.policy_arr
    );

    -- execute all changes in the queue
    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
      in_test                => g_params.test.get_bool_value
    );

    -- update current_name in rename_rec

    -- deffered data copy

    IF g_params.data.get_value = 'DEFERRED_COPY' AND
       NOT io_source_table_rec.is_table_empty AND
       in_recreate_mode in (cort_comp_pkg.gc_result_recreate, cort_comp_pkg.gc_result_part_exchange)
    THEN
      l_frwd_stmt_arr.DELETE;
      l_rlbk_stmt_arr.DELETE;
      out_last_ddl_index := io_frwd_stmt_arr.COUNT;
      copy_data(
        in_source_table_rec => io_source_table_rec,
        in_target_table_rec => io_target_table_rec,
        in_recreate_mode    => in_recreate_mode,
        in_filter_sql       => in_data_filter,
        io_frwd_stmt_arr    => l_frwd_stmt_arr,
        io_rlbk_stmt_arr    => l_rlbk_stmt_arr
      );
      FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
      END LOOP;
    END IF;

    -- for CREATE AS SELECT FROM itself
    IF (in_recreate_mode = cort_comp_pkg.gc_result_cas_from_itself) AND NOT g_params.test.get_bool_value THEN
      -- resume pending job to complete it in main procedure
      cort_job_pkg.success_job(
        in_rec => in_job_rec
      );
    END IF;

    stop_timer;
  END recreate_table;

  -- Parses SQL syntax. Do not call for DDL!!! It executes DDL!
  FUNCTION is_sql_valid(in_sql IN CLOB)
  RETURN VARCHAR2
  AS
    l_sql_arr dbms_sql.varchar2a;
    l_cursor  NUMBER;
    l_result  VARCHAR2(4000);
  BEGIN
    cort_aux_pkg.clob_to_varchar2a(in_sql, l_sql_arr);
    l_cursor := dbms_sql.open_cursor;
    BEGIN
      dbms_sql.parse(
        c             => l_cursor,
        statement     => l_sql_arr,
        lb            => 1,
        ub            => l_sql_arr.COUNT,
        lfflg         => FALSE,
        language_flag => dbms_sql.native
      );
      l_result := NULL;
    EXCEPTION
      WHEN OTHERS THEN
        l_result := sqlerrm;
        IF dbms_sql.is_open(l_cursor) THEN
          dbms_sql.close_cursor(l_cursor);
        END IF;
    END;
    IF dbms_sql.is_open(l_cursor) THEN
      dbms_sql.close_cursor(l_cursor);
    END IF;
    RETURN l_result;
  END is_sql_valid;

  -- Check for all columns cort values
  PROCEDURE check_cort_values(
    io_sql_rec       IN OUT NOCOPY cort_parse_pkg.gt_sql_rec,
    io_new_table_rec IN OUT NOCOPY gt_table_rec,
    in_old_table_rec IN gt_table_rec
  )
  AS
    l_data_source  CLOB;
    l_sql          CLOB;
    l_sql_errm     VARCHAR2(4000);
  BEGIN
    IF io_sql_rec.data_source IS NOT NULL THEN
      l_data_source := '('||io_sql_rec.data_source||') a';
      l_sql := 'SELECT * FROM '||l_data_source;
      l_sql_errm := is_sql_valid(l_sql);
      IF l_sql_errm IS NOT NULL THEN
        debug('Invalid data source sql - '||l_sql_errm, l_sql);
        cort_log_pkg.echo('Warning! Invalid data source sql - '||l_sql_errm);
        io_sql_rec.data_source := null;
        l_data_source := '"'||in_old_table_rec.owner||'"."'||in_old_table_rec.rename_rec.current_name||'" '||g_params.alias.get_value;
      END IF;
    ELSIF io_sql_rec.data_filter IS NOT NULL THEN
      l_data_source := '"'||in_old_table_rec.owner||'"."'||in_old_table_rec.rename_rec.current_name||'" '||g_params.alias.get_value||CHR(10)||io_sql_rec.data_filter;
      l_sql := 'SELECT * FROM '||l_data_source;
      l_sql_errm := is_sql_valid(l_sql);
      IF l_sql_errm IS NOT NULL THEN
        debug('Invalid data filter sql - '||l_sql_errm, l_sql);
        cort_log_pkg.echo('Warning! Invalid data filter sql - '||l_sql_errm);
        io_sql_rec.data_filter := null;
        l_data_source := '"'||in_old_table_rec.owner||'"."'||in_old_table_rec.rename_rec.current_name||'" '||g_params.alias.get_value;
      END IF;
    ELSE
      l_data_source := '"'||in_old_table_rec.owner||'"."'||in_old_table_rec.rename_rec.current_name||'" '||g_params.alias.get_value;
    END IF;

    FOR i IN 1..io_new_table_rec.column_arr.COUNT LOOP
      IF io_new_table_rec.column_arr(i).cort_value IS NOT NULL THEN
        l_sql := 'SELECT '||io_new_table_rec.column_arr(i).cort_value||' FROM '||l_data_source;
        l_sql_errm := is_sql_valid(l_sql);
        IF l_sql_errm IS NOT NULL THEN
          debug('Invalid cort value on column - '||io_new_table_rec.column_arr(i).column_name||' - '||l_sql_errm, l_sql);
          cort_log_pkg.echo('Warning! invalid cort value on column '||io_new_table_rec.column_arr(i).column_name||' - '||l_sql_errm);
          io_new_table_rec.column_arr(i).cort_value := NULL;
        END IF;
      END IF;
    END LOOP;


  END check_cort_values;


  -- find indexes used for PK/UK and set contsraint_name property for them
  PROCEDURE mark_pk_uk_indexes(
    io_source_table_rec IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_indx       PLS_INTEGER;
    l_index_name VARCHAR2(65);
  BEGIN
    -- find renaming columns
    FOR i IN 1..in_target_table_rec.index_arr.COUNT LOOP
      IF in_target_table_rec.index_arr(i).constraint_name IS NOT NULL THEN
        l_index_name := '"'||in_target_table_rec.index_arr(i).owner||'"."'||in_target_table_rec.index_arr(i).rename_rec.object_name||'"';
        IF io_source_table_rec.index_indx_arr.EXISTS(l_index_name) THEN
          l_indx := io_source_table_rec.index_indx_arr(l_index_name);
          IF io_source_table_rec.index_arr(l_indx).constraint_name IS NULL THEN
            io_source_table_rec.index_arr(l_indx).constraint_name := in_target_table_rec.index_arr(i).constraint_name;
          END IF;
        END IF;
      END IF;
    END LOOP;
  END mark_pk_uk_indexes;


  -- find renaming columns and set new_column_name property for them
  PROCEDURE find_renaming_columns(
    io_source_table_rec IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_cort_value  VARCHAR2(32767);
    l_name1       VARCHAR2(4000);
    l_name2       VARCHAR2(4000);
    l_name3       VARCHAR2(4000);
    l_name4       VARCHAR2(4000);
    l_col_name    arrays.gt_name;
    l_pos         PLS_INTEGER;
  BEGIN
    -- find renaming columns
    FOR i IN 1..in_target_table_rec.column_arr.COUNT LOOP
      IF in_target_table_rec.column_arr(i).cort_value IS NOT NULL THEN
        l_cort_value := TRIM(in_target_table_rec.column_arr(i).cort_value);

        l_name1 := NULL;
        l_name2 := NULL;
        l_name3 := NULL;
        l_name4 := NULL;
        l_pos   := NULL;

        BEGIN
          dbms_utility.name_tokenize(l_cort_value, l_name1, l_name2, l_name3, l_name4, l_pos);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;

        IF l_pos = LENGTH(l_cort_value) THEN

          IF l_name1 = io_source_table_rec.owner AND
             l_name2 = io_source_table_rec.table_name
          THEN
            l_col_name := l_name3;
          ELSIF l_name1 = io_source_table_rec.table_name OR
                l_name1 = g_params.alias.get_value
          THEN
            l_col_name := l_name2;
          ELSE
            l_col_name := l_name1;
          END IF;

          IF io_source_table_rec.column_indx_arr.EXISTS(l_col_name) AND
             NOT in_target_table_rec.column_indx_arr.EXISTS(l_col_name)
          THEN
            l_pos := io_source_table_rec.column_indx_arr(l_col_name);
            io_source_table_rec.column_arr(l_pos).new_column_name := in_target_table_rec.column_arr(i).column_name;
            -- update reference by name
            io_source_table_rec.column_indx_arr.DELETE(l_col_name);
            io_source_table_rec.column_indx_arr(io_source_table_rec.column_arr(l_pos).new_column_name) := l_pos;
            debug('Column '||io_source_table_rec.column_arr(l_pos).column_name||': new column name = '||io_source_table_rec.column_arr(l_pos).new_column_name);
          END IF;

        END IF;
      END IF;
    END LOOP;
  END find_renaming_columns;

  -- compare tables
  FUNCTION comp_tables(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result               PLS_INTEGER;
    l_comp_result          PLS_INTEGER;
    l_stop_vaue            PLS_INTEGER;
    l_comp_partitions      BOOLEAN;
  BEGIN
    start_timer;

    l_result := cort_comp_pkg.gc_result_nochange;
    IF g_params.change.get_value = 'RECREATE' THEN
      l_stop_vaue := cort_comp_pkg.gc_result_alter;
    ELSE
      l_stop_vaue := cort_comp_pkg.gc_result_exchange;
    END IF;

    -- If teher is at least one force value then always recreate table
    FOR i IN 1..io_target_table_rec.column_arr.COUNT LOOP
      IF io_target_table_rec.column_arr(i).cort_value IS NOT NULL AND
         io_target_table_rec.column_arr(i).cort_value_force
      THEN
        debug('There is force cort-value - recreate');
        l_result := cort_comp_pkg.gc_result_recreate;
        EXIT;
      END IF;
    END LOOP;

    debug('Compare start - '||l_result);
    -- compare table attributes
    IF l_result < l_stop_vaue THEN
      -- compare main table attributes
      l_result := cort_comp_pkg.comp_tables(
                    in_source_table_rec    => io_source_table_rec,
                    in_target_table_rec    => io_target_table_rec,
                    io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                  );
      debug('Compare table attributes - '||l_result);
    END IF;

    -- if table attributes are the same then compare columns
    IF (l_result < l_stop_vaue) OR
       (io_source_table_rec.partitioned = 'YES' AND io_target_table_rec.partitioned = 'YES')
    THEN
      -- compare columns
      l_comp_result := cort_comp_pkg.comp_table_columns(
                         io_source_table_rec    => io_source_table_rec,
                         io_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare columns - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);

      -- compares column name dependent table attributes
      l_comp_result := cort_comp_pkg.comp_table_col_attrs(
                         in_source_table_rec => io_source_table_rec,
                         in_target_table_rec => io_target_table_rec
                       );
      debug('Compare col attributes - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;

    -- constraints
    IF l_result < l_stop_vaue THEN
      l_comp_result := cort_comp_pkg.comp_constraints(
                         io_source_table_rec      => io_source_table_rec,
                         io_target_table_rec      => io_target_table_rec,
                         io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
                       );
      debug('Compare constraints - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- logging groups
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_log_groups(
                         io_source_table_rec      => io_source_table_rec,
                         io_target_table_rec      => io_target_table_rec,
                         io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
                       );
      debug('Compare logging groups - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- lobs
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_lobs(
                         in_source_table_rec    => io_source_table_rec,
                         in_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare lobs - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- xml columns
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_xml_columns(
                         in_source_table_rec    => io_source_table_rec,
                         in_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare xml cols - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- xml columns
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_varray_columns(
                         in_source_table_rec    => io_source_table_rec,
                         in_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare varray cols - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;

    debug('Compare result - '||l_result);

    stop_timer;

    RETURN l_result;
  END comp_tables;

  -- lock table in exclusive mode. Return TRUE if lock acquired. Return FALSE if table not found. Otherwise raise unhandled exception
  FUNCTION lock_table(
    in_table_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_sql                  VARCHAR2(1000);
    l_result               BOOLEAN;
    e_table_not_found      EXCEPTION;
    e_resource_busy        EXCEPTION;
    e_has_errors           EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_not_found, -942);
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
    PRAGMA EXCEPTION_INIT(e_has_errors, -4063);
  BEGIN
    -- obtain table lock
    l_sql := 'LOCK TABLE "'||in_owner||'"."'||in_table_name||'" IN EXCLUSIVE MODE NOWAIT';
    BEGIN
      execute_immediate(l_sql, NULL);
      l_result := TRUE;
    EXCEPTION
      WHEN e_table_not_found THEN
        l_result := FALSE;
      WHEN e_resource_busy THEN
        raise_error('Table is used by another session(s)');
      WHEN e_has_errors THEN
        exec_drop_table(
          in_table_name   => in_table_name,
          in_owner        => in_owner,
          in_echo         => TRUE,
          in_test         => g_params.test.get_bool_value
        );
        l_result := FALSE;
    END;
    RETURN l_result;
  END lock_table;

  -- lock table in exclusive mode. Return TRUE if lock acquired. Return FALSE if table not found. Otherwise raise unhandled exception
  FUNCTION check_locked_table(
    in_table_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_sql                  VARCHAR2(1000);
    l_result               BOOLEAN;
    e_table_not_found      EXCEPTION;
    e_resource_busy        EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_not_found, -942);
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
  BEGIN
    -- obtain table lock
    l_sql := 'LOCK TABLE "'||in_owner||'"."'||in_table_name||'" IN EXCLUSIVE MODE NOWAIT';
    BEGIN
      execute_immediate(l_sql, NULL);
      l_result := TRUE;
    EXCEPTION
      WHEN e_table_not_found THEN
        l_result := FALSE;
      WHEN e_resource_busy THEN
        l_result := TRUE;
    END;
    RETURN l_result;
  END check_locked_table;

  PROCEDURE backup_table(
    in_table_name    IN VARCHAR2,
    in_table_owner   IN VARCHAR2,
    in_sql_rec       IN cort_parse_pkg.gt_sql_rec,
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_source_table_rec  gt_table_rec;
    l_target_table_rec  gt_table_rec;
  BEGIN
    start_timer;
    -- read table attributes
    read_table_cascade(
      in_table_name => in_table_name,
      in_owner      => in_table_owner,
      in_read_data  => g_params.data.get_value <> 'NONE',
      out_table_rec => l_source_table_rec
    );
    -- create temp table with the same structure
    cort_comp_pkg.create_clone_table_sql(
      in_table_rec       => l_source_table_rec,
      in_simple_mode     => TRUE,
      io_frwd_stmt_arr   => io_frwd_stmt_arr,
      io_rlbk_stmt_arr   => io_rlbk_stmt_arr
    );
    l_target_table_rec := l_source_table_rec;
    l_target_table_rec.rename_rec.current_name := l_source_table_rec.rename_rec.temp_name;

    IF g_params.data.get_value <> 'NONE' AND
       NOT l_source_table_rec.is_table_empty
    THEN
      copy_data(
        in_source_table_rec => l_source_table_rec,
        in_target_table_rec => l_target_table_rec,
        in_recreate_mode    => cort_comp_pkg.gc_result_recreate,
        in_filter_sql       => in_sql_rec.data_filter,
        io_frwd_stmt_arr    => io_frwd_stmt_arr,
        io_rlbk_stmt_arr    => io_rlbk_stmt_arr
      );
    END IF;
    copy_privileges(
      io_source_table_rec => l_source_table_rec,
      io_target_table_rec => l_target_table_rec,
      io_frwd_stmt_arr    => io_frwd_stmt_arr,
      io_rlbk_stmt_arr    => io_rlbk_stmt_arr
    );
    cort_comp_pkg.add_stmt(
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr,
      in_frwd_stmt     => get_drop_table_ddl(
                            in_table_name => in_table_name,
                            in_owner      => in_table_owner
                          ),
      in_rlbk_stmt     => NULL
    );

    cort_comp_pkg.rename_object(
      in_rename_mode   => 'TO_ORIGINAL',
      io_rename_rec    => l_target_table_rec.rename_rec,
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );
    -- remove rollback renaming
    io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT) := NULL;

    stop_timer;
  END backup_table;

  PROCEDURE read_data_source(
    in_data_source          IN CLOB,
    out_data_source_tab_rec OUT NOCOPY gt_table_rec
  )
  AS
    l_sql                  CLOB;
    l_rowid                ROWID;
    l_object_id            NUMBER;
    l_table_owner          arrays.gt_name;
    l_table_name           arrays.gt_name;
  BEGIN
    --l_data_source_sql_rec := cort_parse_pkg.parse_sql(io_sql_rec.data_source);
    --cort_parse_pkg.find_data_source_table

    -- check if custom data source is key-preserve table
    -- to do that we try to select rowid for any row from this data set. If it is returned then it is key preserved. Otherwise it will raise an exception
    l_sql := '
    SELECT /*+ first_rows */ rowid as row_id
      FROM ('||in_data_source||')
     WHERE ROWNUM = 1';

    cort_log_pkg.execute(
      in_text      => l_sql
    );
    BEGIN
      EXECUTE IMMEDIATE l_sql INTO l_rowid;
    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.update_exec_time;
        cort_log_pkg.error('Error in parsing/executing data source sql',l_sql);
        l_rowid := NULL;
    END;
    cort_log_pkg.update_exec_time;

    -- if rowid is returned then we can identify table name by it
    IF l_rowid IS NOT NULL THEN
      l_object_id := dbms_rowid.rowid_object(l_rowid);
      SELECT owner, object_name
        INTO l_table_owner, l_table_name
        FROM all_objects
       WHERE object_id = l_object_id;

       read_table(
         in_table_name      => l_table_name,
         in_owner           => l_table_owner,
         in_read_data       => FALSE,
         out_table_rec      => out_data_source_tab_rec
       );
       read_partitions(
         io_table_rec       => out_data_source_tab_rec,
         in_read_data       => FALSE
       );
       read_subpartitions(
         io_table_rec       => out_data_source_tab_rec,
         in_read_data       => FALSE
       );
    END IF;

  END read_data_source;

  FUNCTION get_partitions_sql(
    in_old_table_rec         IN gt_table_rec,
    in_new_table_rec         IN gt_table_rec
  )
  RETURN CLOB
  AS
    l_partitions_sql        CLOB;
    l_src_part_table_rec    gt_table_rec;  
    l_schema                VARCHAR2(128); 
    l_table_name            VARCHAR2(128); 
    l_part2                 VARCHAR2(128);
    l_dblink                VARCHAR2(128); 
    l_part1_type            VARCHAR2(128);
    l_object_number         VARCHAR2(128);
  BEGIN
    IF g_params.partitions_source.get_value IS NULL THEN -- copy from the object 
      IF cort_comp_pkg.comp_partitioning(in_old_table_rec, in_new_table_rec) = 0 THEN
        IF g_params.copy_subpartitions.get_bool_value AND in_old_table_rec.subpartitioning_type <> 'NONE' AND l_src_part_table_rec.subpartitioning_type <> 'NONE' THEN 
          IF cort_comp_pkg.comp_subpartitioning(in_old_table_rec, l_src_part_table_rec) = 0 THEN
            -- generate SQL for partitions definitions from source table
            l_partitions_sql := cort_comp_pkg.get_partitions_sql(
                                  in_partition_arr    => in_old_table_rec.partition_arr,
                                  in_subpartition_arr => in_old_table_rec.subpartition_arr 
                                );
           ELSE
             debug('New table has different type of subpartitioning - '||in_new_table_rec.subpartitioning_type);
           END IF;                 
        ELSE
          l_partitions_sql := cort_comp_pkg.get_partitions_sql(
                                in_partition_arr    => in_old_table_rec.partition_arr,
                                in_subpartition_arr => l_src_part_table_rec.subpartition_arr -- <- empty collection
                              );
        
        END IF;
      ELSE
        debug('New table has different type of partitioning - '||in_new_table_rec.partitioning_type);  
      END IF;                    
    ELSIF g_params.partitions_source.get_value <> '""' THEN
      -- parsing source object name. If it is invalid the exception will be raised 
      BEGIN
        dbms_utility.name_resolve(
          name          => g_params.partitions_source.get_value, 
          context       => 0,
          schema        => l_schema, 
          part1         => l_table_name, 
          part2         => l_part2,
          dblink        => l_dblink, 
          part1_type    => l_part1_type, 
          object_number => l_object_number
        );
      EXCEPTION
        WHEN OTHERS THEN
          raise_error('Invalid table name specified in partition_source param'||chr(10)||sqlerrm);
      END;
      read_table(
        in_table_name => l_table_name,
        in_owner      => l_schema,
        in_read_data  => FALSE,
        out_table_rec => l_src_part_table_rec
      );
      read_table_columns(
        io_table_rec => l_src_part_table_rec
      );
      -- read source partitions
      IF cort_comp_pkg.comp_partitioning(in_new_table_rec, l_src_part_table_rec, FALSE) = 0 THEN
        -- check that all partition key columns data types are matching
        read_partitions(l_src_part_table_rec, FALSE);
        IF g_params.copy_subpartitions.get_bool_value AND in_new_table_rec.subpartitioning_type <> 'NONE' AND l_src_part_table_rec.subpartitioning_type <> 'NONE' THEN 
          IF cort_comp_pkg.comp_subpartitioning(in_new_table_rec, l_src_part_table_rec, FALSE) = 0 THEN 
            debug('Replace subpartitions: g_params.copy_subpartitions = TRUE');
            -- read source subpartitions
            read_subpartitions(l_src_part_table_rec, FALSE);
          ELSE
            debug('partition source '||g_params.partitions_source.get_value||' has different type of subpartitioning - '||l_src_part_table_rec.subpartitioning_type);
          END IF;    
        END IF;
      ELSE
        raise_error('partition source '||g_params.partitions_source.get_value||' has different type of partitioning - '||l_src_part_table_rec.partitioning_type);  
      END IF;  
             
      -- generate SQL for partitions definitions from another table
      l_partitions_sql := cort_comp_pkg.get_partitions_sql(
                            in_partition_arr    => l_src_part_table_rec.partition_arr,
                            in_subpartition_arr => l_src_part_table_rec.subpartition_arr
                          );
                 
    END IF;
    
    debug('l_partitions_sql = '||l_partitions_sql);
   
    RETURN l_partitions_sql;  
   
  END get_partitions_sql;
  
  -- create or replace table
  PROCEDURE create_or_replace_table(
    in_job_rec       IN cort_jobs%ROWTYPE,
    io_sql_rec       IN OUT NOCOPY cort_parse_pkg.gt_sql_rec
  )
  AS
    l_new_sql_rec          cort_parse_pkg.gt_sql_rec;
    l_new_sql              CLOB;
    l_partitions_sql       CLOB;

    l_old_table_rec        gt_table_rec;
    l_new_table_rec        gt_table_rec;
    l_subpart_arr          gt_partition_arr;

    l_create_ddl_arr       arrays.gt_clob_arr;
    l_drop_ddl_arr         arrays.gt_clob_arr;

    l_frwd_alter_stmt_arr  arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr  arrays.gt_clob_arr;

    l_result               PLS_INTEGER;

    l_xml                  XMLTYPE;

    l_revert_name          arrays.gt_name;
    l_last_ddl_index       PLS_INTEGER;

    l_index_sql_arr        cort_parse_pkg.gt_sql_arr;
    l_index_sql_rec        cort_parse_pkg.gt_sql_rec;
    l_index_rec            gt_index_rec;

    l_data_source_sql_rec  cort_parse_pkg.gt_sql_rec;
    l_data_source_tab_rec  gt_table_rec;

    l_comp_partitions      BOOLEAN;
    l_comp_result          PLS_INTEGER;
    l_policy_arr           gt_policy_arr;
  BEGIN
    -- read exsting policies
    read_table_policies(
      in_table_name  => in_job_rec.object_name,
      in_owner       => in_job_rec.object_owner,
      out_policy_arr => l_policy_arr
    );

    BEGIN
      -- disable existing policies
      enable_policies(
        in_policy_arr => l_policy_arr,
        in_enable     => false
      );

      -- try to obtain exclusive lock
      IF lock_table(
           in_table_name  => in_job_rec.object_name,
           in_owner       => in_job_rec.object_owner
         )
      THEN
        -- read table attributes
        read_table_cascade(
          in_table_name => in_job_rec.object_name,
          in_owner      => in_job_rec.object_owner,
          in_read_data  => TRUE,
          out_table_rec => l_old_table_rec
        );
        l_old_table_rec.policy_arr := l_policy_arr;

        IF g_params.debug.get_bool_value THEN
          -- convert table rec into XML
          cort_xml_pkg.write_to_xml(
            in_value => l_old_table_rec,
            out_xml  => l_xml
          );

          -- log source table rec
          cort_log_pkg.debug(
            in_text     => 'OLD_TABLE_REC',
            in_details  => l_xml.getClobVal()
          );
        END IF;

        -- modify original SQL - replace original names with temp ones.
        cort_parse_pkg.replace_names(
          in_table_rec => l_old_table_rec,
          io_sql_rec   => io_sql_rec,
          out_sql      => l_new_sql
        );

        l_new_sql_rec := cort_parse_pkg.parse_sql(l_new_sql);

        -- parse modified sql
        cort_parse_pkg.parse_table_sql(
          io_sql_rec    => l_new_sql_rec,
          in_table_name => l_old_table_rec.rename_rec.temp_name,
          in_owner_name => in_job_rec.object_owner
        );

        -- search and parse "AS SELECT" section
        cort_parse_pkg.parse_as_select(
          io_sql_rec => l_new_sql_rec
        );

        $IF dbms_db_version.version >= 12 OR dbms_db_version.ver_le_11_2 $THEN
          IF NOT g_params.tablespace.is_empty THEN
            execute_immediate('ALTER SESSION SET DEFERRED_SEGMENT_CREATION=TRUE', NULL);
          END IF;
        $END

        -- save DDL into array before any modification for TEST mode in case of CREATE AS SELECT
        l_create_ddl_arr(1) := l_new_sql_rec.original_sql;
        -- save DROP table sql for revert in case go ahead with this change
        l_drop_ddl_arr(1)   := get_drop_table_ddl(
                                 in_table_name => l_old_table_rec.rename_rec.temp_name,
                                 in_owner      => l_old_table_rec.owner
                               );

        -- if "CREATE AS SELECT"  and test mode enabled
        IF l_new_sql_rec.as_select_flag AND g_params.test.get_bool_value THEN
          -- save original SQL for display
          -- modify sql: make subquery return 0 rows
          cort_parse_pkg.modify_as_select(
            io_sql_rec  => l_new_sql_rec
          );
        END IF;

        -- Finds index declarations in cort comments
        cort_parse_pkg.get_cort_indexes(
          in_sql_rec   => l_new_sql_rec,
          in_job_rec   => in_job_rec,
          out_sql_arr  => l_index_sql_arr
        );

        -- execute modified sql
        execute_immediate(
          in_sql  => l_new_sql_rec.original_sql,
          in_echo => FALSE
        );

        l_new_sql := null;
        IF l_index_sql_arr IS NOT NULL THEN
          start_timer;
          FOR i IN 1..l_index_sql_arr.COUNT LOOP
            l_index_sql_rec := l_index_sql_arr(i);
            -- modify original SQL - replace original names with temp ones.
            l_index_rec.owner := l_index_sql_rec.object_owner;
            l_index_rec.index_name := l_index_sql_rec.object_name;
            l_index_rec.rename_rec.temp_name := get_object_temp_name(
                                                   in_object_type => 'INDEX',
                                                   in_owner       => l_index_sql_rec.object_owner,
                                                   in_prefix      => cort_params_pkg.gc_temp_prefix
                                                 );
            debug('Index original DDL', l_index_sql_rec.original_sql);

            cort_parse_pkg.replace_names(
              in_table_rec => l_old_table_rec,
              in_index_rec => l_index_rec,
              io_sql_rec   => l_index_sql_rec,
              out_sql      => l_new_sql
            );

            execute_immediate(
              in_sql  => l_new_sql,
              in_echo => FALSE
            );

            cort_comp_pkg.add_stmt(
              io_frwd_stmt_arr => l_create_ddl_arr,
              io_rlbk_stmt_arr => l_drop_ddl_arr,
              in_frwd_stmt     => l_new_sql,
              in_rlbk_stmt     => get_drop_index_ddl(
                                     in_index_name => l_index_rec.rename_rec.temp_name,
                                     in_owner      => l_index_rec.owner
                                   )
            );
          END LOOP;
          stop_timer;
        END IF;

        l_last_ddl_index := l_create_ddl_arr.COUNT;

        BEGIN
          -- read new table attributes
          read_table_cascade(
            in_table_name => l_old_table_rec.rename_rec.temp_name,
            in_owner      => in_job_rec.object_owner,
            in_read_data  => FALSE,
            out_table_rec => l_new_table_rec
          );

          -- Comment temp table
          cort_comp_pkg.add_stmt(
            io_frwd_stmt_arr => l_create_ddl_arr,
            io_rlbk_stmt_arr => l_drop_ddl_arr,
            in_frwd_stmt     => 'COMMENT ON TABLE "'||l_new_table_rec.owner||'"."'||l_new_table_rec.table_name||'" IS Q''{CORT temp table for table '||l_old_table_rec.table_name||'}''',
            in_rlbk_stmt     => null
          );
          IF g_params.keep_objects.value_exists('PRIVILEGES') THEN
            copy_privileges(
              io_source_table_rec => l_old_table_rec,
              io_target_table_rec => l_new_table_rec,
              io_frwd_stmt_arr    => l_create_ddl_arr,
              io_rlbk_stmt_arr    => l_drop_ddl_arr
            );
          END IF;

          -- execute changes without displaying
          apply_changes(
            in_frwd_alter_stmt_arr => l_create_ddl_arr,
            in_rlbk_alter_stmt_arr => l_drop_ddl_arr,
            io_last_ddl_index      => l_last_ddl_index,
            in_echo                => FALSE
          );

          -- assign column cort_values
          cort_parse_pkg.parse_columns(
            io_sql_rec    => l_new_sql_rec,
            io_table_rec  => l_new_table_rec
          );

          -- find valid cort values
          check_cort_values(
            io_sql_rec       => io_sql_rec,
            io_new_table_rec => l_new_table_rec,
            in_old_table_rec => l_old_table_rec
          );

          IF g_params.debug.get_bool_value THEN
            -- convert target table recinto XML
            cort_xml_pkg.write_to_xml(
              in_value => l_new_table_rec,
              out_xml  => l_xml
            );

            -- log target table rec
            cort_log_pkg.debug(
              in_text     => 'NEW_TABLE_REC',
              in_details  => l_xml.getClobVal()
            );
          END IF;

          -- mark all check constraints implicitly created from NOT NULL column constraint
          mark_notnull_constraints(
            io_constraint_arr => l_old_table_rec.constraint_arr,
            io_column_arr     => l_old_table_rec.column_arr
          );
          --
          mark_notnull_constraints(
            io_constraint_arr => l_new_table_rec.constraint_arr,
            io_column_arr     => l_new_table_rec.column_arr
          );

          -- mark all indexes explicitly used for PK/UK constraints
          mark_pk_uk_indexes(
            io_source_table_rec    => l_old_table_rec,
            in_target_table_rec    => l_new_table_rec
          );

          -- find columns to rename
          find_renaming_columns(
            io_source_table_rec    => l_old_table_rec,
            in_target_table_rec    => l_new_table_rec
          );

          l_result := cort_comp_pkg.gc_result_nochange;

          IF g_params.change.get_value = 'ALWAYS' THEN
            l_result := cort_comp_pkg.gc_result_recreate;
          ELSIF l_new_sql_rec.as_select_flag THEN
            IF cort_parse_pkg.as_select_from(l_new_sql_rec, l_old_table_rec.table_name) THEN
              l_result := cort_comp_pkg.gc_result_cas_from_itself;
            ELSE
              l_result := cort_comp_pkg.gc_result_create_as_select;
            END IF;
            debug('Create as select change type = '||l_result);
          ELSE
            -- compare tables
            l_result := cort_comp_pkg.gc_result_nochange;

            l_result := comp_tables(
                          io_source_table_rec    => l_old_table_rec,
                          io_target_table_rec    => l_new_table_rec,
                          io_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
                          io_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr
                        );
          END IF;

          debug('partitions_source = '||g_params.partitions_source.get_value);

          IF l_new_table_rec.partitioned = 'YES' AND l_new_table_rec.partition_arr.count = 1 AND 
             (g_params.partitions_source.get_value IS NULL  -- NULL identify copying existing partitions 
             OR 
             g_params.partitions_source.get_value <> '""')  -- "" disables copying partitions. Other values identify table name where partitions need to be copied from 
          THEN
            IF l_result > cort_comp_pkg.gc_result_alter_move THEN
              debug('Need to retreive partitions');

              -- need to replace partitions
              l_partitions_sql := get_partitions_sql(
                                    in_old_table_rec    => l_old_table_rec,
                                    in_new_table_rec    => l_new_table_rec
                                  );

               debug('l_partitions_sql = '||l_partitions_sql);
              -- compare partitions
              IF l_partitions_sql IS NOT NULL THEN
                -- drop temp table
                execute_immediate(
                  in_sql  => l_drop_ddl_arr(1),
                  in_echo => FALSE
                );
                -- parse partitioning types, positions
                cort_parse_pkg.parse_partitioning(
                  io_sql_rec => l_new_sql_rec
                );
                -- replace partitions
                cort_parse_pkg.replace_partitions_sql(
                  io_sql_rec       => l_new_sql_rec,
                  in_partition_sql => l_partitions_sql
                );
                -- replace DDL
                l_create_ddl_arr(1) := l_new_sql_rec.original_sql;

                -- reapply all changes: recraete table, comments policies and indexes
                apply_changes(
                  in_frwd_alter_stmt_arr => l_create_ddl_arr,
                  in_rlbk_alter_stmt_arr => l_drop_ddl_arr,
                  in_echo                => FALSE
                );
                -- read partitions
                read_partitions(l_new_table_rec, FALSE);
                -- read subpartitions
                read_subpartitions(l_new_table_rec, FALSE);
                l_comp_partitions := FALSE;    
              ELSE
                l_comp_partitions := TRUE;    
              END IF;
            ELSE
              l_comp_partitions := FALSE;    
            END IF;
          ELSE
            l_comp_partitions := l_new_table_rec.partitioned = 'YES';    
          END IF;    
            
          IF l_comp_partitions THEN
            -- Need to run even if we use keep_partitions to match all partitions to ech other
            l_comp_result := cort_comp_pkg.comp_partitions(
                               io_source_table_rec     => l_old_table_rec,
                               io_target_table_rec     => l_new_table_rec,
                               io_source_partition_arr => l_old_table_rec.partition_arr,
                               io_target_partition_arr => l_new_table_rec.partition_arr,
                               io_frwd_alter_stmt_arr  => l_frwd_alter_stmt_arr,
                               io_rlbk_alter_stmt_arr  => l_rlbk_alter_stmt_arr
                             );
          ELSE
            l_comp_result := 0;
          END IF;
          debug('Compare partitions - '||l_comp_result);
          l_result := GREATEST(l_result, l_comp_result);

          IF io_sql_rec.data_filter IS NOT NULL THEN
            debug('Data filter is not null so we need to recreate table...');
            l_result := cort_comp_pkg.gc_result_recreate;
          END IF;

          IF l_result = cort_comp_pkg.gc_result_part_exchange AND g_params.data.get_value = 'PRELIMINARY_COPY' THEN
            l_result := cort_comp_pkg.gc_result_recreate;
          END IF;

          IF l_result IN (cort_comp_pkg.gc_result_nochange, cort_comp_pkg.gc_result_alter, cort_comp_pkg.gc_result_alter_move) THEN
            -- Start compare indexes
            debug('Compare indexes...');
            IF cort_comp_pkg.comp_indexes(
                 in_source_table_rec    => l_old_table_rec,
                 in_target_table_rec    => l_new_table_rec,
                 io_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
                 io_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr
               ) <> cort_comp_pkg.gc_result_nochange
            THEN
              l_result := cort_comp_pkg.gc_result_alter;
            END IF;
          END IF;

          -- reset temp comment
          execute_immediate(
            in_sql  => 'COMMENT ON TABLE "'||l_new_table_rec.owner||'"."'||l_new_table_rec.table_name||'" IS Q''{}''',
            in_echo => FALSE
          );

        EXCEPTION
          WHEN OTHERS THEN
            cort_log_pkg.error('Error. Rolling back...');
            -- drop temp objects
            apply_changes_ignore_errors(
              io_stmt_arr => l_drop_ddl_arr,
              in_echo     => FALSE,
              in_reverse  => TRUE
            );
            RAISE;
        END;

        debug('Change type as result of comparison = '||cort_comp_pkg.get_result_name(l_result));

        CASE g_params.change.get_value
          WHEN 'ALWAYS' THEN
            l_result := cort_comp_pkg.gc_result_recreate;
          WHEN 'RECREATE' THEN
            IF l_result > cort_comp_pkg.gc_result_nochange THEN
              l_result := cort_comp_pkg.gc_result_recreate;
            END IF;
          WHEN 'ALTER' THEN
            IF l_result > cort_comp_pkg.gc_result_alter_move THEN
              raise_error('Changes could not be applied without table recreation');
            END IF;
          ELSE
            NULL;
        END CASE;

        debug('Change type after applying change param = '||cort_comp_pkg.get_result_name(l_result));

        BEGIN
          CASE
          WHEN l_result = cort_comp_pkg.gc_result_nochange THEN
            debug('No changes ... ');

            -- drop temp objects
            apply_changes_ignore_errors(
              io_stmt_arr => l_drop_ddl_arr,
              in_echo     => FALSE,
              in_reverse  => TRUE
            );

          WHEN l_result IN (cort_comp_pkg.gc_result_alter, cort_comp_pkg.gc_result_alter_move) THEN
            debug('Do alters ... change type = '||cort_comp_pkg.get_result_name(l_result));

            -- drop temp objects
            apply_changes_ignore_errors(
              io_stmt_arr => l_drop_ddl_arr,
              in_echo     => FALSE,
              in_reverse  => TRUE
            );

            -- altering existing table
            apply_changes(
              in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
              in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr,
              in_test                => g_params.test.get_bool_value
            );

            IF NOT g_params.test.get_bool_value THEN
              cort_aux_pkg.register_change(
                in_object_owner   => in_job_rec.object_owner,
                in_object_type    => in_job_rec.object_type,
                in_object_name    => in_job_rec.object_name,
                in_job_rec        => in_job_rec,
                in_sql            => in_job_rec.sql_text,
                in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                       in_object_owner   => in_job_rec.object_owner,
                                       in_object_name    => in_job_rec.object_name,
                                       in_object_type    => in_job_rec.object_type
                                     ),
                in_change_type    => l_result,
                in_revert_name    => NULL,
                in_last_ddl_index => l_frwd_alter_stmt_arr.COUNT,
                in_frwd_stmt_arr  => l_frwd_alter_stmt_arr,
                in_rlbk_stmt_arr  => l_rlbk_alter_stmt_arr
              );
            ELSE  
              cort_log_pkg.echo('== REVERT DDL ==');
              FOR i IN REVERSE 1..l_rlbk_alter_stmt_arr.COUNT LOOP
                cort_log_pkg.echo(
                  in_text => l_rlbk_alter_stmt_arr(i)
                );
                cort_log_pkg.revert(
                  in_text => l_rlbk_alter_stmt_arr(i)
                );
              END LOOP;
            END IF;

          WHEN l_result IN (cort_comp_pkg.gc_result_exchange, cort_comp_pkg.gc_result_part_exchange, cort_comp_pkg.gc_result_recreate, cort_comp_pkg.gc_result_create_as_select, cort_comp_pkg.gc_result_cas_from_itself) THEN
            debug('Do recreate ... change type = '||cort_comp_pkg.get_result_name(l_result));

            IF io_sql_rec.data_source IS NOT NULL THEN
              read_data_source(io_sql_rec.data_source, l_data_source_tab_rec);
            END IF;

            -- drop previous revert table
            l_revert_name := cort_aux_pkg.get_last_revert_name(
                                in_object_owner => in_job_rec.object_owner,
                                in_object_type  => 'TABLE',
                                in_object_name  => in_job_rec.object_name
                              );
            exec_drop_table(
              in_table_name => l_revert_name,
              in_owner      => in_job_rec.object_owner,
              in_test       => g_params.test.get_bool_value
            );

            -- display all already applied CREATE DDL (without execution)
            apply_changes(
              in_frwd_alter_stmt_arr => l_create_ddl_arr,
              in_rlbk_alter_stmt_arr => l_drop_ddl_arr,
              in_echo                => TRUE,
              in_test                => TRUE
            );

            -- Move data and depending objects and rename temp and original tables
            recreate_table(
              io_source_table_rec => l_old_table_rec,
              io_target_table_rec => l_new_table_rec,
              io_frwd_stmt_arr    => l_create_ddl_arr,
              io_rlbk_stmt_arr    => l_drop_ddl_arr,
              in_job_rec          => in_job_rec,
              in_data_filter      => io_sql_rec.data_filter,
              in_recreate_mode    => l_result,
              out_last_ddl_index  => l_last_ddl_index
            );

            enable_policies(
              in_policy_arr => l_policy_arr
            );

            l_revert_name := l_old_table_rec.rename_rec.cort_name;

            IF NOT g_params.test.get_bool_value THEN
              cort_aux_pkg.register_change(
                in_object_owner   => in_job_rec.object_owner,
                in_object_type    => in_job_rec.object_type,
                in_object_name    => in_job_rec.object_name,
                in_job_rec        => in_job_rec,
                in_sql            => in_job_rec.sql_text,
                in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                       in_object_owner   => in_job_rec.object_owner,
                                       in_object_name    => in_job_rec.object_name,
                                       in_object_type    => in_job_rec.object_type
                                     ),
                in_change_type    => l_result,
                in_revert_name    => l_revert_name,
                in_last_ddl_index => l_last_ddl_index,
                in_frwd_stmt_arr  => l_create_ddl_arr,
                in_rlbk_stmt_arr  => l_drop_ddl_arr
              );

              IF g_params.data.get_value = 'DEFERRED_COPY' AND
                 NOT l_old_table_rec.is_table_empty AND
                 l_result in (cort_comp_pkg.gc_result_recreate, cort_comp_pkg.gc_result_part_exchange)
              THEN
                cort_job_pkg.suspend_job(
                  in_rec => in_job_rec
                );
              END IF;

            ELSE
              cort_log_pkg.echo('== REVERT DDL ==');
              FOR i IN REVERSE 1..l_drop_ddl_arr.COUNT LOOP
                cort_log_pkg.echo(
                  in_text => l_drop_ddl_arr(i)
                );
                cort_log_pkg.revert(
                  in_text => l_drop_ddl_arr(i)
                );
              END LOOP;
              -- drop temp table
              execute_immediate(
                in_sql  => l_drop_ddl_arr(1),
                in_echo => FALSE
              );
            END IF;
          END CASE;

        EXCEPTION
          WHEN OTHERS THEN
            cort_log_pkg.error('Error. Rolling back...');
            -- drop temp table
            exec_drop_table(
              in_table_name => l_old_table_rec.rename_rec.temp_name,
              in_owner      => l_old_table_rec.owner,
              in_echo       => FALSE
            );
            RAISE;
        END;

      ELSE
        -- Finds index declarations in cort comments
        cort_parse_pkg.get_cort_indexes(
          in_sql_rec   => io_sql_rec,
          in_job_rec   => in_job_rec,
          out_sql_arr  => l_index_sql_arr
        );

              -- search and parse "AS SELECT" section
        cort_parse_pkg.parse_as_select(
          io_sql_rec => io_sql_rec
        );

        -- Table does not exist.
        -- Just execute SQL as is
        l_create_ddl_arr(1) := in_job_rec.sql_text;
        l_drop_ddl_arr(1)   := get_drop_table_ddl(
                                 in_table_name => in_job_rec.object_name,
                                 in_owner      => in_job_rec.object_owner
                               );

        IF l_index_sql_arr IS NOT NULL THEN
          FOR i IN 1..l_index_sql_arr.COUNT LOOP
            l_create_ddl_arr(l_create_ddl_arr.COUNT+1) := l_index_sql_arr(i).original_sql;
            l_drop_ddl_arr(l_drop_ddl_arr.COUNT+1) := get_drop_index_ddl(
                                                         in_index_name => l_index_sql_arr(i).object_name,
                                                         in_owner      => l_index_sql_arr(i).object_owner
                                                       );
          END LOOP;
        END IF;

        -- table doesn't exist. Create it as is.
        apply_changes(
          in_frwd_alter_stmt_arr => l_create_ddl_arr,
          in_rlbk_alter_stmt_arr => l_drop_ddl_arr,
          in_test                => g_params.test.get_bool_value
        );

        IF NOT g_params.test.get_bool_value THEN
          -- register table
          cort_aux_pkg.register_change(
            in_object_owner   => in_job_rec.object_owner,
            in_object_type    => in_job_rec.object_type,
            in_object_name    => in_job_rec.object_name,
            in_job_rec        => in_job_rec,
            in_sql            => in_job_rec.sql_text,
            in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                   in_object_owner   => in_job_rec.object_owner,
                                   in_object_name    => in_job_rec.object_name,
                                   in_object_type    => in_job_rec.object_type
                                 ),
            in_change_type    => case when io_sql_rec.as_select_flag then cort_comp_pkg.gc_result_create_as_select else cort_comp_pkg.gc_result_create end,
            in_revert_name    => NULL,
            in_last_ddl_index => l_create_ddl_arr.COUNT,
            in_frwd_stmt_arr  => l_create_ddl_arr,
            in_rlbk_stmt_arr  => l_drop_ddl_arr
          );
        ELSE
          cort_log_pkg.echo('== REVERT DDL ==');
          FOR i IN REVERSE 1..l_drop_ddl_arr.COUNT LOOP
                cort_log_pkg.echo(
                  in_text => l_drop_ddl_arr(i)
                );
                cort_log_pkg.revert(
                  in_text => l_drop_ddl_arr(i)
                );
          END LOOP;
        END IF;
      END IF;

      enable_policies(l_policy_arr);

      cort_session_pkg.enable;

      COMMIT; -- release table lock
    EXCEPTION
      WHEN OTHERS THEN
        enable_policies(l_policy_arr);
        RAISE;
    END;
  END create_or_replace_table;

  -- Create or replace sequence
  PROCEDURE create_or_replace_sequence(
    in_job_rec       IN cort_jobs%ROWTYPE,
    io_sql_rec       IN OUT NOCOPY cort_parse_pkg.gt_sql_rec
  )
  AS
    l_new_sql              CLOB;

    l_old_sequence_rec     gt_sequence_rec;
    l_new_sequence_rec     gt_sequence_rec;

    l_frwd_stmt_arr        arrays.gt_clob_arr;
    l_rlbk_stmt_arr        arrays.gt_clob_arr;

    l_result               PLS_INTEGER;
  BEGIN
    -- disable CORT trigger for current session
    cort_session_pkg.disable;

    -- read sequence attributes
    read_sequence(
      in_sequence_name => in_job_rec.object_name,
      in_owner         => in_job_rec.object_owner,
      out_sequence_rec => l_old_sequence_rec
    );
    IF l_old_sequence_rec.sequence_name IS NOT NULL THEN

      read_privileges(
        in_owner         => l_old_sequence_rec.owner,
        in_table_name    => l_old_sequence_rec.sequence_name,
        io_privilege_arr => l_old_sequence_rec.privilege_arr
      );

      l_new_sql := in_job_rec.sql_text;

      -- modify original SQL - replace original names with temp ones.
      cort_parse_pkg.replace_names(
        in_sequence_rec => l_old_sequence_rec,
        io_sql_rec      => io_sql_rec,
        out_sql         => l_new_sql
      );

      -- execute original sql with replaces names
      execute_immediate(l_new_sql, FALSE);

      l_result := cort_comp_pkg.gc_result_nochange;

      BEGIN
        -- read new sequence attributes
        read_sequence(
          in_sequence_name => l_old_sequence_rec.rename_rec.temp_name,
          in_owner         => in_job_rec.object_owner,
          out_sequence_rec => l_new_sequence_rec
        );

        -- compare sequences
        l_result := cort_comp_pkg.comp_sequences(
                      in_source_sequence_rec => l_old_sequence_rec,
                      in_target_sequence_rec => l_new_sequence_rec,
                      io_frwd_alter_stmt_arr => l_frwd_stmt_arr,
                      io_rlbk_alter_stmt_arr => l_rlbk_stmt_arr
                    );
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.error('Error. Rolling back...');
          -- drop temp sequence
          exec_drop_object(
            in_object_type  => 'SEQUENCE',
            in_object_name  => l_new_sequence_rec.rename_rec.current_name,
            in_object_owner => l_new_sequence_rec.owner,
            in_echo         => TRUE
          );
          RAISE;
      END;

      -- drop temp sequence
      exec_drop_object(
        in_object_type  => 'SEQUENCE',
        in_object_name  => l_new_sequence_rec.rename_rec.current_name,
        in_object_owner => l_new_sequence_rec.owner,
        in_echo         => TRUE
      );

      CASE
      WHEN l_result = cort_comp_pkg.gc_result_nochange THEN
        debug('Do nothing ...');
      WHEN l_result = cort_comp_pkg.gc_result_alter THEN
        -- drop temp sequence
        debug('Do alters ...');
        -- altering existing sequence

        apply_changes(
          in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
          in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
          in_test                => g_params.test.get_bool_value
        );

      WHEN l_result = cort_comp_pkg.gc_result_recreate THEN
        debug('Do recreate ...');

        l_frwd_stmt_arr.DELETE;
        l_rlbk_stmt_arr.DELETE;

        cort_comp_pkg.get_privileges_stmt(
          in_privilege_arr         => l_old_sequence_rec.privilege_arr,
          io_frwd_alter_stmt_arr   => l_rlbk_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_frwd_stmt_arr  -- GRANT privs for rollback
        );

        FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
          l_frwd_stmt_arr(i) := NULL;
        END LOOP;

        l_frwd_stmt_arr(l_frwd_stmt_arr.COUNT+1) := get_drop_object_ddl(
                                                      in_object_type  => 'SEQUENCE',
                                                      in_object_owner => l_old_sequence_rec.owner,
                                                      in_object_name  => l_old_sequence_rec.sequence_name
                                                    );
        l_rlbk_stmt_arr(l_rlbk_stmt_arr.COUNT+1) := cort_comp_pkg.get_sequence_sql(l_old_sequence_rec);

        l_frwd_stmt_arr(l_frwd_stmt_arr.COUNT+1) := in_job_rec.sql_text;
        l_rlbk_stmt_arr(l_rlbk_stmt_arr.COUNT+1) := get_drop_object_ddl(
                                                      in_object_type  => 'SEQUENCE',
                                                      in_object_owner => l_old_sequence_rec.owner,
                                                      in_object_name  => l_old_sequence_rec.sequence_name
                                                    );

        IF g_params.keep_objects.value_exists('PRIVILEGES') THEN
          cort_comp_pkg.get_privileges_stmt(
            in_privilege_arr         => l_old_sequence_rec.privilege_arr,
            io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
            io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
          );
        END IF;

        -- execute all changes in the queue
        BEGIN
          apply_changes(
            in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
            in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
            in_test                => g_params.test.get_bool_value
          );
        EXCEPTION
          WHEN OTHERS THEN
            cort_log_pkg.error('Error. Rolling back...');
            RAISE;
        END;

      END CASE;

      IF NOT g_params.test.get_bool_value THEN
        cort_aux_pkg.register_change(
          in_object_owner   => in_job_rec.object_owner,
          in_object_type    => in_job_rec.object_type,
          in_object_name    => in_job_rec.object_name,
          in_job_rec        => in_job_rec,
          in_sql            => in_job_rec.sql_text,
          in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                 in_object_owner   => in_job_rec.object_owner,
                                 in_object_name    => in_job_rec.object_name,
                                 in_object_type    => in_job_rec.object_type
                               ),
          in_change_type    => l_result,
          in_revert_name    => NULL,
          in_last_ddl_index => NULL,
          in_frwd_stmt_arr  => l_frwd_stmt_arr,
          in_rlbk_stmt_arr  => l_rlbk_stmt_arr
        );
      END IF;

    ELSE
      -- Sequence does not exist.
      -- Just execute SQL as is
      l_frwd_stmt_arr(1) := in_job_rec.sql_text;
      l_rlbk_stmt_arr(1) := get_drop_sequence_ddl(
                              in_sequence_name => in_job_rec.object_name,
                              in_owner         => in_job_rec.object_owner
                            );

      -- Sequence doesn't exist. Create it as is.
      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
        in_test                => g_params.test.get_bool_value,
        in_echo                => TRUE
      );

      IF NOT g_params.test.get_bool_value THEN
        -- delete any previous records for given object
        cleanup_history(
          in_object_owner  => in_job_rec.object_owner,
          in_object_type   => in_job_rec.object_type,
          in_object_name   => in_job_rec.object_name
        );
        -- register sequence
        cort_aux_pkg.register_change(
          in_object_owner   => in_job_rec.object_owner,
          in_object_type    => in_job_rec.object_type,
          in_object_name    => in_job_rec.object_name,
          in_job_rec        => in_job_rec,
          in_sql            => in_job_rec.sql_text,
          in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                 in_object_owner   => in_job_rec.object_owner,
                                 in_object_name    => in_job_rec.object_name,
                                 in_object_type    => in_job_rec.object_type
                               ),
          in_change_type    => cort_comp_pkg.gc_result_create,
          in_revert_name    => NULL,
          in_last_ddl_index => NULL,
          in_frwd_stmt_arr  => l_frwd_stmt_arr,
          in_rlbk_stmt_arr  => l_rlbk_stmt_arr
        );
      END IF;
    END IF;
    cort_session_pkg.enable;

  END create_or_replace_sequence;

  -- Create or replace type
  PROCEDURE create_or_replace_type(
    in_job_rec       IN cort_jobs%ROWTYPE,
    io_sql_rec       IN OUT NOCOPY cort_parse_pkg.gt_sql_rec
  )
  AS
    l_new_sql              CLOB;

    l_old_type_rec         gt_type_rec;
    l_new_type_rec         gt_type_rec;

    l_frwd_stmt_arr        arrays.gt_clob_arr;
    l_rlbk_stmt_arr        arrays.gt_clob_arr;

    l_result               PLS_INTEGER;
  BEGIN
    start_timer;

    -- disable CORT trigger for current session
    cort_session_pkg.disable;

    -- read sequence attributes
    read_type(
      in_type_name => in_job_rec.object_name,
      in_owner     => in_job_rec.object_owner,
      out_type_rec => l_old_type_rec
    );

    IF l_old_type_rec.type_name IS NOT NULL THEN

      read_privileges(
        in_owner         => l_old_type_rec.owner,
        in_table_name    => l_old_type_rec.type_name,
        io_privilege_arr => l_old_type_rec.privilege_arr
      );

      -- modify original SQL - replace original names with temp ones.
      cort_parse_pkg.replace_names(
        in_type_rec => l_old_type_rec,
        io_sql_rec  => io_sql_rec,
        out_sql     => l_new_sql
      );

      -- execute original sql with replaces names
      execute_immediate(l_new_sql, FALSE);


      IF g_params.change.get_value = 'ALWAYS' THEN
        l_result := cort_comp_pkg.gc_result_recreate;
      ELSE
        l_result := cort_comp_pkg.gc_result_nochange;
      END IF;

      BEGIN
        -- read new type attributes
        read_type(
          in_type_name => l_old_type_rec.rename_rec.temp_name,
          in_owner     => in_job_rec.object_owner,
          out_type_rec => l_new_type_rec
        );

        IF l_result = cort_comp_pkg.gc_result_nochange THEN
          -- compare types
          l_result := cort_comp_pkg.comp_types(
                        in_source_type_rec     => l_old_type_rec,
                        in_target_type_rec     => l_new_type_rec,
                        io_frwd_alter_stmt_arr => l_frwd_stmt_arr,
                        io_rlbk_alter_stmt_arr => l_rlbk_stmt_arr
                      );
        END IF;

      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.error('Error. Rolling back...');
          -- drop temp type
          exec_drop_object(
            in_object_type  => 'TYPE',
            in_object_name  => l_new_type_rec.rename_rec.current_name,
            in_object_owner => l_new_type_rec.owner,
            in_echo         => FALSE
          );
          RAISE;
      END;
      debug('Drop temp object ...');

      -- drop temp type
      exec_drop_object(
        in_object_type  => 'TYPE',
        in_object_name  => l_new_type_rec.rename_rec.current_name,
        in_object_owner => l_new_type_rec.owner,
        in_echo         => FALSE
      );

      debug('Change type as result of comparison = '||cort_comp_pkg.get_result_name(l_result));

      CASE g_params.change.get_value
        WHEN 'ALWAYS' THEN
          l_result := cort_comp_pkg.gc_result_recreate;
        WHEN 'RECREATE' THEN
          IF l_result > cort_comp_pkg.gc_result_nochange THEN
            l_result := cort_comp_pkg.gc_result_recreate;
          END IF;
        WHEN 'ALTER' THEN
          IF l_result > cort_comp_pkg.gc_result_alter_move THEN
            raise_error('Changes could not be applied without table recreation');
          END IF;
        ELSE
          NULL;
      END CASE;

      debug('Change type after applying change param = '||cort_comp_pkg.get_result_name(l_result));

      CASE
      WHEN l_result = cort_comp_pkg.gc_result_nochange THEN
        debug('Do nothing ...');
        l_frwd_stmt_arr.DELETE;
        l_rlbk_stmt_arr.DELETE;
      WHEN l_result = cort_comp_pkg.gc_result_alter THEN
        debug('Do alters ...');
      WHEN l_result = cort_comp_pkg.gc_result_recreate THEN
        debug('Do recreate ...');
        l_frwd_stmt_arr.DELETE;
        l_rlbk_stmt_arr.DELETE;

        FOR i IN 1..l_old_type_rec.dependency_arr.COUNT LOOP
          debug('dependent object '||l_old_type_rec.dependency_arr(i).type||'   "'||l_old_type_rec.dependency_arr(i).owner||'"."'||l_old_type_rec.dependency_arr(i).name||'"');
          IF l_old_type_rec.dependency_arr(i).type = 'TABLE' THEN
            -- check objects_id for NULL is workaround for absence of ALL_RECYCLEBIN view.
            IF l_old_type_rec.dependency_arr(i).object_id IS NOT NULL THEN
              IF g_params.data.get_value <> 'NONE' THEN
                --recreate table as heap organized;
                backup_table(
                  in_table_name    => l_old_type_rec.dependency_arr(i).name,
                  in_table_owner   => l_old_type_rec.dependency_arr(i).owner,
                  in_sql_rec       => io_sql_rec,
                  io_frwd_stmt_arr => l_frwd_stmt_arr,
                  io_rlbk_stmt_arr => l_rlbk_stmt_arr
                );
              ELSE
                cort_comp_pkg.add_stmt(
                  io_frwd_stmt_arr => l_frwd_stmt_arr,
                  io_rlbk_stmt_arr => l_rlbk_stmt_arr,
                  in_frwd_stmt     => get_drop_object_ddl(
                                        in_object_type  => l_old_type_rec.dependency_arr(i).type,
                                        in_object_name  => l_old_type_rec.dependency_arr(i).name,
                                        in_object_owner => l_old_type_rec.dependency_arr(i).owner,
                                        in_purge        => TRUE
                                      ),
                  in_rlbk_stmt     => NULL
                );
              END IF;
            ELSE
              -- table is in recycle bin. Purge it.
              cort_comp_pkg.add_stmt(
                io_frwd_stmt_arr => l_frwd_stmt_arr,
                io_rlbk_stmt_arr => l_rlbk_stmt_arr,
                in_frwd_stmt     => 'PURGE TABLE "'||l_old_type_rec.dependency_arr(i).owner||'"."'||l_old_type_rec.dependency_arr(i).name||'"',
                in_rlbk_stmt     => NULL
              );
            END IF;
          END IF;
        END LOOP;

       cort_comp_pkg.add_stmt(
          io_frwd_stmt_arr => l_frwd_stmt_arr,
          io_rlbk_stmt_arr => l_rlbk_stmt_arr,
          in_frwd_stmt     => get_drop_type_ddl(
                                in_type_name => l_old_type_rec.type_name,
                                in_owner     => l_old_type_rec.owner
                              ),
          in_rlbk_stmt     => NULL
        );

        cort_comp_pkg.add_stmt(
          io_frwd_stmt_arr => l_frwd_stmt_arr,
          io_rlbk_stmt_arr => l_rlbk_stmt_arr,
          in_frwd_stmt     => in_job_rec.sql_text,
          in_rlbk_stmt     => NULL
        );

        IF g_params.keep_objects.value_exists('PRIVILEGES') THEN
          cort_comp_pkg.get_privileges_stmt(
            in_privilege_arr         => l_old_type_rec.privilege_arr,
            io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
            io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
          );
        END IF;
      END CASE;

      IF l_result != cort_comp_pkg.gc_result_nochange THEN
        BEGIN
          -- execute all changes in the queue
          apply_changes(
            in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
            in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
            in_test                => g_params.test.get_bool_value,
            in_echo                => TRUE
          );
        EXCEPTION
          WHEN OTHERS THEN
            cort_log_pkg.error('Error. Rolling back...');
            RAISE;
        END;
      END IF;

      IF NOT g_params.test.get_bool_value THEN
        debug('Register ...');
        cort_aux_pkg.register_change(
          in_object_owner   => in_job_rec.object_owner,
          in_object_type    => in_job_rec.object_type,
          in_object_name    => in_job_rec.object_name,
          in_job_rec        => in_job_rec,
          in_sql            => in_job_rec.sql_text,
          in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                 in_object_owner   => in_job_rec.object_owner,
                                 in_object_name    => in_job_rec.object_name,
                                 in_object_type    => in_job_rec.object_type
                               ),
          in_change_type    => l_result,
          in_revert_name    => NULL,
          in_last_ddl_index => NULL,
          in_frwd_stmt_arr  => l_frwd_stmt_arr,
          in_rlbk_stmt_arr  => l_rlbk_stmt_arr
        );
      END IF;
    ELSE
      debug('Create ...');
      -- Type does not exist.
      -- Just execute SQL as is
      l_frwd_stmt_arr(1) := in_job_rec.sql_text;
      l_rlbk_stmt_arr(1) := get_drop_type_ddl(
                              in_type_name => in_job_rec.object_name,
                              in_owner     => in_job_rec.object_owner
                            );

      -- Sequence doesn't exist. Create it as is.
      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
        in_test                => g_params.test.get_bool_value,
        in_echo                => TRUE
      );

      IF NOT g_params.test.get_bool_value THEN
        debug('Register ...');
        -- delete any previous records for given object
        cleanup_history(
          in_object_owner   => in_job_rec.object_owner,
          in_object_type    => in_job_rec.object_type,
          in_object_name    => in_job_rec.object_name
        );
        -- register type
        cort_aux_pkg.register_change(
          in_object_owner   => in_job_rec.object_owner,
          in_object_type    => in_job_rec.object_type,
          in_object_name    => in_job_rec.object_name,
          in_job_rec        => in_job_rec,
          in_sql            => in_job_rec.sql_text,
          in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                 in_object_owner   => in_job_rec.object_owner,
                                 in_object_name    => in_job_rec.object_name,
                                 in_object_type    => in_job_rec.object_type
                               ),
          in_change_type    => cort_comp_pkg.gc_result_create,
          in_revert_name    => NULL,
          in_last_ddl_index => NULL,
          in_frwd_stmt_arr  => l_frwd_stmt_arr,
          in_rlbk_stmt_arr  => l_rlbk_stmt_arr
        );
      END IF;
    END IF;
    cort_session_pkg.enable;

    stop_timer;
  END create_or_replace_type;

  FUNCTION get_job_rec
  RETURN cort_jobs%ROWTYPE
  AS
  BEGIN
    RETURN g_job_rec;
  END get_job_rec;

  PROCEDURE init(
    in_job_rec IN cort_jobs%ROWTYPE
  )
  AS
  BEGIN
    cort_params_pkg.read_from_xml(XMLType(in_job_rec.session_params), g_params);
    cort_log_pkg.init_log(in_job_rec);
    g_job_rec := in_job_rec;
    cort_parse_pkg.cleanup;
  END init;

  -- Public: create or replace object
  PROCEDURE create_or_replace(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
    l_sql_rec cort_parse_pkg.gt_sql_rec;
  BEGIN
    -- parse original sql and cort-hints and obtain object params
    start_timer;

    l_sql_rec := cort_parse_pkg.parse_sql(in_job_rec.sql_text);

    cort_parse_pkg.parse_create_sql(
      io_sql_rec       => l_sql_rec,
      in_object_type   => in_job_rec.object_type,
      in_object_name   => in_job_rec.object_name,
      in_object_owner  => in_job_rec.object_owner
    );

    -- read cort hints
    cort_parse_pkg.parse_params(
      io_sql_rec      => l_sql_rec,
      io_params_rec   => g_params
    );

    cort_job_pkg.update_object_params(
      in_rec           => in_job_rec,
      in_object_params => cort_params_pkg.rec_to_xml(g_params)
    );

    IF g_params.test.get_bool_value THEN
      cort_log_pkg.echo('== TEST OUTPUT ==');
    END IF;

    IF g_params.parallel.get_num_value > 1 THEN
      execute_immediate('ALTER SESSION ENABLE PARALLEL DML');
    END IF;

    IF in_job_rec.current_schema <> in_job_rec.job_owner THEN
      execute_immediate('ALTER SESSION SET CURRENT_SCHEMA = "'||in_job_rec.current_schema||'"', TRUE);
    END IF;

    CASE in_job_rec.object_type
      WHEN 'TABLE' THEN
        create_or_replace_table(
          in_job_rec => in_job_rec,
          io_sql_rec => l_sql_rec
        );
      WHEN 'SEQUENCE' THEN
        create_or_replace_sequence(
          in_job_rec => in_job_rec,
          io_sql_rec => l_sql_rec
        );
      WHEN 'TYPE' THEN
        create_or_replace_type(
          in_job_rec => in_job_rec,
          io_sql_rec => l_sql_rec
        );
      ELSE
        raise_error('Unsupported object type');
    END CASE;

    IF g_params.test.get_bool_value THEN
      cort_log_pkg.echo('== END OF TEST OUTPUT ==');
    END IF;
    stop_timer;
  END create_or_replace;

  -- Add metadata of creating recreatable object
  PROCEDURE before_create_or_replace(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
    l_sql              CLOB;
    l_object_type      arrays.gt_name;
    l_meta_object_type arrays.gt_name;
    l_frwd_stmt_arr    arrays.gt_clob_arr;
    l_rlbk_stmt_arr    arrays.gt_clob_arr;
    l_exists           BOOLEAN;
    l_change_type      PLS_INTEGER;
  BEGIN
    CASE
    WHEN in_job_rec.object_type IN ('TABLE','SEQUENCE') THEN
      l_sql := 'DROP '||in_job_rec.object_type||' "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'"';
      l_exists := FALSE;
    WHEN in_job_rec.object_type IN ('VIEW','PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','LIBRARY','SYNONYM','JAVA','CONTEXT') THEN
      IF in_job_rec.object_type = 'JAVA' THEN
        l_object_type := 'JAVA SOURCE';
      END IF;

      CASE
      WHEN l_object_type = 'PACKAGE' THEN
        l_meta_object_type := 'PACKAGE_SPEC';
      WHEN l_object_type = 'TYPE' THEN
        l_meta_object_type := 'TYPE_SPEC';
      ELSE
        l_meta_object_type := REPLACE(l_object_type, ' ', '_');
      END CASE;

      l_exists := object_exists(
                    in_object_type  => l_object_type,
                    in_object_name  => in_job_rec.object_name,
                    in_object_owner => NVL(in_job_rec.object_owner,'SYS')
                  );
      IF l_exists THEN
        BEGIN
          l_sql := dbms_metadata.get_ddl(l_meta_object_type, in_job_rec.object_name, in_job_rec.object_owner);
        EXCEPTION
          WHEN OTHERS THEN
            cort_log_pkg.error('Error in dbms_metadata.get_ddl');
            RAISE;
        END;
      ELSE
        IF l_object_type = 'SYNONYM' THEN
          IF in_job_rec.object_owner = 'PUBLIC' THEN
            l_sql := 'DROP PUBLIC SYNONYM "'||in_job_rec.object_name||'"';
          ELSE
            l_sql := 'DROP SYNONYM "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'"';
          END IF;
        ELSE
          IF in_job_rec.object_owner IS NOT NULL THEN
            l_sql := 'DROP '||l_object_type||' "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'"';
          ELSE
            l_sql := 'DROP '||l_object_type||' "'||in_job_rec.object_name||'"';
          END IF;
        END IF;
        l_exists := FALSE;
      END IF;
    ELSE
      NULL;
    END CASE;
    l_frwd_stmt_arr(1) := in_job_rec.sql_text;
    l_rlbk_stmt_arr(1) := l_sql;

    IF NOT l_exists THEN
      -- delete all previous data for given object (if any)
      cort_aux_pkg.cleanup_history(
        in_object_owner  => in_job_rec.object_owner,
        in_object_type   => in_job_rec.object_type,
        in_object_name   => in_job_rec.object_name
      );
      l_change_type := cort_comp_pkg.gc_result_create;
    ELSE
      l_change_type := cort_comp_pkg.gc_result_replace;
    END IF;

    cort_aux_pkg.register_change(
      in_object_owner   => in_job_rec.object_owner,
      in_object_name    => in_job_rec.object_name,
      in_object_type    => in_job_rec.object_type,
      in_job_rec        => in_job_rec,
      in_sql            => in_job_rec.sql_text,
      in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                             in_object_owner   => in_job_rec.object_owner,
                             in_object_name    => in_job_rec.object_name,
                             in_object_type    => in_job_rec.object_type
                           ),
      in_change_type    => l_change_type,
      in_revert_name    => NULL,
      in_last_ddl_index => NULL,
      in_frwd_stmt_arr  => l_frwd_stmt_arr,
      in_rlbk_stmt_arr  => l_rlbk_stmt_arr
    );
  END before_create_or_replace;


  -- Rename table and it's constraints and indexes
  PROCEDURE rename_table(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
    l_frwd_stmt_arr    arrays.gt_clob_arr;
    l_rlbk_stmt_arr    arrays.gt_clob_arr;
    l_table_rec        gt_table_rec;
  BEGIN
    IF NOT cort_aux_pkg.is_object_renamed(
             in_object_type   => in_job_rec.object_type,
             in_object_name   => in_job_rec.object_name,
             in_object_owner  => in_job_rec.object_owner,
             in_rename_name   => in_job_rec.new_name
           )
    THEN
      -- first time rename in release
      read_table_cascade(
        in_table_name      => in_job_rec.object_name,
        in_owner           => in_job_rec.object_owner,
        in_read_data       => FALSE,
        out_table_rec      => l_table_rec
      );

      -- register renamed table
      l_table_rec.rename_rec.rename_name := in_job_rec.new_name;

      rename_table_cascade(
        io_table_rec       => l_table_rec,
        in_rename_mode     => 'TO_RENAME',
        io_frwd_stmt_arr   => l_frwd_stmt_arr,
        io_rlbk_stmt_arr   => l_rlbk_stmt_arr
      );

      BEGIN
        apply_changes(
          in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
          in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
          in_test                => g_params.test.get_bool_value
        );
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.error('Error. Rolling back...');
          RAISE;
      END;

      IF NOT g_params.test.get_bool_value THEN
        cort_aux_pkg.register_change(
          in_object_owner   => in_job_rec.object_owner,
          in_object_name    => in_job_rec.object_name,
          in_object_type    => in_job_rec.object_type,
          in_job_rec        => in_job_rec,
          in_sql            => in_job_rec.sql_text,
          in_last_ddl_time  => cort_event_exec_pkg.get_object_last_ddl_time(
                                 in_object_owner   => in_job_rec.object_owner,
                                 in_object_name    => in_job_rec.object_name,
                                 in_object_type    => in_job_rec.object_type
                               ),
          in_change_type    => cort_comp_pkg.gc_result_rename,
          in_revert_name    => NULL,
          in_last_ddl_index => NULL,
          in_rename_name    => in_job_rec.new_name,
          in_frwd_stmt_arr  => l_frwd_stmt_arr,
          in_rlbk_stmt_arr  => l_rlbk_stmt_arr
        );
      END IF;
    END IF;

  END rename_table;

  -- Generic alter
  PROCEDURE alter_object(
     in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
  BEGIN
    execute_immediate(
      in_sql   => in_job_rec.sql_text,
      in_echo  => TRUE,
      in_test  => cort_exec_pkg.g_params.test.get_bool_value
    );

  END alter_object;

  PROCEDURE drop_object(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
  BEGIN
    exec_drop_object(
      in_object_type  => in_job_rec.object_type,
      in_object_name  => in_job_rec.object_name,
      in_object_owner => in_job_rec.object_owner,
      in_test         => cort_exec_pkg.g_params.test.get_bool_value,
      in_purge        => FALSE
    );
  END drop_object;

  -- Rollback the latest change for given object
  PROCEDURE revert_object(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
    l_metadata_rec      cort_objects%ROWTYPE;
    l_prev_metadata_rec cort_objects%ROWTYPE;
    l_rlbk_stmt_arr     arrays.gt_clob_arr;
  BEGIN
    l_metadata_rec := cort_aux_pkg.get_last_change(
                        in_object_type  => in_job_rec.object_type,
                        in_object_name  => in_job_rec.object_name,
                        in_object_owner => in_job_rec.object_owner
                      );

    IF l_metadata_rec.object_name IS NOT NULL THEN

      IF g_params.test.get_bool_value THEN
        cort_log_pkg.echo('reverting of '||l_metadata_rec.object_type||' "'||l_metadata_rec.object_owner||'"."'||l_metadata_rec.object_name||'"');
        cort_log_pkg.echo('== TEST OUTPUT ==');
      END IF;

      IF in_job_rec.current_schema <> in_job_rec.job_owner THEN
        execute_immediate('ALTER SESSION SET CURRENT_SCHEMA = "'||in_job_rec.current_schema||'"', TRUE);
      END IF;

      IF l_metadata_rec.revert_ddl IS NOT NULL THEN
        cort_xml_pkg.read_from_xml(
          in_value => XMLType(l_metadata_rec.revert_ddl),
          out_arr  => l_rlbk_stmt_arr
        );
      END IF;

      IF l_rlbk_stmt_arr.COUNT > 0 THEN

        IF l_metadata_rec.revert_name IS NOT NULL AND
           NOT object_exists(
                 in_object_type  => l_metadata_rec.object_type,
                 in_object_owner => l_metadata_rec.object_owner,
                 in_object_name  => l_metadata_rec.revert_name
               )
        THEN
          raise_error('Unable to revert of the last change for object "'||l_metadata_rec.object_owner||'"."'||l_metadata_rec.object_name||'" because backup object is not available');
        END IF;

        IF l_metadata_rec.last_ddl_index IS NOT NULL THEN
          l_rlbk_stmt_arr.DELETE(l_metadata_rec.last_ddl_index+1, l_rlbk_stmt_arr.LAST);
        END IF;

        --reverse array to execute in reverse order
        cort_aux_pkg.reverse_array(l_rlbk_stmt_arr);

        apply_changes_ignore_errors(
          io_stmt_arr => l_rlbk_stmt_arr,
          in_test     => g_params.test.get_bool_value
        );

      ELSE
        cort_log_pkg.echo('There is no revert information');
      END IF;

      IF NOT g_params.test.get_bool_value THEN
        cort_aux_pkg.unregister_change(
          in_object_owner => l_metadata_rec.object_owner,
          in_object_type  => l_metadata_rec.object_type,
          in_object_name  => l_metadata_rec.object_name,
          in_exec_time    => l_metadata_rec.exec_time
        );

        IF l_metadata_rec.object_type = 'TABLE' THEN
          l_prev_metadata_rec := cort_aux_pkg.get_last_change(
                                   in_object_type  => l_metadata_rec.object_type,
                                   in_object_name  => l_metadata_rec.object_name,
                                   in_object_owner => l_metadata_rec.object_owner
                                 );
          -- if reverted first change recreate in the release
          IF l_prev_metadata_rec.release <> l_metadata_rec.release THEN
            -- then recreate prev synonym
            create_prev_synonym(
              in_synonym_name  => l_prev_metadata_rec.object_name,
              in_table_name    => l_prev_metadata_rec.object_name,
              in_table_owner   => l_prev_metadata_rec.object_owner
            );
          END IF;
        END IF;
      END IF;

      IF g_params.test.get_bool_value THEN
        cort_log_pkg.echo('== END OF TEST OUTPUT ==');
      END IF;

    ELSE
      raise_error('Object '||in_job_rec.object_type||' "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'" is not found');
    END IF;
  END revert_object;


  -- Resume pending changes for given object
  PROCEDURE resume_change(
    in_job_rec  in cort_jobs%ROWTYPE
  )
  AS
    l_frwd_stmt_arr      arrays.gt_clob_arr;
    l_rlbk_stmt_arr      arrays.gt_clob_arr;
    l_metadata_rec       cort_objects%ROWTYPE;
    l_last_ddl_index     PLS_INTEGER;
  BEGIN

    BEGIN
      l_metadata_rec := cort_aux_pkg.get_last_change(
                          in_object_type  => in_job_rec.object_type,
                          in_object_name  => in_job_rec.object_name,
                          in_object_owner => in_job_rec.object_owner
                        );

      IF l_metadata_rec.object_name IS NOT NULL THEN

        IF g_params.test.get_bool_value THEN
          cort_log_pkg.echo('resuming of '||l_metadata_rec.object_type||' "'||l_metadata_rec.object_owner||'"."'||l_metadata_rec.object_name||'"');
          cort_log_pkg.echo('== TEST OUTPUT ==');
        END IF;

        cort_xml_pkg.read_from_xml(
          in_value => XMLType(l_metadata_rec.forward_ddl),
          out_arr  => l_frwd_stmt_arr
        );
        cort_xml_pkg.read_from_xml(
          in_value => XMLType(l_metadata_rec.revert_ddl),
          out_arr  => l_rlbk_stmt_arr
        );

        l_last_ddl_index := l_metadata_rec.last_ddl_index;
        BEGIN
          cort_log_pkg.echo('initial l_metadata_rec.last_ddl_index = '||l_metadata_rec.last_ddl_index);
          apply_changes(
            in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
            in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
            in_revert              => FALSE,
            io_last_ddl_index      => l_last_ddl_index,
            in_test                => g_params.test.get_bool_value
          );
        EXCEPTION
          WHEN OTHERS THEN
            l_metadata_rec.last_ddl_index := l_last_ddl_index;
            cort_log_pkg.echo('l_metadata_rec.last_ddl_index = '||l_metadata_rec.last_ddl_index);
            cort_aux_pkg.update_change(
              in_rec => l_metadata_rec
            );
            cort_log_pkg.error('Error. Rolling back...');
            RAISE;
        END;

        l_metadata_rec.last_ddl_index := NULL;

        cort_aux_pkg.update_change(
          in_rec => l_metadata_rec
        );
        cort_job_pkg.success_job(in_rec => in_job_rec);

        IF g_params.test.get_bool_value THEN
          cort_log_pkg.echo('== END OF TEST OUTPUT ==');
        END IF;

      ELSE
        raise_error('Object '||in_job_rec.object_type||' "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'" is not found');
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.error('Failed resumed job...');
        cort_job_pkg.fail_job(
          in_rec             => in_job_rec,
          in_error_message   => sqlerrm
        );
        dbms_application_info.set_client_info(NULL);
        RAISE;
    END;
  END resume_change;

  -- Drop revert object when new release is created. Repoint prev synonym to actual table
  PROCEDURE reset_object(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
    l_metadata_rec      cort_objects%ROWTYPE;
  BEGIN
    l_metadata_rec := cort_aux_pkg.get_last_change(
                        in_object_type  => in_job_rec.object_type,
                        in_object_name  => in_job_rec.object_name,
                        in_object_owner => in_job_rec.object_owner
                      );

    IF l_metadata_rec.object_name IS NOT NULL THEN

      IF l_metadata_rec.revert_name IS NOT NULL THEN
        exec_drop_object(
          in_object_type  => 'TABLE',
          in_object_name  => l_metadata_rec.revert_name,
          in_object_owner => l_metadata_rec.object_owner
        );
        create_prev_synonym(
          in_synonym_name => l_metadata_rec.object_name,
          in_table_name   => l_metadata_rec.object_name,
          in_table_owner  => l_metadata_rec.object_owner
        );
      END IF;
    ELSE
      raise_error('Object '||in_job_rec.object_type||' "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'" is not found');
    END IF;
  END reset_object;

END cort_exec_pkg;
/