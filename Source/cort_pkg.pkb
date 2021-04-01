CREATE OR REPLACE PACKAGE BODY cort_pkg
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
  Description: API for end-user - wrappers around main procedures/functions.
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added build API
  15.00   | Rustam Kafarov    | Added API for manual execution
  17.00   | Rustam Kafarov    | Use cort_jobs for rename/drop API
  19.00   | Rustam Kafarov    | Revised parameters
  20.00   | Rustam Kafarov    | Added support of long names introduced in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------
*/

  g_current_release         varchar2(20);
  g_current_application     varchar2(20);

  /* Private */
  PROCEDURE check_param_is_not_null(
    in_param IN VARCHAR2,
    in_value IN VARCHAR2
  )
  AS
  BEGIN
    IF in_value IS NULL THEN
      cort_exec_pkg.raise_error( 'Parameter '||in_param||' must have a value');
    END IF;
  END check_param_is_not_null;

  PROCEDURE check_param_max_length(
    in_param   IN VARCHAR2,
    in_value   IN VARCHAR2,
    in_max_len IN NUMBER
  )
  AS
  BEGIN
    IF LENGTH(in_value) > in_max_len THEN
      cort_exec_pkg.raise_error( 'Parameter '||in_param||' is too long (longer '||in_max_len||' symbols)');
    END IF;
  END check_param_max_length;

  PROCEDURE check_param_min_length(
    in_param   IN VARCHAR2,
    in_value   IN VARCHAR2,
    in_min_len IN NUMBER
  )
  AS
  BEGIN
    IF LENGTH(in_value) < in_min_len THEN
      cort_exec_pkg.raise_error( 'Parameter '||in_param||' must be longer '||in_min_len||' symbols');
    END IF;
  END check_param_min_length;

  /* Public */

  -- Generic execution procedure which is called from job
  PROCEDURE execute_action(
    in_sid IN VARCHAR2
  )
  AS
    l_rec        cort_jobs%ROWTYPE;
    l_sql        CLOB;
  BEGIN
    -- assign job to current session
    l_rec := cort_job_pkg.assign_job(
               in_sid => in_sid
             );
    dbms_application_info.set_client_info('CORT_BUILD='||l_rec.build);

    BEGIN
      cort_aux_pkg.set_context(l_rec.action||'_'||l_rec.object_type,l_rec.object_name);
      cort_session_pkg.disable;
      cort_exec_pkg.init(
        in_job_rec => l_rec
      );

      IF l_rec.sql_text IS NOT NULL THEN
        dbms_lob.createtemporary(l_sql, true, dbms_lob.call);
        dbms_lob.copy(l_sql, l_rec.sql_text, dbms_lob.getlength(l_rec.sql_text));
        l_rec.sql_text := l_sql;
        dbms_lob.freetemporary(l_sql);
      END IF;

      CASE l_rec.action
      WHEN 'CREATE_OR_REPLACE' THEN
        cort_exec_pkg.create_or_replace(
          in_job_rec    => l_rec
        );
      WHEN 'EXPLAIN_PLAN_FOR' THEN
        cort_exec_pkg.create_or_replace(
          in_job_rec    => l_rec
        );
      WHEN 'DATA_COPY' THEN
        cort_exec_pkg.resume_change(
          in_job_rec    => l_rec
        );
      WHEN 'REGISTER' THEN
        cort_exec_pkg.before_create_or_replace(
          in_job_rec    => l_rec
        );
      WHEN 'RENAME' THEN
        cort_exec_pkg.rename_table(
          in_job_rec    => l_rec
        );
      WHEN 'ALTER' THEN
        cort_exec_pkg.alter_object(
          in_job_rec    => l_rec
        );
      WHEN 'DROP' THEN
        cort_exec_pkg.drop_object(
          in_job_rec    => l_rec
        );
      WHEN 'REVERT' THEN
        cort_exec_pkg.revert_object(
          in_job_rec    => l_rec
        );
      WHEN 'RESET' THEN
        cort_exec_pkg.reset_object(
          in_job_rec    => l_rec
        );
      $IF cort_options_pkg.gc_threading $THEN
      WHEN 'THREADING' THEN
        cort_thread_exec_pkg.process_thread(
          in_job_rec    => l_rec
        );
      $END
      END CASE;

      cort_aux_pkg.set_context(l_rec.action||'_'||l_rec.object_type,NULL);
      cort_session_pkg.enable;
      l_rec := cort_job_pkg.get_job_rec(
                 in_sid          => l_rec.sid,
                 in_object_name  => l_rec.object_name,
                 in_object_owner => l_rec.object_owner
               );

      IF l_rec.status = 'RUNNING' THEN
        cort_job_pkg.success_job(in_rec => l_rec);
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.error('Failed cort_pkg.execute_action');
        IF sqlcode = -20900 THEN
          cort_job_pkg.cancel_job(
            in_rec             => l_rec,
            in_error_message   => sqlerrm
          );
        ELSE
          cort_job_pkg.fail_job(
            in_rec             => l_rec,
            in_error_message   => sqlerrm
          );
        END IF;
        dbms_application_info.set_client_info(NULL);
        cort_aux_pkg.set_context(l_rec.action||'_'||l_rec.object_type,NULL);
        cort_session_pkg.enable;
        RAISE;
    END;
  END execute_action;

  -- permanently enable CORT
  PROCEDURE enable
  AS
  BEGIN
    execute immediate 'ALTER TRIGGER CORT_CREATE_TRG ENABLE';
  END enable;

  -- permanently disable CORT
  PROCEDURE disable
  AS
  BEGIN
    execute immediate 'ALTER TRIGGER CORT_CREATE_TRG DISABLE';
  END disable;

  -- get CORT status (ENABLED/DISABLED/DISABLED FOR SESSION)
  FUNCTION get_status
  RETURN VARCHAR2
  AS
    l_status VARCHAR2(20);
  BEGIN
    BEGIN
      SELECT status
        INTO l_status
        FROM user_triggers
       WHERE trigger_name = 'CORT_CREATE_TRG';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_status := NULL;
    END;
    RETURN l_status;
  END get_status;

  -- Rollback the latest change for given object (Private declaration)
  PROCEDURE int_revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 ,
    in_params_rec   IN cort_params_pkg.gt_params_rec
  )
  AS
    l_sql          CLOB;
    l_rec          cort_jobs%ROWTYPE;
  BEGIN
    l_rec := cort_job_pkg.find_pending_job(
               in_object_name  => in_object_name,
               in_object_owner => in_object_owner
             );

    IF l_rec.action = 'CREATE_OR_REPLACE' THEN
      cort_job_pkg.cancel_job(in_rec => l_rec);
    END IF;


    l_sql :=
'BEGIN
  cort_pkg.revert_object(
    in_object_type   => '''||in_object_type||''',
    in_object_name   => '''||in_object_name||''',
    in_object_owner  => '''||in_object_owner||'''
  );
END;';

    cort_event_exec_pkg.process_event(
      in_async        => FALSE,
      in_action       => 'REVERT',
      in_object_type  => in_object_type,
      in_object_name  => in_object_name,
      in_object_owner => in_object_owner,
      in_sql          => l_sql,
      in_params_rec   => in_params_rec
    );

  END int_revert_object;

  -- Revert the latest change for given object (overloaded - Public declaration)
  PROCEDURE revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  )
  AS
    l_params_rec cort_params_pkg.gt_params_rec := cort_session_pkg.get_params;
  BEGIN
    if in_test is not null then
      l_params_rec.test.set_value(in_test);
    end if;

    int_revert_object(
      in_object_type  => in_object_type,
      in_object_name  => in_object_name,
      in_object_owner => in_object_owner,
      in_params_rec   => l_params_rec
    );
  END revert_object;

  -- Wrapper for tables
  PROCEDURE revert_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  )
  AS
  BEGIN
    revert_object(
      in_object_type  => 'TABLE',
      in_object_name  => in_table_name,
      in_object_owner => in_table_owner,
      in_test         => in_test
    );
  END revert_table;


  -- revert last change in current session
  PROCEDURE revert(
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  )
  AS
    l_rec   cort_objects%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM (SELECT *
                FROM cort_objects
               WHERE sid = dbms_session.unique_session_id
               ORDER BY exec_time DESC)
       WHERE ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        cort_exec_pkg.raise_error('No CORT changes in current session');
    END;
    IF l_rec.revert_ddl IS NOT NULL THEN
      revert_object(
        in_object_type  => l_rec.object_type,
        in_object_name  => l_rec.object_name,
        in_object_owner => l_rec.object_owner,
        in_test         => in_test
      );
    ELSE
      cort_exec_pkg.raise_error('Last CORT change is not revertable');
    END IF;
  END revert;


  PROCEDURE resume(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_rec          cort_jobs%ROWTYPE;
  BEGIN
    l_rec := cort_job_pkg.find_pending_job(
               in_object_name  => in_object_name,
               in_object_owner => in_object_owner
             );

    IF l_rec.action = 'CREATE_OR_REPLACE' THEN

      l_rec.action := 'DATA_COPY';
      l_rec.resume_action := 'RESUME_CHANGE';
      l_rec.sql_text :=
'BEGIN
  cort_pkg.resume(
    in_object_name   => '''||in_object_name||''',
    in_object_owner  => '''||in_object_owner||'''
  );
END;';

      cort_event_exec_pkg.resume_process(
        in_rec => l_rec
      );

    ELSE
      cort_exec_pkg.raise_error('No pending job found for object "'||in_object_owner||'"."'||in_object_name||'"');
    END IF;
  END resume;

  -- drop object if it exists using current session params (in test mode just log and spool)
  PROCEDURE drop_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_sql          CLOB;
  BEGIN
    l_sql :=
'BEGIN
  cort_pkg.drop_object(
    in_object_type   => '''||in_object_type||''',
    in_object_name   => '''||in_object_name||''',
    in_object_owner  => '''||in_object_owner||'''
  );
END;';

    cort_event_exec_pkg.process_event(
      in_async        => FALSE,
      in_action       => 'DROP',
      in_object_type  => in_object_type,
      in_object_name  => in_object_name,
      in_object_owner => in_object_owner,
      in_sql          => l_sql
    );
  END drop_object;

  -- Wrapper for tables
  PROCEDURE drop_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
  BEGIN
    drop_object(
      in_object_type  => 'TABLE',
      in_object_name  => in_table_name,
      in_object_owner => in_table_owner
    );
  END drop_table;

  -- drop revert table if it exists
  PROCEDURE drop_revert_table(
    in_table_name  IN VARCHAR2,
    in_table_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_revert_name arrays.gt_name;
  BEGIN
    l_revert_name := cort_aux_pkg.get_last_revert_name(
                       in_object_type  => 'TABLE',
                       in_object_name  => in_table_name,
                       in_object_owner => in_table_owner
                     );
    cort_exec_pkg.debug('revert table for "'||in_table_owner||'"."'||in_table_name||'" is "'||l_revert_name||'"');
    IF l_revert_name IS NOT NULL THEN
      drop_table(
        in_table_name   => l_revert_name,
        in_table_owner  => in_table_owner
      );
    END IF;
  END drop_revert_table;

  -- disable all foreign keys on given table
  PROCEDURE disable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_owner_arr            arrays.gt_name_arr;
    l_table_name_arr       arrays.gt_name_arr;
    l_constraint_name_arr  arrays.gt_name_arr;
  BEGIN
    SELECT fk.owner, fk.table_name, fk.constraint_name
      BULK COLLECT
      INTO l_owner_arr, l_table_name_arr, l_constraint_name_arr
      FROM all_constraints pk
     INNER JOIN all_constraints fk
        ON fk.r_constraint_name = pk.constraint_name
       AND fk.r_owner = pk.owner
       AND fk.status = 'ENABLED'
     WHERE pk.table_name = in_table_name
       AND pk.owner = in_owner
       AND pk.constraint_type in ('P','U');

    FOR i IN 1..l_constraint_name_arr.COUNT LOOP
      cort_event_exec_pkg.process_event(
        in_async        => FALSE,
        in_action       => 'ALTER',
        in_object_type  => 'TABLE',
        in_object_owner => in_owner,
        in_object_name  => in_table_name,
        in_sql          => 'ALTER TABLE "'||l_owner_arr(i)||'"."'||l_table_name_arr(i)||'" DISABLE CONSTRAINT "'||l_constraint_name_arr(i)||'"'
      );
    END LOOP;
  END disable_all_references;

  -- enable all foreign keys on given table
  PROCEDURE enable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_validate   IN BOOLEAN DEFAULT TRUE
  )
  AS
    l_owner_arr            arrays.gt_name_arr;
    l_table_name_arr       arrays.gt_name_arr;
    l_constraint_name_arr  arrays.gt_name_arr;
    l_sql                  VARCHAR2(32767);
    l_validate             VARCHAR2(50);
  BEGIN
    SELECT fk.owner, fk.table_name, fk.constraint_name
      BULK COLLECT
      INTO l_owner_arr, l_table_name_arr, l_constraint_name_arr
      FROM all_constraints pk
     INNER JOIN all_constraints fk
        ON fk.r_constraint_name = pk.constraint_name
       AND fk.r_owner = pk.owner
       AND fk.status = 'DISABLED'
     WHERE pk.table_name = in_table_name
       AND pk.owner = in_owner
       AND pk.constraint_type in ('P','U');

    FOR i IN 1..l_constraint_name_arr.COUNT LOOP
      IF in_validate THEN
        l_validate := 'VALIDATE';
      ELSE
        l_validate := 'NOVALIDATE';
      END IF;
      l_sql := 'ALTER TABLE "'||l_owner_arr(i)||'"."'||l_table_name_arr(i)||'" ENABLE '||l_validate||' CONSTRAINT "'||l_constraint_name_arr(i)||'"';
      cort_event_exec_pkg.process_event(
        in_async        => FALSE,
        in_action       => 'ALTER',
        in_object_type  => 'TABLE',
        in_object_owner => in_owner,
        in_object_name  => in_table_name,
        in_sql          => l_sql
      );
    END LOOP;
  END enable_all_references;

  -- Rename table and it's constraints and indexes
  PROCEDURE rename_table(
    in_table_name IN VARCHAR2,
    in_new_name   IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_sql             VARCHAR(1000);
  BEGIN
    l_sql :=
'BEGIN
  cort_pkg.rename_table(
    in_table_name => '''||in_table_name||''',
    in_new_name   => '''||in_new_name||''',
    in_owner      => '''||in_owner||'''
  );
END;';

    cort_event_exec_pkg.process_event(
      in_async        => FALSE,
      in_action       => 'RENAME',
      in_object_type  => 'TABLE',
      in_object_owner => in_owner,
      in_object_name  => in_table_name,
      in_sql          => l_sql,
      in_new_name     => in_new_name
    );
  END rename_table;

  -- start new release
  PROCEDURE start_release(
    in_release     IN VARCHAR2
  )
  AS
    l_rec             cort_releases%ROWTYPE;
    l_object_metadata cort_aux_pkg.gt_object_metadata;
    l_application     cort_applications.application%TYPE;
  BEGIN
    check_param_is_not_null('in_release', in_release);
    check_param_max_length('in_release', in_release, 20);

    l_application := cort_session_pkg.get_param_value('APPLICATION');

    cort_aux_pkg.check_application(l_application);

    IF l_application IS NOT NULL THEN
      l_rec := cort_aux_pkg.get_curr_release_rec(l_application);

    -- Check if it is first call
    IF (l_rec.release IS NULL) OR (in_release <> l_rec.release) THEN

      -- drop all revert tables and rename synonyms
      l_object_metadata := cort_aux_pkg.get_object_metadata(
                             in_object_type   => 'TABLE',
                             in_object_name   => NULL, -- all objects
                             in_object_owner  => NULL, -- all schemas
                             in_application   => l_rec.application,
                             in_release       => l_rec.release
                           );
      -- drop temp objects from prev release
      FOR i IN 1..l_object_metadata.COUNT LOOP
        cort_event_exec_pkg.process_event(
          in_async        => FALSE,
          in_action       => 'RESET',
          in_object_type  => 'TABLE',
          in_object_owner => l_object_metadata(i).object_owner,
          in_object_name  => l_object_metadata(i).object_name,
          in_sql          => 'begin cort_pkg.start_release(in_release => '''||in_release||'''); end;'
        );
      END LOOP;

      -- staring new release
      cort_aux_pkg.register_new_release(
          in_application => l_application,
        in_release     => in_release
      );
      END IF;
    END IF;
  END start_release;

  -- return current release
  FUNCTION get_current_release
  RETURN VARCHAR2
  AS
    l_rec cort_releases%ROWTYPE;
    l_application     cort_applications.application%TYPE;
  BEGIN
    l_application := cort_session_pkg.get_param_value('APPLICATION');
    IF l_application IS NULL THEN
      RETURN NULL;
    END IF;
    l_rec := cort_aux_pkg.get_curr_release_rec(l_application);
    RETURN l_rec.release;
  END get_current_release;


  PROCEDURE start_build
  AS
    l_rec             cort_builds%ROWTYPE;
    l_application     cort_applications.application%TYPE;
  BEGIN
    l_application := cort_session_pkg.get_param_value('APPLICATION');

    IF l_application IS NOT NULL THEN
      l_rec := cort_aux_pkg.find_build(l_application);
      IF l_rec.build IS NULL THEN
        l_rec := cort_aux_pkg.create_build(l_application);
      END IF;
      dbms_application_info.set_client_info('CORT_BUILD='||l_rec.build);
    END IF;
  END start_build;

  PROCEDURE end_build
  AS
    l_rec             cort_builds%ROWTYPE;
    l_application     cort_applications.application%TYPE;
  BEGIN
    l_application := cort_session_pkg.get_param_value('APPLICATION');
    IF l_application IS NOT NULL THEN
      l_rec := cort_aux_pkg.find_build(l_application);

      IF l_rec.build IS NOT NULL THEN
        l_rec.status := 'COMPLETED';
        l_rec.end_time := SYSDATE;
        cort_aux_pkg.update_build(l_rec);
        dbms_application_info.set_client_info(null);
      END IF;

    END IF;
  END end_build;

  PROCEDURE fail_build
  AS
    l_rec             cort_builds%ROWTYPE;
    l_application     cort_applications.application%TYPE;
  BEGIN
    l_application := cort_session_pkg.get_param_value('APPLICATION');
    IF l_application IS NOT NULL THEN
      l_rec := cort_aux_pkg.find_build(l_application);

      IF l_rec.build IS NOT NULL THEN
        l_rec.status := 'FAILED';
        l_rec.end_time := SYSDATE;
        cort_aux_pkg.update_build(l_rec);
        dbms_application_info.set_client_info(null);
      END IF;
    END IF;
  END fail_build;

  PROCEDURE revert_build(in_build IN VARCHAR2 DEFAULT NULL)
  AS
    l_rec             cort_builds%ROWTYPE;
    l_application     cort_applications.application%TYPE;
    l_params          cort_params_pkg.gt_params_rec;
    l_object_metadata cort_aux_pkg.gt_object_metadata;
  BEGIN
    l_params := cort_session_pkg.get_params;
    l_application := l_params.application.get_value;
    IF l_application IS NOT NULL THEN
      IF in_build IS NULL THEN
        l_rec := cort_aux_pkg.find_build(l_application);
      ELSE
        l_rec := cort_aux_pkg.get_build_rec(in_build);
      END IF;

      IF l_rec.build IS NOT NULL THEN
        -- revert schema changes first - apply in reverse order
        -- drop all revert tables and rename synonyms
        l_object_metadata := cort_aux_pkg.get_object_metadata(
                               in_object_type   => NULL, -- all objects
                               in_object_name   => NULL, -- all objects
                               in_object_owner  => NULL, -- all schemas
                               in_application   => l_rec.application,
                               in_release       => l_rec.release,
                               in_build         => l_rec.build
                             );
        -- revert non replacable objects in reverse order
        FOR i IN REVERSE 1..l_object_metadata.COUNT LOOP
          IF l_object_metadata(i).object_type IN ('TABLE','INDEX','INDEXES','SEQUENCE') THEN
            int_revert_object(
              in_object_type  => l_object_metadata(i).object_type,
              in_object_name  => l_object_metadata(i).object_name,
              in_object_owner => l_object_metadata(i).object_owner,
              in_params_rec   => l_params
            );
          END IF;
        END LOOP;

        -- revert replacable object first
        FOR i IN 1..l_object_metadata.COUNT LOOP
          IF l_object_metadata(i).object_type NOT IN ('TABLE','INDEX','INDEXES','SEQUENCE') THEN
            int_revert_object(
              in_object_type  => l_object_metadata(i).object_type,
              in_object_name  => l_object_metadata(i).object_name,
              in_object_owner => l_object_metadata(i).object_owner,
              in_params_rec   => l_params
            );
          END IF;
        END LOOP;


        IF NOT l_params.test.get_bool_value THEN
          l_rec.status := 'REVERTED';
          l_rec.end_time := SYSDATE;
          cort_aux_pkg.update_build(l_rec);
          dbms_application_info.set_client_info(null);
        END IF;
      ELSE
        cort_exec_pkg.raise_error( 'Build for application '||l_application||' not found');
      END IF;
    END IF;
  END revert_build;

  -- Create error table for given table
  PROCEDURE create_error_table(
    in_main_table_name IN VARCHAR2,
    in_err_table_name  IN VARCHAR2,
    in_table_owner     IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_system_columns  IN arrays.gt_str_tab DEFAULT NULL
 )
  AS
    l_sql     CLOB;
    l_clause  VARCHAR2(1000);
  BEGIN
    l_sql := 'CREATE /*# OR REPLACE */ TABLE "'||in_table_owner||'"."'||in_err_table_name||'" (
    ORA_ERR_NUMBER$  NUMBER,
    ORA_ERR_MESG$    VARCHAR2(2000),
    ORA_ERR_ROWID$   UROWID(4000),
    ORA_ERR_OPTYP$   VARCHAR2(2),
    ORA_ERR_TAG$     VARCHAR2(2000)';
    FOR x in (SELECT *
                FROM all_tab_columns
               WHERE table_name = in_main_table_name
                 AND owner = in_table_owner
               ORDER BY column_id)
    LOOP
      IF x.column_name MEMBER OF SET(in_system_columns) THEN
        IF x.data_type_owner IS NULL THEN
          CASE
           WHEN x.data_type IN ('CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2') THEN
             l_clause := x.data_type||'('||x.char_length||')';
           WHEN x.data_type = 'NUMBER' AND x.data_precision IS NOT NULL THEN
             l_clause := x.data_type||'('||x.data_precision||','||x.data_scale||')';
           WHEN x.data_type = 'RAW' THEN
             l_clause := x.data_type||'('||x.data_length||')';
           ELSE
             l_clause := x.data_type;
          END CASE;
        ELSE
          l_clause := x.data_type_mod||' "'||x.data_type_owner||'"."'||x.data_type||'" ';
        END IF;
      ELSE
        l_clause := 'VARCHAR2(4000)';
      END IF;
        l_sql := l_sql||',
    '||x.column_name||'  '||l_clause;
    END LOOP;
    l_sql := l_sql||')';
    cort_event_exec_pkg.instead_of_create(
      in_object_type   => 'TABLE',
      in_object_name   => in_err_table_name,
      in_object_owner  => in_table_owner,
      in_sql           => l_sql
    );
  END create_error_table;

  FUNCTION get_cort_ddl(
    in_table_name          IN VARCHAR2, 
    in_table_owner         IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_all_partitions      IN BOOLEAN DEFAULT TRUE,
    in_segment_attributes  IN BOOLEAN DEFAULT FALSE,
    in_storage             IN BOOLEAN DEFAULT FALSE,
    in_tablespace          IN BOOLEAN DEFAULT FALSE,
    in_size_byte_keyword   IN BOOLEAN DEFAULT FALSE
  )
  RETURN CLOB
  AS
    l_result CLOB;
    l_xml    XMLType;
    l_sxml   CLOB;
    h        number;
    tr       number;
    ku$      sys.ku$_SubmitResults;
  BEGIN
    dbms_metadata.set_transform_param(dbms_metadata.session_transform,'DEFAULT', true);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',in_segment_attributes);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',in_storage);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',in_tablespace);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SIZE_BYTE_KEYWORD',in_size_byte_keyword);
    
    IF in_all_partitions THEN
      l_result := dbms_metadata.get_ddl(
                    object_type => 'TABLE',
                    name        => in_table_name,
                    schema      => in_table_owner
                  );
    ELSE
      l_xml := xmlType(dbms_metadata.get_xml(
                      object_type => 'TABLE',
                      name        => in_table_name,
                      schema      => in_table_owner
                    ));
      SELECT UPDATEXML(
                 l_xml, 
                 '//PART_LIST', XMLType('<PART_LIST>'||extract(l_xml, '//PART_LIST/PART_LIST_ITEM[1]').getClobVal()||'</PART_LIST>'),
                 '//COMPART_LIST', XMLType('<COMPART_LIST>'||extract(l_xml, '//COMPART_LIST/COMPART_LIST_ITEM[1]').getClobVal()||'</COMPART_LIST>')
               ) 
        INTO l_xml 
        FROM dual;
      dbms_lob.createtemporary(l_result, TRUE);
      h := dbms_metadata.openw('TABLE');
      tr := dbms_metadata.add_transform(h, 'DDL');
      dbms_metadata.set_transform_param(tr,'DEFAULT',true);
      dbms_metadata.set_transform_param(tr,'SEGMENT_ATTRIBUTES',in_segment_attributes);
      dbms_metadata.set_transform_param(tr,'STORAGE',in_storage);
      dbms_metadata.set_transform_param(tr,'TABLESPACE',in_tablespace);
      dbms_metadata.set_transform_param(tr,'SIZE_BYTE_KEYWORD',in_size_byte_keyword);
      dbms_metadata.convert(h, l_xml, l_result);
      dbms_metadata.close(h);
    END IF;           
    l_result := regexp_replace(l_result, 'CREATE\W', 'CREATE /*'||cort_params_pkg.gc_prefix||' OR REPLACE */ ', 1, 1);
    RETURN l_result;
  END get_cort_ddl;
    

END cort_pkg;
/