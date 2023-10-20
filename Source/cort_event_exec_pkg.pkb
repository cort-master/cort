CREATE OR REPLACE PACKAGE BODY cort_event_exec_pkg
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
  Description: event execution API
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  17.00   | Rustam Kafarov    | Added package to execute actions on events with current user privileges
  20.00   | Rustam Kafarov    | Added support of long names introduced in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------
*/



  -- returns object last modification date
  FUNCTION get_object_last_ddl_time(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN DATE
  AS
    l_last_ddl_time DATE;
  BEGIN
    BEGIN
      SELECT TO_DATE(timestamp, 'yyyy-mm-dd:hh24:mi:ss')
        INTO l_last_ddl_time
        FROM all_objects
       WHERE owner = in_object_owner
         AND object_name = in_object_name
         AND object_type = in_object_type
         AND subobject_name IS NULL;
    EXCEPTION
      WHEN OTHERS THEN
        l_last_ddl_time := NULL;
    END;
    RETURN l_last_ddl_time;
  END get_object_last_ddl_time;


  -- Check if executed SQL and objects's last_ddl_time match to the last registered corresponded parameters
  FUNCTION is_object_modified(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_sql_text     IN CLOB
  )
  RETURN BOOLEAN
  AS
    l_rec           cort_objects%ROWTYPE;
    l_result        BOOLEAN;
    l_cnt           PLS_INTEGER := 0;
  BEGIN
    l_result := TRUE;
    l_rec := cort_aux_pkg.get_last_change(
               in_object_owner => in_object_owner,
               in_object_type  => in_object_type,
               in_object_name  => in_object_name
             );
    IF in_object_type = 'TABLE' THEN
      SELECT COUNT(*)
        INTO l_cnt
        FROM all_object_tables
       WHERE owner = in_object_owner
         AND table_name = in_object_name;
    
      IF l_rec.last_ddl_text = in_sql_text AND l_cnt = 0 THEN
        IF l_rec.last_ddl_time >= get_object_last_ddl_time(
                                    in_object_owner => in_object_owner,
                                    in_object_name  => in_object_name,
                                    in_object_type  => in_object_type
                                  ) AND
          l_rec.change_type != 'create_as_select'
        THEN
          l_result := FALSE;
        END IF;
      END IF;
    
    END IF;
    RETURN l_result;
  END is_object_modified;

  -- Run infinit loop until dbms_scheduler job is done
  PROCEDURE wait_for_job_end(
    in_rec IN cort_jobs%ROWTYPE
  )
  AS
    l_rec              cort_jobs%ROWTYPE;
    l_job_status       all_scheduler_job_run_details.status%TYPE;
    l_last_job_status  all_scheduler_job_run_details.status%TYPE;
    l_additional_info  all_scheduler_job_run_details.additional_info%TYPE;
  BEGIN
    IF in_rec.status IS NULL THEN
      --  Unknown status
      RETURN;
    END IF;

    l_last_job_status := 'SCHEDULED';
    -- start waiting loop
    LOOP
      -- check that running job exists
      BEGIN
        SELECT state
          INTO l_job_status
          FROM all_scheduler_jobs
         WHERE owner = l_rec.job_owner
           AND job_name = l_rec.job_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          BEGIN
            -- if there is no record check if record in log
            SELECT status
              INTO l_job_status
              FROM all_scheduler_job_log
             WHERE owner = l_rec.job_owner
               AND job_name = l_rec.job_name
               AND log_date >= l_rec.job_id
               AND ROWNUM <= 1;
          EXCEPTION
             WHEN NO_DATA_FOUND THEN
               NULL;
          END;
      END;
      EXIT when l_job_status NOT IN ('SCHEDULED', 'RUNNING');
      IF l_job_status = l_last_job_status THEN
        NULL;
      ELSE
        l_last_job_status := l_job_status;
      END IF;
      -- Read cort_job status
      l_rec := cort_job_pkg.get_job_rec(
                 in_job_id    => in_rec.job_id
               );

      IF l_rec.status = 'PENDING' THEN
        EXIT;
      END IF;
      -- sleep for half second
      DBMS_LOCK.SLEEP(0.5);
    END LOOP;

    -- Read job status
    BEGIN
      SELECT status, additional_info
        INTO l_job_status, l_additional_info
        FROM (SELECT *
                FROM all_scheduler_job_run_details
               WHERE owner = l_rec.job_owner
                 AND job_name = l_rec.job_name
                 AND req_start_date >= l_rec.job_id
               ORDER BY log_date DESC
             )
       WHERE ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_job_status := 'UNKNOWN';
    END;

    -- Read cort_job status
    l_rec := cort_job_pkg.get_job_rec(
               in_job_id    => in_rec.job_id
             );

    IF l_job_status <> 'SUCCEEDED' AND l_rec.status NOT IN ('PENDING', 'COMPLETED') THEN
      cort_log_pkg.error('DBMS_SCHEDULER Job status is '||l_job_status||' but record in cort_job has status '||l_rec.status||'. Failing job');
      cort_job_pkg.fail_job(
        in_rec             => l_rec,
        in_error_message   => l_additional_info
      );
    END IF;

  END wait_for_job_end;


  -- run registred job via DBMS_SCHEDULER in the same session
  PROCEDURE run_job(
    in_rec    IN cort_jobs%rowtype,
    in_async  IN BOOLEAN,
    in_wait   IN BOOLEAN DEFAULT TRUE
  )
  AS
  PRAGMA autonomous_transaction;
    l_job_full_name       arrays.gt_full_name;
    l_job_action          VARCHAR2(200);
    e_job_already_exists  EXCEPTION;
    PRAGMA                EXCEPTION_INIT(e_job_already_exists, -27477);
  BEGIN
    l_job_full_name := '"'||in_rec.job_owner||'"."'||in_rec.job_name||'"';
    IF in_rec.action = 'THREADING' THEN
      l_job_action := 'CORT_THREAD_EXEC_PKG.EXECUTE_THREAD';
    ELSE
      l_job_action := 'CORT_EXEC_PKG.EXECUTE_ACTION';
    END IF;  

    -- create dbms_scheduler job
    BEGIN
      dbms_scheduler.create_job(
        job_name            => l_job_full_name,
        job_type            => 'STORED_PROCEDURE',
        job_action          => l_job_action,
        number_of_arguments => 1,
        auto_drop           => TRUE,
        enabled             => FALSE
      );
    EXCEPTION
      WHEN e_job_already_exists THEN
        cort_exec_pkg.debug('Job '||l_job_full_name||' alreasy exists');
        NULL;
    END;
    dbms_scheduler.set_job_argument_value(
      job_name          => l_job_full_name,
      argument_position => 1,
      argument_value    => in_rec.job_id
    );

    IF in_async THEN
      dbms_scheduler.enable(
        name => l_job_full_name
      );

      -- start job
      COMMIT;

      IF in_wait THEN
        -- start waiting ...
        wait_for_job_end(in_rec => in_rec);
      END IF;

    ELSE
      BEGIN
        -- execute in the current session
        dbms_scheduler.run_job(
           job_name            => l_job_full_name,
           use_current_session => TRUE
        );
      EXCEPTION
        WHEN OTHERS THEN
          -- drop job
          dbms_scheduler.drop_job(
            job_name            => l_job_full_name
          );
          cort_job_pkg.fail_job(
            in_rec             => in_rec,
            in_error_message   => sqlerrm
          );
          RAISE;
      END;
      -- drop job
      dbms_scheduler.drop_job(
        job_name            => l_job_full_name
      );
    END IF;

    COMMIT;

  END run_job;


  -- run action synchronously or asynchronously
  PROCEDURE run_action(
    in_rec           IN cort_jobs%rowtype,
    in_async         IN BOOLEAN,
    in_wait          IN BOOLEAN DEFAULT TRUE
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec                 cort_jobs%ROWTYPE;
  BEGIN
    run_job(
      in_rec   => in_rec,
      in_async => in_async,
      in_wait  => in_wait
    );

    IF NOT in_async OR in_wait THEN
      -- reread job rec to get status
      l_rec := cort_job_pkg.get_job_rec(
                 in_job_id    => in_rec.job_id
               );
      -- output whatever is written into job_rec.output
      cort_aux_pkg.output(l_rec.output);

      IF (l_rec.status = 'RUNNING') OR (l_rec.status IS NULL) THEN
        cort_job_pkg.cancel_job(
          in_rec             => l_rec,
          in_error_message   => 'Job '||l_rec.job_name||' cancelled'
        );
        cort_exec_pkg.raise_error('Job '||l_rec.job_name||' cancelled');
      ELSIF (l_rec.status = 'FAILED') THEN
        cort_exec_pkg.raise_error(l_rec.error_message);
      END IF;
    END IF;

  END run_action;

  -- process event action via job or directly
  FUNCTION process_event(
    in_async         IN BOOLEAN,
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB,
    in_new_name      IN VARCHAR2,
    in_params_rec    IN cort_params_pkg.gt_run_params_rec
  )
  RETURN cort_jobs%ROWTYPE
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_rec                 cort_jobs%ROWTYPE;
  BEGIN  
    -- lock for control session is created here:
    l_rec := cort_job_pkg.register_job(
               in_action         => in_action,
               in_object_type    => in_object_type,
               in_object_name    => in_object_name,
               in_object_owner   => in_object_owner,
               in_sql            => in_sql,
               in_current_schema => SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
               in_new_name       => in_new_name,
               in_params_rec     => in_params_rec
             );

    run_action(
      in_rec   => l_rec,
      in_async => in_async,
      in_wait  => NOT in_params_rec.async.get_bool_value
    );
    -- release lock at the end
    COMMIT;
    RETURN l_rec;
  END process_event;

  PROCEDURE process_event(
    in_async         IN BOOLEAN,
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB,
    in_new_name      IN VARCHAR2 DEFAULT NULL,
    in_params_rec    IN cort_params_pkg.gt_run_params_rec DEFAULT cort_session_pkg.get_params
  )
  AS
    l_rec                 cort_jobs%ROWTYPE;
  BEGIN
    l_rec := cort_job_pkg.find_pending_job(
               in_object_name  => in_object_name,
               in_object_owner => in_object_owner
             );
    IF l_rec.action = 'CREATE_OR_REPLACE' THEN
      l_rec.action := 'RESUME_RECREATE';
      l_rec.sid := dbms_session.unique_session_id;
      resume_process(
        in_rec => l_rec
      );
    ELSE
      l_rec := process_event(
                 in_async         => in_async,
                 in_action        => in_action,
                 in_object_type   => in_object_type,
                 in_object_name   => in_object_name,
                 in_object_owner  => in_object_owner,
                 in_sql           => in_sql,
                 in_new_name      => in_new_name,
                 in_params_rec    => in_params_rec
               );
    END IF;

  END process_event;

  -- resume pending process
  PROCEDURE resume_process(
    in_rec IN cort_jobs%ROWTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_rec                 cort_jobs%ROWTYPE;
  BEGIN
    l_rec := cort_job_pkg.resume_job(
               in_job_rec => in_rec
             );

    run_action(
      in_rec   => l_rec,
      in_async => FALSE
    );
    -- release lock at the end
    COMMIT;
  END resume_process;

  -- Run create or replace for non recreatable objects
  PROCEDURE instead_of_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  )
  AS
    l_run_params          cort_params_pkg.gt_run_params_rec;
  BEGIN
    l_run_params := cort_session_pkg.get_params;
    -- get run params values from hints
    cort_parse_pkg.parse_run_params(in_sql, l_run_params);
    
    -- if test mode 
    IF l_run_params.test.get_bool_value OR
       -- or object has been changed
       is_object_modified(
         in_object_type  => in_object_type,
         in_object_name  => in_object_name,
         in_object_owner => in_object_owner,
         in_sql_text     => in_sql
       ) OR 
       cort_job_pkg.find_pending_job(
         in_object_name  => in_object_name,
         in_object_owner => in_object_owner
       ).action = 'CREATE_OR_REPLACE'
    THEN
      process_event(
        in_async        => TRUE,
        in_action       => 'CREATE_OR_REPLACE',
        in_object_type  => in_object_type,
        in_object_name  => in_object_name,
        in_object_owner => in_object_owner,
        in_sql          => in_sql,
        in_params_rec   => l_run_params 
      );
    ELSE
      -- do not execute the job
      cort_exec_pkg.debug('No changes since last operation applied');
    END IF;
  END instead_of_create;

  -- Register metadata of recreatable objects
  PROCEDURE before_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  )
  AS
  BEGIN
    process_event(
      in_async        => FALSE,
      in_action       => 'REGISTER',
      in_object_type  => in_object_type,
      in_object_name  => in_object_name,
      in_object_owner => in_object_owner,
      in_sql          => in_sql
    );
  END before_create;

  FUNCTION get_xplan_statement(in_xplan_id IN VARCHAR2)
  RETURN CLOB
  AS
    l_explan       VARCHAR2(32767);
    l_sql_text     CLOB;
    l_owner        arrays.gt_name;
  BEGIN
    IF cort_options_pkg.gc_explain_plan THEN
      l_owner := cort_parse_pkg.get_regexp_const(SYS_CONTEXT('USERENV','CURRENT_USER'));
      l_explan := 'explain\s+plan\s+set\s+statement_id\s*=\s*'''||cort_parse_pkg.get_regexp_const(in_xplan_id)||'''\s*(into\s+("?'||l_owner||'"?\s*.\s*)?"?PLAN_TABLE"?\s*)?for\s+';

      l_sql_text := cort_aux_pkg.get_explain_sql(l_explan);

      IF REGEXP_INSTR(l_sql_text, l_explan, 1, 1, 0, 'i') = 1 THEN
        l_sql_text := REGEXP_REPLACE(l_sql_text, l_explan, NULL, 1, 1, 'i');
      ELSE
        l_sql_text := NULL;
      END IF;
    END IF;
    RETURN l_sql_text;
  END get_xplan_statement;

  -- Test CORT changes and add results into PLAN_TABLE$
  PROCEDURE explain_cort_sql(
    in_statement_id  IN VARCHAR2,
    in_plan_id       IN NUMBER,
    in_timestamp     IN DATE,
    out_sql          OUT CLOB,
    out_last_id      OUT NUMBER
  )
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_sql           CLOB;
    l_object_type   VARCHAR2(100);
    l_object_name   VARCHAR2(100);
    l_object_owner  VARCHAR2(100);
    l_text_arr      arrays.gt_lstr_arr;
    l_revert_arr    arrays.gt_lstr_arr;
    l_id_arr        arrays.gt_num_arr;
    l_last_id       NUMBER;
    l_rec           cort_jobs%ROWTYPE;
    l_params        cort_params_pkg.gt_run_params_rec;
    l_shift         NUMBER;
  BEGIN
    IF cort_pkg.get_status = 'ENABLED' AND
       cort_session_pkg.get_status = 'ENABLED'
    THEN
      l_sql := get_xplan_statement(in_statement_id);
      IF cort_trg_pkg.is_replace_mode(in_sql => l_sql) THEN
        cort_parse_pkg.parse_explain_sql(
          in_sql           => l_sql,
          out_object_type  => l_object_type,
          out_object_owner => l_object_owner,
          out_object_name  => l_object_name
        );
        l_object_owner := NVL(l_object_owner,SYS_CONTEXT('USERENV','CURRENT_SCHEMA'));
        IF l_object_type IN ('TABLE','SEQUENCE','TYPE') AND
           l_object_name IS NOT NULL
        THEN
          out_sql := l_sql;
          l_params := cort_session_pkg.get_params;
          -- Enable TEST mode for the current session
          l_params.test.set_value(TRUE);
          l_params.debug.set_value(FALSE);
          l_rec := process_event(
                     in_async        => TRUE,
                     in_action       => 'EXPLAIN_PLAN_FOR',
                     in_object_type  => l_object_type,
                     in_object_name  => l_object_name,
                     in_object_owner => l_object_owner,
                     in_sql          => l_sql,
                     in_new_name     => NULL,
                     in_params_rec   => l_params
                   );

          COMMIT;
          
          SELECT text, revert_ddl
            BULK COLLECT
            INTO l_text_arr, l_revert_arr
            FROM cort_log
           WHERE action = 'EXPLAIN_PLAN_FOR'
             AND job_id = l_rec.job_id
             AND log_type = 'TEST'
           ORDER BY log_time;

          -- disable cort triggers
          cort_session_pkg.disable;
          BEGIN

            l_shift := 0;
            FOR i IN 1..l_text_arr.COUNT LOOP
              l_id_arr(i) := l_shift + i;
              IF upper(l_text_arr(i)) like 'CREATE TABLE%' THEN
                l_shift := 100000;
                l_last_id := i;
              END IF;
            END LOOP;

            FORALL i IN 1..l_text_arr.COUNT
              INSERT INTO plan_table(statement_id, plan_id, timestamp, parent_id, id, operation, depth, access_predicates)
              VALUES(in_statement_id, in_plan_id, in_timestamp, 0, l_id_arr(i), SUBSTR(l_text_arr(i),1,1000), 1, l_revert_arr(i));

          EXCEPTION
            WHEN OTHERS THEN
              -- enable cort triggers before raise exception
              cort_session_pkg.enable;
              RAISE;
          END;
          cort_session_pkg.enable;
          out_last_id := l_last_id;
        END IF;

      END IF;
    END IF;
    COMMIT;
  END explain_cort_sql;

  -- Prevent any DDL for object changing by CORT
  PROCEDURE lock_object(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    out_lock_error   OUT VARCHAR2
  )
  AS
    l_rec cort_jobs%ROWTYPE;
  BEGIN
    l_rec := cort_job_pkg.find_active_job(
               in_object_name   => in_object_name,
               in_object_owner  => in_object_owner
            );
    IF (l_rec.status = 'RUNNING' AND cort_job_pkg.is_object_locked(l_rec.object_owner,l_rec.object_name,'JOB SESSION')) OR 
       (l_rec.status = 'REGISTERED' AND cort_job_pkg.is_object_locked(l_rec.object_owner,l_rec.object_name,'CONTROL SESSION'))
    THEN
      out_lock_error := 'Object '||in_object_type||' "'||in_object_owner||'"."'||in_object_name||'" is changing by another CORT session';
    END IF;
    IF (l_rec.status = 'PENDING')
    THEN
      out_lock_error := 'Object '||in_object_type||' "'||in_object_owner||'"."'||in_object_name||'" is pending for CORT change';
    END IF;
  END lock_object;

END cort_event_exec_pkg;
/