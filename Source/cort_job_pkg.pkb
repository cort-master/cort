CREATE OR REPLACE PACKAGE BODY cort_job_pkg
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
  Description: job execution API
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added procedure for running job in same session. Improved waiting cycle.
  15.00   | Rustam Kafarov    | Added support for manual execution
  17.00   | Rustam Kafarov    | Added functions find_active_job, find_pending_job. Moved all dbms_scheduler dependent API to cort_event_exec_pkg
  18.00   | Rustam Kafarov    | Fixing behaviour for create as from itself
  19.00   | Rustam Kafarov    | Revised parameters
  20.00   | Rustam Kafarov    | Added support of long names introduced in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------
*/

  -- Public

  -- Return job record
  FUNCTION get_job_rec(
    in_sid           IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_jobs%ROWTYPE
  AS
    l_rec              cort_jobs%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE sid = in_sid
         AND object_name = in_object_name
         AND object_owner = in_object_owner;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        cort_exec_pkg.debug('Record with sid = '||in_sid||', object_owner = '||in_object_owner||' object_name = '||in_object_name||' not found in cort_jobs');
        RAISE;
    END;
    RETURN l_rec;
  END get_job_rec;


  -- Return running job record and assign it to current session (set job_session_id)
  FUNCTION assign_job(
    in_sid IN VARCHAR2
  )
  RETURN cort_jobs%ROWTYPE
  AS
  PRAGMA autonomous_transaction;
    l_rec cort_jobs%ROWTYPE;
  BEGIN
    SELECT *
      INTO l_rec
      FROM cort_jobs
     WHERE sid = in_sid
       AND status = 'RUNNING'
       FOR UPDATE WAIT 1;

    UPDATE cort_jobs
       SET job_sid = dbms_session.unique_session_id
     WHERE sid = in_sid
       AND status = 'RUNNING';

    cort_log_pkg.log_job(
      in_status    => 'RUNNING',
      in_job_rec   => l_rec
    );

    COMMIT;

    RETURN l_rec;
  END assign_job;

  -- Find pending job for given object
  FUNCTION find_pending_job(
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_jobs%ROWTYPE
  AS
    l_rec  cort_jobs%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE object_name = in_object_name
         AND object_owner = in_object_owner
         AND decode(status,'RUNNING',nvl(substr(sid,13),'0'),'PENDING','X',sid) = 'X'
         AND status = 'PENDING';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    RETURN l_rec;
  END find_pending_job;

  -- Find running or pending job for given object
  FUNCTION find_active_job(
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_jobs%ROWTYPE
  AS
    l_rec  cort_jobs%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE object_name = in_object_name
         AND object_owner = in_object_owner
         AND DECODE(status,'RUNNING','0','PENDING','0',sid) = '0';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    RETURN l_rec;
  END find_active_job;

  FUNCTION is_job_alive(
    in_job_rec IN  cort_jobs%ROWTYPE
  )
  RETURN BOOLEAN
  AS
  PRAGMA autonomous_transaction;
    l_sid                  VARCHAR2(30);
    e_resource_busy        EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
  BEGIN
    BEGIN
      SELECT sid
        INTO l_sid
        FROM cort_job_control
       WHERE object_type = in_job_rec.object_type
         AND object_name = in_job_rec.object_name
         AND object_owner = in_job_rec.object_owner
         AND sid = in_job_rec.sid
         FOR UPDATE NOWAIT;
    EXCEPTION
      WHEN e_resource_busy THEN
        ROLLBACK;
        RETURN TRUE;
      WHEN NO_DATA_FOUND THEN
        ROLLBACK;
        RETURN FALSE;
    END;
    ROLLBACK;
    RETURN FALSE;
  END is_job_alive;

  PROCEDURE check_sibling_jobs_alive(
    in_job_rec IN  cort_jobs%ROWTYPE
  )
  AS
  PRAGMA autonomous_transaction;
    l_sid                  VARCHAR2(30);
  BEGIN
    SELECT MAX(sid) KEEP(DENSE_RANK FIRST ORDER BY job_time)
      INTO l_sid
      FROM cort_jobs
     WHERE object_type = in_job_rec.object_type
       AND object_name = in_job_rec.object_name
       AND object_owner = in_job_rec.object_owner
       AND sid like SUBSTR(in_job_rec.sid, 1, 12)||'%'
       AND status in ('FAILED','CANCELED');

    IF l_sid IS NOT NULL THEN
      cort_exec_pkg.raise_error('Operation cancelled for sid = '||l_sid, -20900);
    END IF;
  END check_sibling_jobs_alive;


  -- add record for given job.
  FUNCTION register_job(
    in_sid            IN VARCHAR2,
    in_job_owner      IN VARCHAR2,
    in_action         IN VARCHAR2,
    in_object_type    IN VARCHAR2,
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_sql            IN CLOB,
    in_current_schema IN VARCHAR2,
    in_params_rec     IN cort_params_pkg.gt_params_rec,
    in_new_name       IN VARCHAR2 DEFAULT NULL,
    in_resume_action  IN VARCHAR2 DEFAULT NULL
  )
  RETURN cort_jobs%ROWTYPE
  AS
  PRAGMA autonomous_transaction;
    l_job_name            arrays.gt_name;
    l_cnt                 PLS_INTEGER;
    l_rec                 cort_jobs%ROWTYPE;
    l_new_rec             cort_jobs%ROWTYPE;
  BEGIN
    l_job_name := gc_cort_job_name||in_sid;

    cort_exec_pkg.debug('Registering job "'||in_job_owner||'"."'||l_job_name||'"');

    -- Check that there is no RUNNING JOB for another object in currency session
    l_cnt := 0;
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE decode(status,'RUNNING',sid) = in_sid;
      l_cnt := 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;

    IF l_cnt = 1 THEN
      IF is_job_alive(l_rec)
      THEN
        -- job is still running
        cort_exec_pkg.raise_error('There is a job attached to this session running in the background');
      ELSE
        DELETE FROM cort_jobs
         WHERE sid = in_sid
           AND status = 'RUNNING';
      END IF;
    END IF;

    -- Check that there is no RUNNING job for current object in another session
    l_cnt := 0;
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE object_name = in_object_name
         AND object_owner = in_object_owner
         AND DECODE(status,'RUNNING',NVL(SUBSTR(sid,13),'0'),'PENDING','X',sid) = '0'
         AND SUBSTR(sid,1,12) <> SUBSTR(in_sid,1,12) -- not slave thread process
         FOR UPDATE WAIT 1;
      l_cnt := 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;

    IF l_cnt = 1 THEN
      -- Check that found sessions are stil alive
      IF l_rec.status = 'RUNNING' AND is_job_alive(l_rec)
      THEN
        -- release locks
        ROLLBACK;
        -- sessions are alive
        cort_exec_pkg.raise_error('Object "'||in_object_owner||'"."'||in_object_name||'" is changing by another process');
      ELSIF l_rec.status = 'PENDING' THEN
        -- release locks
        ROLLBACK;
        -- sessions are alive
        cort_exec_pkg.raise_error('Object "'||in_object_owner||'"."'||in_object_name||'" change is pending for completion');
      ELSE
        -- cancel job for dead session
        UPDATE cort_jobs
           SET status = 'CANCELED'
         WHERE sid = l_rec.sid
           AND object_name = in_object_name
           AND object_owner = in_object_owner;
      END IF;
    END IF;

    -- delete all slave thread jobs as well
    DELETE FROM cort_jobs
     WHERE substr(sid,1,12) = in_sid
       AND object_name = in_object_name
       AND object_owner = in_object_owner;

    l_new_rec.sid := in_sid;
    l_new_rec.action := in_action;
    l_new_rec.resume_action := in_resume_action;
    l_new_rec.status := 'RUNNING';
    l_new_rec.job_time := SYSTIMESTAMP;
    l_new_rec.job_owner := in_job_owner;
    l_new_rec.job_name := l_job_name;
    l_new_rec.object_type := in_object_type;
    l_new_rec.object_owner := in_object_owner;
    l_new_rec.object_name := in_object_name;
    l_new_rec.sql_text := in_sql;
    l_new_rec.new_name := in_new_name;
    l_new_rec.current_schema := in_current_schema;
    l_new_rec.application := cort_session_pkg.get_param_value('APPLICATION');
    l_new_rec.release := cort_pkg.get_current_release;
    l_new_rec.build := SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12);
    l_new_rec.session_params := cort_params_pkg.rec_to_xml(in_params_rec);
    l_new_rec.session_id := SYS_CONTEXT('USERENV','SID');
    l_new_rec.username := USER;
    l_new_rec.osuser := SYS_CONTEXT('USERENV','OS_USER');
    l_new_rec.machine := SYS_CONTEXT('USERENV','HOST');
    l_new_rec.terminal := SYS_CONTEXT('USERENV','TERMINAL');
    l_new_rec.module := SYS_CONTEXT('USERENV','MODULE');

    cort_log_pkg.log_job(
      in_status       => 'REGISTERING',
      in_job_rec    => l_new_rec
    );

    -- Try to register new job
    INSERT INTO cort_jobs
    VALUES l_new_rec;


    IF is_job_alive(l_new_rec) THEN
      ROLLBACK;
      cort_exec_pkg.raise_error('There is a job attached to this session running in the background');
    END IF;

    DELETE FROM cort_job_control
     WHERE object_type = in_object_type
       AND object_name = in_object_name
       AND object_owner = in_object_owner
       AND sid = in_sid;

    INSERT INTO cort_job_control trg
    VALUES(l_new_rec.object_type, l_new_rec.object_owner, l_new_rec.object_name, l_new_rec.sid);

    COMMIT;

    RETURN l_new_rec;

  END register_job;

  -- Create lock for starting job
  PROCEDURE start_job(
    in_rec           IN cort_jobs%ROWTYPE
  )
  AS
    l_sid                  VARCHAR2(30);
    e_resource_busy        EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
  BEGIN
    BEGIN
      SELECT sid
        INTO l_sid
        FROM cort_job_control
       WHERE object_type = in_rec.object_type
         AND object_name = in_rec.object_name
         AND object_owner = in_rec.object_owner
         AND sid = in_rec.sid
         FOR UPDATE NOWAIT;
    EXCEPTION
      WHEN e_resource_busy THEN
        cort_exec_pkg.raise_error('Job already running for '||in_rec.object_type||'  "'||in_rec.object_owner||'"."'||in_rec.object_name||'"');
    END;
  END start_job;

  PROCEDURE write_output(
    io_rec IN OUT NOCOPY cort_jobs%ROWTYPE
  )
  AS
    l_text_arr arrays.gt_clob_arr;
  BEGIN
    SELECT details
      BULK COLLECT
      INTO l_text_arr
      FROM cort_log
     WHERE sid = io_rec.sid
       AND object_owner = io_rec.object_owner
       AND object_name = io_rec.object_name
       AND job_time = io_rec.job_time
       AND log_type = 'ECHO'
     ORDER BY log_time;
    FOR i IN 1..l_text_arr.COUNT LOOP
      io_rec.output := io_rec.output||l_text_arr(i)||chr(13);
    END LOOP;
  END write_output;


  -- Finish job
  PROCEDURE success_job(
    in_rec IN cort_jobs%ROWTYPE
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec    cort_jobs%ROWTYPE;
  BEGIN
    IF in_rec.status IN ('RUNNING','PENDING') THEN
      l_rec := in_rec;

      write_output(l_rec);

      cort_log_pkg.log_job(
        in_status  => 'COMPLETING',
        in_job_rec => l_rec
      );

      UPDATE cort_jobs
         SET status = 'COMPLETED',
             output = l_rec.output,
             run_time = systimestamp - job_time
       WHERE sid = in_rec.sid
         AND object_name = in_rec.object_name
         AND object_owner = in_rec.object_owner;
    END IF;

    COMMIT;
  END success_job;

  -- Set job to PENDING status
  PROCEDURE suspend_job(
    in_rec IN cort_jobs%ROWTYPE
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec    cort_jobs%ROWTYPE;
  BEGIN
    IF in_rec.status = 'RUNNING' THEN
      l_rec := in_rec;

      write_output(l_rec);

      cort_log_pkg.log_job(
        in_status   => 'PENDING',
        in_job_rec  => l_rec
      );

      UPDATE cort_jobs
         SET status = 'PENDING',
             output = l_rec.output,
             run_time = systimestamp - job_time
       WHERE sid = in_rec.sid
         AND object_name = in_rec.object_name
         AND object_owner = in_rec.object_owner;

    END IF;

    COMMIT;
  END suspend_job;

  -- Cancel running job
  PROCEDURE cancel_job(
    in_rec             IN cort_jobs%ROWTYPE,
    in_error_message   IN VARCHAR2 DEFAULT NULL
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec    cort_jobs%ROWTYPE;
  BEGIN
    IF in_rec.status in ('RUNNING','PENDING') THEN
      l_rec := in_rec;
      l_rec.error_message   := in_error_message;
      l_rec.error_code      := sqlcode;
      l_rec.error_backtrace := dbms_utility.format_error_backtrace;
      l_rec.call_stack      := dbms_utility.format_call_stack;
      l_rec.error_stack     := cort_log_pkg.get_error_stack;

      write_output(l_rec);

      cort_log_pkg.log_job(
        in_status  => 'CANCELLING',
        in_job_rec => in_rec
      );

      UPDATE cort_jobs
         SET status          = 'CANCELED',
             output          = l_rec.output,
             run_time        = systimestamp - job_time,
             error_code      = l_rec.error_code,
             error_message   = l_rec.error_message,
             error_backtrace = l_rec.error_backtrace,
             call_stack      = l_rec.call_stack,
             error_stack     = l_rec.error_stack
       WHERE sid = in_rec.sid
         AND object_name = in_rec.object_name
         AND object_owner = in_rec.object_owner;

      COMMIT;

      IF in_rec.action = 'CREATE_OR_REPLACE' THEN
        -- recursive calcelling of all dependent jobs
        FOR x IN (SELECT *
                    FROM cort_jobs
                   WHERE parent_object_type = in_rec.object_type
                     AND parent_object_name = in_rec.object_name
                     AND parent_object_owner = in_rec.object_owner
                     AND status in ('PENDING', 'RUNNING'))
        LOOP
          cancel_job(x);
        END LOOP;
      END IF;

    END IF;

    COMMIT;
  END cancel_job;

  -- Finish job with error
  PROCEDURE fail_job(
    in_rec             IN cort_jobs%ROWTYPE,
    in_error_message   IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec    cort_jobs%ROWTYPE;
  BEGIN
    l_rec := in_rec;
    l_rec.error_message   := in_error_message;
    l_rec.error_code      := sqlcode;
    l_rec.error_backtrace := dbms_utility.format_error_backtrace;
    l_rec.call_stack      := dbms_utility.format_call_stack;
    l_rec.error_stack     := cort_log_pkg.get_error_stack;

    write_output(l_rec);

    cort_log_pkg.log_job(
      in_status  => 'FAILED',
      in_job_rec => l_rec
    );

    -- fail main job
    UPDATE cort_jobs
       SET status          = 'FAILED',
           output          = l_rec.output,
           run_time        = systimestamp - job_time,
           error_code      = l_rec.error_code,
           error_message   = l_rec.error_message,
           error_backtrace = l_rec.error_backtrace,
           call_stack      = l_rec.call_stack,
           error_stack     = l_rec.error_stack
     WHERE sid = in_rec.sid
       AND object_name = in_rec.object_name
       AND object_owner = in_rec.object_owner
       AND status <> 'FAILED';

    -- fail all threads
    UPDATE cort_jobs
       SET status          = 'FAILED',
           run_time        = systimestamp - job_time,
           error_code      = l_rec.error_code,
           error_message   = l_rec.error_message,
           error_backtrace = l_rec.error_backtrace,
           call_stack      = l_rec.call_stack,
           error_stack     = l_rec.error_stack
     WHERE sid like in_rec.sid||'%'
       AND object_name = in_rec.object_name
       AND object_owner = in_rec.object_owner
       and action = 'THREADING'
       AND status not in ('FAILED','CANCELLED');

    COMMIT;
  END fail_job;

  -- Update info about parent object for given job
  PROCEDURE update_parent_obejct(
    in_rec                 IN cort_jobs%ROWTYPE,
    in_parent_object_type  IN VARCHAR2,
    in_parent_object_owner IN VARCHAR2,
    in_parent_object_name  IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_jobs
       SET parent_object_type = in_parent_object_type,
           parent_object_owner = in_parent_object_owner,
           parent_object_name = in_parent_object_name
     WHERE sid = in_rec.sid
       AND object_name = in_rec.object_name
       AND object_owner = in_rec.object_owner;

    COMMIT;
  END update_parent_obejct;

  -- Update object_params for given job
  PROCEDURE update_object_params(
    in_rec           IN cort_jobs%ROWTYPE,
    in_object_params IN CLOB
  )
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_jobs
       SET object_params = in_object_params
     WHERE sid = in_rec.sid
       AND object_name = in_rec.object_name
       AND object_owner = in_rec.object_owner;

    COMMIT;
  END update_object_params;


END cort_job_pkg;
/
