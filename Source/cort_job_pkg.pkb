CREATE OR REPLACE PACKAGE BODY cort_job_pkg
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
    in_job_id        IN TIMESTAMP
  )
  RETURN cort_jobs%ROWTYPE
  AS
    l_rec              cort_jobs%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE job_id = in_job_id;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        cort_exec_pkg.debug('Record with job_id = '||TO_CHAR(in_job_id, 'yyyy-mm-dd hh24:mi:ss.ff6')||' not found in cort_jobs');
        RAISE;
    END;
    RETURN l_rec;
  END get_job_rec;


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
         AND decode(status,'RUNNING','R','PENDING','P','REGISTERED','G',TO_CHAR(job_id, 'YYYYMMDDHH24MISSFF6')) = 'P'
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
         AND decode(status,'RUNNING','R','PENDING','P','REGISTERED','G',job_id) IN ('R', 'P', 'G');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    RETURN l_rec;
  END find_active_job;

  FUNCTION is_object_locked(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_lock_type    IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
  PRAGMA autonomous_transaction;
    l_rec                  cort_job_control%ROWTYPE;
    e_resource_busy        EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_job_control
       WHERE object_owner = in_object_owner
         AND object_name = in_object_name
         AND lock_type = in_lock_type
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
  END is_object_locked;

  -- Enquire lock for job
  PROCEDURE lock_object(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_lock_type    IN VARCHAR2
  )
  AS
    l_rec                  cort_job_control%ROWTYPE;
    l_cnt                  PLS_INTEGER;
    e_resource_busy        EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_job_control
       WHERE object_owner = in_object_owner
         AND object_name = in_object_name
         AND lock_type = in_lock_type
         FOR UPDATE NOWAIT;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
      WHEN e_resource_busy THEN
        cort_exec_pkg.raise_error('Job already running for object  "'||in_object_owner||'"."'||in_object_name||'"');
    END;
    IF l_cnt = 0 THEN
      BEGIN
        INSERT INTO cort_job_control(object_owner, object_name, lock_type)
        VALUES(in_object_owner, in_object_name, in_lock_type);
      EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
          cort_exec_pkg.raise_error('Job already running for object  "'||in_object_owner||'"."'||in_object_name||'"');
      END;  
      COMMIT;
      BEGIN
        SELECT *
          INTO l_rec
          FROM cort_job_control
         WHERE object_owner = in_object_owner
           AND object_name = in_object_name
           AND lock_type = in_lock_type
           FOR UPDATE NOWAIT;
      EXCEPTION
        WHEN e_resource_busy THEN
          cort_exec_pkg.raise_error('Job  already running for object  "'||in_object_owner||'"."'||in_object_name||'"');
      END;
    END IF;    
  END lock_object;
  
  PROCEDURE created_job_record(io_rec IN OUT NOCOPY cort_jobs%ROWTYPE)
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    INSERT INTO cort_jobs
    VALUES io_rec;

    COMMIT;
  END created_job_record;

  PROCEDURE update_job_record(in_rec IN cort_jobs%ROWTYPE)
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_jobs
       SET row = in_rec
     WHERE job_id = in_rec.job_id;

    COMMIT;
  END update_job_record;


  -- add record for given job.
  FUNCTION register_job(
    in_action         IN VARCHAR2,
    in_object_type    IN VARCHAR2,
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_sql            IN CLOB,
    in_current_schema IN VARCHAR2,
    in_params_rec     IN cort_params_pkg.gt_run_params_rec,
    in_new_name       IN VARCHAR2 DEFAULT NULL
  )
  RETURN cort_jobs%ROWTYPE
  AS
    l_cnt                 PLS_INTEGER;
    l_rec                 cort_jobs%ROWTYPE;
    l_new_rec             cort_jobs%ROWTYPE;
    l_time                TIMESTAMP(3) := SYSTIMESTAMP;
  BEGIN
    l_new_rec.sid := dbms_session.unique_session_id;
    l_new_rec.job_id := l_time + TO_DSINTERVAL('PT0.'||TO_CHAR(cort_job_seq.nextval, 'fm000009')||'S');
    l_new_rec.job_owner := user;
    l_new_rec.job_name := gc_cort_job_name||l_new_rec.sid;
    l_new_rec.action := in_action;
    l_new_rec.status := 'REGISTERED';
    l_new_rec.object_type := in_object_type;
    l_new_rec.object_owner := in_object_owner;
    l_new_rec.object_name := in_object_name;
    l_new_rec.sql_text := in_sql;
    l_new_rec.new_name := in_new_name;
    l_new_rec.current_schema := in_current_schema;
    l_new_rec.run_params := cort_params_pkg.write_to_xml(in_params_rec);
    l_new_rec.osuser := SYS_CONTEXT('USERENV','OS_USER');
    l_new_rec.machine := SYS_CONTEXT('USERENV','HOST');
    l_new_rec.terminal := SYS_CONTEXT('USERENV','TERMINAL');
    l_new_rec.module := SYS_CONTEXT('USERENV','MODULE');

    cort_log_pkg.debug('Registering job "'||user||'"."'||l_new_rec.job_name||'"');
    
    -- obtain CONTROL SESSION lock on object
    lock_object(
      in_object_owner   => in_object_owner,
      in_object_name    => in_object_name, 
      in_lock_type      => 'CONTROL SESSION'
    );

    -- Check that there is no RUNNING JOB for another object in currency session
    l_cnt := 0;
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE decode(status,'RUNNING',sid,'REGISTERED',sid) = l_new_rec.sid;
      l_cnt := 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;

    IF l_cnt = 1 THEN
      IF (l_rec.status = 'RUNNING' AND is_object_locked(l_rec.object_owner,l_rec.object_name,'JOB SESSION')) THEN
        -- job is still running
        ROLLBACK;
        cort_exec_pkg.raise_error('There is a job attached to this session running in the background');
      ELSE
        cancel_job(l_rec);
      END IF;
    END IF;

    -- Check that there is no RUNNING job for current object in another session
    l_rec := find_active_job(
               in_object_name  => in_object_name,
               in_object_owner => in_object_owner
             );

    IF l_rec.job_id IS NOT NULL THEN
      -- Check that found sessions are stil alive
      IF ((l_rec.status = 'RUNNING' AND is_object_locked(l_rec.object_owner,l_rec.object_name,'JOB SESSION')) OR 
          (l_rec.status = 'REGISTERED' AND is_object_locked(l_rec.object_owner,l_rec.object_name,'CONTROL SESSION'))) AND 
          l_rec.sid <> l_new_rec.sid
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
        cancel_job(l_rec);
      END IF;
    END IF;

    created_job_record(l_new_rec);

    RETURN l_new_rec;

  END register_job;

  -- Return running job record and assign it to current session (set status = RUNNING)
  FUNCTION run_job(
    in_job_id        IN TIMESTAMP
  )
  RETURN cort_jobs%ROWTYPE
  AS
    l_rec cort_jobs%ROWTYPE;
  BEGIN
    SELECT *
      INTO l_rec 
      FROM cort_jobs
     WHERE job_id = in_job_id; 
     
    l_rec.job_id := in_job_id;
    l_rec.status := 'RUNNING';
    l_rec.start_time := SYSTIMESTAMP;

    lock_object(
      in_object_owner   => l_rec.object_owner,
      in_object_name    => l_rec.object_name, 
      in_lock_type      => 'JOB SESSION'
    );
    
    update_job_record(l_rec);

    RETURN l_rec;
  END run_job;


  -- Return running job record and assign it to current session (set status = RUNNING)
  FUNCTION resume_job(
    in_job_rec        IN cort_jobs%ROWTYPE
  )
  RETURN cort_jobs%ROWTYPE
  AS
    l_rec cort_jobs%ROWTYPE;
  BEGIN
    l_rec := in_job_rec;     
    l_rec.status := 'REGISTERED';
    l_rec.start_time := SYSTIMESTAMP;
    l_rec.end_time := NULL;
    l_rec.run_time := NULL;

    lock_object(
      in_object_owner   => l_rec.object_owner,
      in_object_name    => l_rec.object_name, 
      in_lock_type      => 'CONTROL SESSION'
    );
    
    update_job_record(l_rec);

    RETURN l_rec;
  END resume_job;



  PROCEDURE write_output(
    io_rec IN OUT NOCOPY cort_jobs%ROWTYPE
  )
  AS
    l_log_type_arr  arrays.gt_str_arr;
    l_text_arr      arrays.gt_lstr_arr;
    l_details_arr   arrays.gt_clob_arr;
  BEGIN
    SELECT log_type, text, details
      BULK COLLECT
      INTO l_log_type_arr, l_text_arr, l_details_arr
      FROM cort_log
     WHERE job_id = io_rec.job_id
       AND echo_flag = 'Y'
       AND details IS NOT NULL
       AND log_time >= io_rec.start_time
     ORDER BY log_time;
    FOR i IN 1..l_details_arr.COUNT LOOP
      IF l_log_type_arr(i) = 'WARNING' THEN
        io_rec.output := io_rec.output||'WARNING - '||l_text_arr(i);
      ELSE
        io_rec.output := io_rec.output||l_details_arr(i);
        IF l_log_type_arr(i) IN ('EXECUTE', 'TEST', 'ECHO') THEN 
          io_rec.output := io_rec.output||CASE WHEN substr(l_details_arr(i),-1) = ';' THEN CHR(13)||'/' ELSE ';' END;
        END IF;
      END IF;
      io_rec.output := io_rec.output||chr(13);
    END LOOP;

    SELECT revert_ddl||';'
      BULK COLLECT
      INTO l_text_arr
      FROM cort_log
     WHERE job_id = io_rec.job_id
       AND echo_flag = 'Y'
       AND revert_ddl IS NOT NULL
     ORDER BY log_time DESC;

    IF l_text_arr.COUNT > 0 THEN
      io_rec.output := io_rec.output||'== TEST REVERT DDL =='||chr(13);
      FOR i IN 1..l_text_arr.COUNT LOOP
        io_rec.output := io_rec.output||l_text_arr(i)||chr(13);
      END LOOP;
      io_rec.output := io_rec.output||'== END OF REVERT DDL =='||chr(13);
    END IF;  
     

  END write_output;


  -- Finish job
  PROCEDURE complete_job(
    in_rec IN cort_jobs%ROWTYPE
  )
  AS
    l_rec    cort_jobs%ROWTYPE;
  BEGIN
    IF in_rec.status IN ('RUNNING','PENDING') THEN
      l_rec := in_rec;
      l_rec.status := 'COMPLETED';
      l_rec.end_time := SYSTIMESTAMP;
      l_rec.run_time := SYSTIMESTAMP - l_rec.start_time;

      write_output(l_rec);

      update_job_record(l_rec);
    END IF;
  END complete_job;

  -- Set job to PENDING status
  PROCEDURE suspend_job(
    in_rec IN cort_jobs%ROWTYPE
  )
  AS
    l_rec    cort_jobs%ROWTYPE;
  BEGIN
    IF in_rec.status = 'RUNNING' THEN
      l_rec := in_rec;
      l_rec.status := 'PENDING';
      l_rec.end_time := SYSTIMESTAMP;
      l_rec.run_time := SYSTIMESTAMP - l_rec.start_time;

      write_output(l_rec);

      update_job_record(l_rec);
    END IF;
  END suspend_job;

  -- Cancel running job
  PROCEDURE cancel_job(
    in_rec             IN cort_jobs%ROWTYPE,
    in_error_message   IN VARCHAR2 DEFAULT NULL
  )
  AS
    l_rec    cort_jobs%ROWTYPE;
  BEGIN
    IF in_rec.status in ('RUNNING','REGISTERED','PENDING') THEN
      l_rec := in_rec;
      l_rec.error_message   := in_error_message;
      l_rec.error_code      := sqlcode;
      l_rec.error_backtrace := dbms_utility.format_error_backtrace;
      l_rec.call_stack      := dbms_utility.format_call_stack;
      l_rec.error_stack     := cort_log_pkg.get_error_stack;
      l_rec.status          := 'CANCELED';
      l_rec.end_time        := SYSTIMESTAMP;
      l_rec.run_time        := SYSTIMESTAMP - l_rec.start_time;

      write_output(l_rec);

      update_job_record(l_rec);

/*
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
*/
    END IF;
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
    IF in_rec.status in ('RUNNING','REGISTERED') THEN
      l_rec := in_rec;
      l_rec.error_message   := in_error_message;
      l_rec.error_code      := sqlcode;
      l_rec.error_backtrace := dbms_utility.format_error_backtrace;
      l_rec.call_stack      := dbms_utility.format_call_stack;
      l_rec.error_stack     := cort_log_pkg.get_error_stack;
      l_rec.status          := 'FAILED';
      l_rec.end_time        := SYSTIMESTAMP;
      l_rec.run_time        := SYSTIMESTAMP - l_rec.start_time;

      write_output(l_rec);
 
      update_job_record(l_rec);


/*    -- fail all threads
    UPDATE cort_jobs
       SET status          = 'FAILED',
           end_time        = SYSTIMESTAMP,
           run_time        = SYSTIMESTAMP - start_time,
           error_code      = l_rec.error_code,
           error_message   = l_rec.error_message,
           error_backtrace = l_rec.error_backtrace,
           call_stack      = l_rec.call_stack,
           error_stack     = l_rec.error_stack
     WHERE sid like in_rec.sid||'%'
       AND object_name = in_rec.object_name
       AND object_owner = in_rec.object_owner
       and action = 'THREADING'
       AND status not in ('FAILED','CANCELLED');*/

    END IF;  
  END fail_job;


  -- Update change_params for given job
  PROCEDURE update_change_params(
    in_rec           IN cort_jobs%ROWTYPE,
    in_change_params IN CLOB
  )
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_jobs
       SET change_params = in_change_params
     WHERE sid = in_rec.sid
       AND object_name = in_rec.object_name
       AND object_owner = in_rec.object_owner;

    COMMIT;
  END update_change_params;

END cort_job_pkg;
/
