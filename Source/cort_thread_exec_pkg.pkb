CREATE OR REPLACE PACKAGE BODY cort_thread_exec_pkg 
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
  Description: event execution API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  18.00   | Rustam Kafarov    | Added package to execute actions in threading mode 
  19.00   | Rustam Kafarov    | Revised parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_thread_id           PLS_INTEGER := 0;
  g_thread_stmt_arr     arrays.gt_clob_arr;
  
  FUNCTION get_threading_value
  RETURN NUMBER
  AS
    l_value VARCHAR2(50);
  BEGIN
    l_value := cort_exec_pkg.g_params.threading.get_value;
    RETURN CASE 
             WHEN UPPER(l_value) = 'AUTO' THEN cort_thread_exec_pkg.gc_auto_threading 
             WHEN UPPER(l_value) = 'MAX' THEN cort_thread_exec_pkg.gc_max_threading
             WHEN UPPER(l_value) = 'NONE' THEN cort_thread_exec_pkg.gc_no_threading
             ELSE TO_NUMBER(l_value)
           END;
  END get_threading_value;
  
  
  PROCEDURE make_sql_threadable(
    io_sql IN OUT NOCOPY CLOB
  )  
  AS
    l_prefix     VARCHAR2(100);
  BEGIN
    IF get_threading_value > 1 THEN
      g_thread_id := g_thread_id + 1;
      l_prefix := '/*thread_id='||to_char(g_thread_id)||'*/';
      io_sql := l_prefix || io_sql;
    END IF;    
  END make_sql_threadable;

  PROCEDURE add_threading_sql(
    io_sql_arr          IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    IF get_threading_value > 1 THEN
      io_sql_arr(io_sql_arr.COUNT+1) := 
      'begin cort_thread_exec_pkg.prepare_for_threading; end;';
      io_sql_arr(io_sql_arr.COUNT+1) := 
      'begin cort_thread_exec_pkg.start_threading(in_threading => '||to_char(get_threading_value)||'); end;';
    END IF;
  END add_threading_sql;

  PROCEDURE find_threading_statements(
    in_frwd_alter_stmt_arr  IN arrays.gt_clob_arr,  -- forward alter statements
    in_rlbk_alter_stmt_arr  IN arrays.gt_clob_arr,  -- rollback alter statements
    out_frwd_alter_stmt_arr OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    out_rlbk_alter_stmt_arr OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_indx                PLS_INTEGER;
  BEGIN
    g_thread_stmt_arr.DELETE;
    l_indx := in_frwd_alter_stmt_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF substr(in_frwd_alter_stmt_arr(l_indx), 1, 50) LIKE '/*thread_id=%*/%' THEN
        cort_exec_pkg.debug('threading sql: '||in_frwd_alter_stmt_arr(l_indx), in_frwd_alter_stmt_arr(l_indx));
        g_thread_stmt_arr(g_thread_stmt_arr.COUNT+1) := in_frwd_alter_stmt_arr(l_indx);
      ELSE
        out_frwd_alter_stmt_arr(l_indx) := in_frwd_alter_stmt_arr(l_indx);
        out_rlbk_alter_stmt_arr(l_indx) := in_rlbk_alter_stmt_arr(l_indx);
      END IF;
      l_indx := in_frwd_alter_stmt_arr.NEXT(l_indx);
    END LOOP;
  END find_threading_statements;
  
  PROCEDURE prepare_for_threading
  AS
  BEGIN
    IF NOT cort_exec_pkg.g_params.test.get_bool_value AND g_thread_stmt_arr.COUNT > 0 THEN 
      cort_thread_job_pkg.register_threads(in_stmt_arr => g_thread_stmt_arr);
      g_thread_stmt_arr.DELETE;
    END IF;
  END prepare_for_threading;  
  

  PROCEDURE int_process_thread(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec           cort_thread_job_pkg.gt_thread_rec;
  BEGIN
    -- start processing
    LOOP
      cort_thread_job_pkg.check_failed_threads(
        in_object_owner => in_job_rec.object_owner,
        in_object_name  => in_job_rec.object_name
      );
      
      --pick up next available thread and update status to 'RUNNING'
      l_rec := cort_thread_job_pkg.get_next_thread(
                 in_object_owner => in_job_rec.object_owner,
                 in_object_name  => in_job_rec.object_name,
                 in_job_name     => in_job_rec.job_name
               );
      -- exit if no thread was available         
      EXIT WHEN l_rec.thread_id IS NULL;

      BEGIN
        cort_exec_pkg.execute_immediate(
          in_sql => l_rec.sql_text
        );
      EXCEPTION
        WHEN OTHERS THEN
          cort_log_pkg.error('Failed sql in thread '||l_rec.thread_id, l_rec.sql_text); 
          cort_thread_job_pkg.fail_sql(
            in_rowid     => l_rec.thread_rowid,
            in_error_msg => sqlerrm
          );
          ROLLBACK;-- release lock
          RAISE;
      END; 
      cort_thread_job_pkg.complete_sql(
        in_rowid    => l_rec.thread_rowid 
      );
      
    END LOOP;

    COMMIT;
  END int_process_thread;

  -- get number of thread we can run
  FUNCTION get_thread_number(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_threading    IN PLS_INTEGER
  )
  RETURN PLS_INTEGER
  AS
    l_max_job_cnt         PLS_INTEGER;
    l_running_jobs_cnt    PLS_INTEGER;
    l_thread_cnt          PLS_INTEGER;
    l_result              PLS_INTEGER;
  BEGIN
    -- get max number of available job in the system
    l_max_job_cnt := LEAST(cort_thread_job_pkg.get_max_job_number, 64);
    -- get number of thread for table
    l_thread_cnt := cort_thread_job_pkg.get_thread_count(
                      in_object_name    => in_object_name,
                      in_object_owner   => in_object_owner
                    );
    -- get number of currently running jobs
    SELECT COUNT(*) 
      INTO l_running_jobs_cnt
      FROM all_scheduler_running_jobs;
    CASE in_threading
    WHEN gc_no_threading THEN 
      l_result := gc_no_threading; 
    WHEN gc_auto_threading THEN
      l_result := LEAST(TRUNC(l_thread_cnt/5), l_max_job_cnt - l_running_jobs_cnt - 3);   
    WHEN gc_max_threading THEN
      l_result := LEAST(l_thread_cnt, l_max_job_cnt - l_running_jobs_cnt);   
    ELSE
      l_result := LEAST(in_threading-1, l_max_job_cnt - l_running_jobs_cnt);
    END CASE;
    RETURN l_result;   
  END get_thread_number;

  PROCEDURE start_threading(
    in_threading IN PLS_INTEGER
  )
  AS
    TYPE t_cort_jobs_arr IS TABLE OF cort_jobs%ROWTYPE INDEX BY PLS_INTEGER;
    l_rec_arr       t_cort_jobs_arr;
    l_thread_number NUMBER;
    l_job_rec       cort_jobs%ROWTYPE;
  BEGIN
    cort_exec_pkg.start_timer;
    
    l_job_rec := cort_exec_pkg.get_job_rec;

    -- reset all failed threads from previous run (in case of rerun after fail)
    cort_thread_job_pkg.reset_failed_threads(
      in_object_owner => l_job_rec.object_owner,
      in_object_name  => l_job_rec.object_name
    );

    l_thread_number := get_thread_number(
                         in_object_name    => l_job_rec.object_name,
                         in_object_owner   => l_job_rec.object_owner,
                         in_threading      => in_threading
                       );

    FOR i IN 1..l_thread_number LOOP
      l_rec_arr(i) := cort_job_pkg.register_job(
                        in_sid            => l_job_rec.sid||'#'||TO_CHAR(i,'fm0XXX'),
                        in_job_owner      => l_job_rec.job_owner,
                        in_action         => 'THREADING',
                        in_object_type    => l_job_rec.object_type,
                        in_object_name    => l_job_rec.object_name,
                        in_object_owner   => l_job_rec.object_owner,
                        in_sql            => 'THREADING',
                        in_current_schema => l_job_rec.current_schema,
                        in_params_rec     => cort_exec_pkg.g_params
                      );
      -- set locks
      cort_job_pkg.start_job(l_rec_arr(i));

      cort_event_exec_pkg.run_action(
        in_rec   => l_rec_arr(i),
        in_async => true,
        in_wait  => false
      );
    END LOOP;

    -- start waiting
    -- instead of just waiting main session works as thread as well 
    int_process_thread(
      in_job_rec       => l_job_rec
    );

    -- check if there are no running threads left
    FOR i IN 1..l_thread_number LOOP
      cort_event_exec_pkg.wait_for_job_end(l_rec_arr(i));
      l_rec_arr(i) := cort_job_pkg.get_job_rec(
                        in_sid          => l_rec_arr(i).sid,
                        in_object_name  => l_rec_arr(i).object_name,
                        in_object_owner => l_rec_arr(i).object_owner
                      );  
      IF l_rec_arr(i).status = 'FAILED' THEN
        cort_exec_pkg.raise_error('Failed thread job: '||l_rec_arr(i).error_message);
      END IF;
    END LOOP;

    cort_exec_pkg.stop_timer;

    -- release locks;
    COMMIT;

  END start_threading;
  
  PROCEDURE process_thread(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
  BEGIN
    cort_params_pkg.read_from_xml(XMLType(in_job_rec.session_params), cort_exec_pkg.g_params);
    
    IF cort_exec_pkg.g_params.parallel.get_num_value > 1 THEN
      cort_exec_pkg.execute_immediate('ALTER SESSION ENABLE PARALLEL DML');
    END IF;  

    IF in_job_rec.current_schema <> in_job_rec.job_owner THEN
      cort_exec_pkg.execute_immediate('ALTER SESSION SET CURRENT_SCHEMA = "'||in_job_rec.current_schema||'"', TRUE);
    END IF;  
    
    int_process_thread(
      in_job_rec  => in_job_rec
    );
    
    -- release lock
    COMMIT;    
  END process_thread;
  
 
  

END cort_thread_exec_pkg;
/