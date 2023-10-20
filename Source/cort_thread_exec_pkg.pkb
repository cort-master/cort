CREATE OR REPLACE PACKAGE BODY cort_thread_exec_pkg 
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
  18.00   | Rustam Kafarov    | Added package to execute actions in threading mode 
  19.00   | Rustam Kafarov    | Revised parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  
  gc_thread_job_name       CONSTANT VARCHAR2(30) := 'thread#';
  
  -- record for holding run params for current thread
  g_run_params             cort_params_pkg.gt_run_params_rec;
  g_partitions_cnt         PLS_INTEGER;
--  g_target_partitions_cnt  PLS_INTEGER;
  g_max_available_threads  PLS_INTEGER;
  g_thread_number          PLS_INTEGER;
 
  FUNCTION get_threading_value
  RETURN NUMBER
  AS
    l_value VARCHAR2(50);
  BEGIN
    l_value := cort_exec_pkg.g_run_params.threading.get_value;
    RETURN CASE 
             WHEN UPPER(l_value) = 'AUTO' THEN gc_auto_threading 
             WHEN UPPER(l_value) = 'MAX' THEN gc_max_threading
             WHEN UPPER(l_value) = 'NONE' THEN gc_no_threading
             ELSE TO_NUMBER(l_value)
           END;
  END get_threading_value;
  
  PROCEDURE reset_threading
  AS
  BEGIN
    g_partitions_cnt := NULL;
--    g_target_partitions_cnt := NULL;
    g_thread_number := NULL;
    g_max_available_threads := NULL;   
  END reset_threading;

  PROCEDURE init_threading(
    in_data_source_rec  IN cort_exec_pkg.gt_data_source_rec,
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_source_partition_rec    cort_exec_pkg.gt_partition_rec;
    l_source_subpartition_rec cort_exec_pkg.gt_partition_rec;
    l_running_jobs_cnt        PLS_INTEGER;
  BEGIN
    g_partitions_cnt := 0;

    IF in_source_table_rec.subpartitioning_type <> 'NONE' AND in_source_table_rec.subpartitioning_type = in_target_table_rec.subpartitioning_type THEN

      FOR i IN REVERSE 1..in_source_table_rec.partition_arr.COUNT LOOP
        l_source_partition_rec := in_source_table_rec.partition_arr(i);
        FOR j IN l_source_partition_rec.subpartition_from_indx..l_source_partition_rec.subpartition_to_indx LOOP
          l_source_subpartition_rec := in_source_table_rec.subpartition_arr(j);
          
          IF (l_source_partition_rec.matching_indx IS NOT NULL AND
              l_source_subpartition_rec.matching_indx IS NOT NULL AND
              in_data_source_rec.change_result = cort_comp_pkg.gc_result_part_exchange) OR
             l_source_partition_rec.is_partition_empty
          THEN
            -- move data
            NULL;
          ELSE
            g_partitions_cnt := g_partitions_cnt + 1;
          END IF;
        END LOOP;

      END LOOP;
  
    ELSIF in_source_table_rec.subpartitioning_type = 'NONE' AND in_source_table_rec.partitioned <> 'NO' THEN
    
      FOR i IN REVERSE 1..in_source_table_rec.partition_arr.COUNT LOOP
        l_source_partition_rec := in_source_table_rec.partition_arr(i);

        IF (l_source_partition_rec.matching_indx IS NOT NULL AND in_data_source_rec.change_result = cort_comp_pkg.gc_result_part_exchange) OR
            l_source_partition_rec.is_partition_empty
        THEN
          -- move data
          NULL;
        ELSE
          IF l_source_partition_rec.matching_indx = -1 THEN
            NULL; -- skip partition
          ELSE
            g_partitions_cnt := g_partitions_cnt + 1;
          END IF;   
            
        END IF;
      END LOOP;      
    
    END IF;      
    
--    g_target_partitions_cnt := in_target_table_rec.partition_arr.COUNT;
    
    -- get number of currently running jobs
    SELECT COUNT(*) 
      INTO l_running_jobs_cnt
      FROM all_scheduler_running_jobs
     WHERE (job_name like gc_thread_job_name||'%');
    
    g_max_available_threads := cort_params_pkg.gc_max_thread_number - l_running_jobs_cnt; 
     
    CASE get_threading_value
    WHEN gc_no_threading THEN 
      g_thread_number := gc_no_threading; 
    WHEN gc_auto_threading THEN
      g_thread_number := LEAST(TRUNC(g_partitions_cnt/5), g_max_available_threads - 3);   
    WHEN gc_max_threading THEN
      g_thread_number := LEAST(g_partitions_cnt, g_max_available_threads);   
    ELSE
      g_thread_number := LEAST(get_threading_value-1, g_max_available_threads);
    END CASE;
    
  END init_threading;
  
  FUNCTION is_threadable
  RETURN BOOLEAN
  AS
  BEGIN
    RETURN g_partitions_cnt > 1;
  END is_threadable;

  FUNCTION get_thread_number
  RETURN PLS_INTEGER
  AS
  BEGIN
    RETURN g_thread_number;   
  END get_thread_number;

  FUNCTION get_dml_parallel
  RETURN VARCHAR2
  AS
    l_parallel            PLS_INTEGER;
    l_result              VARCHAR2(100);
  BEGIN
    l_parallel := g_run_params.parallel.get_value;

    IF get_threading_value IN (gc_auto_threading, gc_max_threading) THEN
      IF g_thread_number > 0 AND g_thread_number < g_max_available_threads THEN
        l_parallel := ROUND(g_max_available_threads/g_thread_number);       
      END IF;
    END IF;  
    IF l_parallel > 1 THEN
      l_result := 'PARALLEL ('||l_parallel||')';
    END IF;  
    RETURN l_result;   
  END get_dml_parallel;


  FUNCTION get_pending_count(in_job_id IN TIMESTAMP)
  RETURN PLS_INTEGER
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt 
      FROM cort_sql
    WHERE job_id = in_job_id
      AND threadable = 'Y'
      AND status = 'REGISTERED';
    RETURN l_cnt;   
  END get_pending_count;
  
  FUNCTION get_running_group(in_job_id IN TIMESTAMP)
  RETURN PLS_INTEGER
  AS
    l_group_type cort_sql.group_type%TYPE;
  BEGIN
    SELECT MAX(REGEXP_SUBSTR(group_type, '[A-Z]+'))
      INTO l_group_type 
      FROM cort_sql
     WHERE job_id = in_job_id
       AND threadable = 'Y'
       AND status = 'RUNNING';
    
    IF l_group_type IS NULL THEN
      SELECT MIN(REGEXP_SUBSTR(group_type, '[A-Z]+')) KEEP(DENSE_RANK FIRST ORDER BY seq_num)
        INTO l_group_type
        FROM cort_sql
       WHERE job_id = in_job_id
         AND threadable = 'Y'
         AND status = 'REGISTERED';
    END IF;
    
    RETURN l_group_type;   
  END get_running_group;
  
  FUNCTION get_failed_job(in_job_id IN TIMESTAMP)
  RETURN cort_sql.job_id%TYPE
  AS
    l_job_id  cort_sql.job_id%TYPE;
  BEGIN    
    SELECT max(job_id)
      INTO l_job_id
      FROM cort_sql
     WHERE job_id = in_job_id
       AND threadable = 'Y'
       AND status IN ('FAILED', 'CANCELED');
    RETURN l_job_id;   
  END get_failed_job;
  
  FUNCTION get_next_thread(
    in_job_id     IN TIMESTAMP, 
    in_group_type IN VARCHAR2 
  )
  RETURN cort_exec_pkg.gt_change_rec
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    CURSOR c_next_thread IS 
    SELECT group_type, 
           change_sql, 
           revert_sql, 
           NULL as display_sql,
           status, 
           start_time, 
           end_time, 
           revert_start_time, 
           revert_end_time, 
           threadable, 
           error_msg,
           a.rowid as row_id
      FROM cort_sql a
     WHERE job_id = in_job_id
       AND threadable = 'Y'
       AND group_type = in_group_type
       AND status = 'REGISTERED'
       FOR UPDATE SKIP LOCKED
     ORDER BY seq_num;
    l_rec cort_exec_pkg.gt_change_rec; 
  BEGIN
    BEGIN
      OPEN c_next_thread;
      FETCH c_next_thread INTO l_rec;
      CLOSE c_next_thread;
    EXCEPTION  
      WHEN NO_DATA_FOUND THEN
        IF c_next_thread%isopen THEN 
          CLOSE c_next_thread;
        END IF;  
        RETURN NULL;
    END;
    
    l_rec.status := 'RUNNING';
    cort_aux_pkg.update_sql(l_rec);
    
    COMMIT; 
    
    RETURN l_rec;
  END get_next_thread;
    
  
  PROCEDURE process_thread(
    in_job_rec       IN cort_jobs%ROWTYPE
  )
  AS
    l_rec           cort_exec_pkg.gt_change_rec;
    l_group_type    cort_sql.group_type%TYPE;
    e_compiled_with_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_compiled_with_errors,-24344);
  BEGIN
    WHILE get_pending_count(in_job_rec.job_id) > 0 LOOP
      l_group_type := get_running_group(in_job_rec.job_id);
        
      EXIT WHEN l_group_type IS NULL;
        
      l_rec := get_next_thread(
                 in_job_id     => in_job_rec.job_id, 
                 in_group_type => l_group_type 
               );
      IF l_rec.status = 'RUNNING' THEN
        BEGIN
          cort_exec_pkg.execute_immediate(l_rec.change_sql);
        EXCEPTION
          -- if trigger created with errors then do not raise exception and carry on
          WHEN e_compiled_with_errors THEN
            NULL;
          WHEN OTHERS THEN
            l_rec := cort_exec_pkg.change_status(l_rec, 'FAILED');
            RAISE;
        END;
      END IF;
      IF get_failed_job(in_job_rec.job_id) IS NOT NULL THEN
        cort_exec_pkg.raise_error('Operation cancelled for job = '||TO_CHAR(get_failed_job(in_job_rec.job_id),'yyyy-mm-dd hh24:mi:ss.ff6'), -20900);
      END IF;
    END LOOP;
  END process_thread;


  PROCEDURE start_threading_jobs
  AS
    l_job_rec       cort_jobs%ROWTYPE;
  BEGIN
    IF NOT is_threadable THEN
      RETURN;
    END IF;
    
    cort_log_pkg.start_timer('THREADING');
    
    l_job_rec := cort_exec_pkg.get_job_rec;
    l_job_rec.action := 'THREADING';
 
    FOR i IN 1..g_thread_number LOOP
     --                     7                   2                    1   20
      l_job_rec.job_name := gc_thread_job_name||to_char(i, 'fm00')||'#'||to_char(l_job_rec.job_id, 'yyyymmddhh24missff6');
      
      cort_event_exec_pkg.run_action(
        in_rec   => l_job_rec,
        in_async => TRUE,
        in_wait  => FALSE
      );
    END LOOP;  
    cort_log_pkg.stop_timer('THREADING');
  END start_threading_jobs;
  
 
  PROCEDURE apply_change(
    io_change_arr   IN OUT NOCOPY cort_exec_pkg.gt_change_arr--,
    --in_act_on_error IN VARCHAR2 DEFAULT 'RAISE'  
  )
  AS
    l_job_rec cort_jobs%ROWTYPE;
  BEGIN
    IF is_threadable AND get_thread_number > 1 THEN
      l_job_rec := cort_exec_pkg.get_job_rec;
      start_threading_jobs;
      process_thread(l_job_rec);
      cort_aux_pkg.read_sql(io_change_arr);
    END IF;
  END apply_change;
  
  -- Generic execution which is called from job
  PROCEDURE execute_thread(
    in_job_id IN TIMESTAMP
  )
  AS
    l_job_rec cort_jobs%ROWTYPE;
  BEGIN
    l_job_rec := cort_job_pkg.get_job_rec(in_job_id);
    l_job_rec.status := 'RUNNING';
    l_job_rec.start_time := SYSTIMESTAMP;
    
    BEGIN
      cort_params_pkg.reset_run_params(g_run_params);
      cort_params_pkg.read_from_xml(g_run_params, XMLType(l_job_rec.run_params));

      IF cort_exec_pkg.g_run_params.parallel.get_num_value > 1 THEN
        cort_exec_pkg.execute_immediate('ALTER SESSION ENABLE PARALLEL DML');
      END IF;  

      IF l_job_rec.current_schema <> l_job_rec.job_owner THEN
        cort_exec_pkg.execute_immediate('ALTER SESSION SET CURRENT_SCHEMA = "'||l_job_rec.current_schema||'"', TRUE);
      END IF;  

      cort_exec_pkg.execute_immediate('ALTER SESSION SET SKIP_UNUSABLE_INDEXES = TRUE', TRUE);
      
      process_thread(l_job_rec);

    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.error('Failed cort_exec_pkg.execute_thread');
        RAISE;
    END;
    
  END execute_thread;
  
END cort_thread_exec_pkg;
/