CREATE OR REPLACE PACKAGE BODY cort_thread_job_pkg 
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
  Description: thread job execution API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  18.00   | Rustam Kafarov    | Added API for creating threading jobs 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  PROCEDURE register_threads(
    in_stmt_arr  IN arrays.gt_clob_arr
  )
  AS
  PRAGMA autonomous_transaction;
    l_job_rec   cort_jobs%ROWTYPE;
  BEGIN
    l_job_rec := cort_exec_pkg.get_job_rec;
    
    DELETE FROM cort_thread_sql
     WHERE object_owner = l_job_rec.object_owner
       AND object_name = l_job_rec.object_name;
       
    FORALL i IN 1..in_stmt_arr.COUNT
      INSERT  
        INTO cort_thread_sql
             (object_owner, object_name, thread_id, status, sql_text) 
      VALUES (l_job_rec.object_owner,
             l_job_rec.object_name,
             TO_NUMBER(REGEXP_SUBSTR(in_stmt_arr(i), '^\/\*thread_id=([0-9]+)\*\/', 1, 1, null, 1)),
             'REGISTERED',
             in_stmt_arr(i));  
    
    COMMIT;
  END register_threads;
  
  FUNCTION get_next_thread(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_job_name     IN VARCHAR2
  )
  RETURN gt_thread_rec
  AS
    l_thread_rec    gt_thread_rec;

    CURSOR c_next_thread IS 
    SELECT a.thread_id, a.rowid as thread_rowid, sql_text
      FROM cort_thread_sql a
     WHERE object_owner = in_object_owner
       AND object_name = in_object_name 
       AND status = 'REGISTERED'
       FOR UPDATE SKIP LOCKED
     ORDER BY thread_id; 
  BEGIN
    BEGIN
      OPEN c_next_thread;
      FETCH c_next_thread INTO l_thread_rec.thread_id, l_thread_rec.thread_rowid, l_thread_rec.sql_text;
      CLOSE c_next_thread;
    EXCEPTION  
      WHEN NO_DATA_FOUND THEN
        IF c_next_thread%isopen THEN 
          CLOSE c_next_thread;
        END IF;  
        RETURN NULL;
    END;
    
    UPDATE cort_thread_sql
       SET status = 'RUNNING',
           job_name = in_job_name,
           start_time = systimestamp
     WHERE rowid = l_thread_rec.thread_rowid;
    COMMIT; 

    RETURN l_thread_rec;   
  END get_next_thread;
  

  PROCEDURE complete_sql(
    in_rowid    IN ROWID
  )
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_thread_sql
       SET status = 'COMPLETED',
           end_time = systimestamp
     WHERE rowid = in_rowid;
    COMMIT; 
  END complete_sql;

  PROCEDURE fail_sql(
    in_rowid     IN ROWID,
    in_error_msg IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_thread_sql
       SET status = 'FAILED',
           end_time = systimestamp,
           error_msg = in_error_msg
     WHERE rowid = in_rowid;
    COMMIT; 
  END fail_sql;

  FUNCTION get_max_job_number
  RETURN NUMBER
  AS
    l_intval BINARY_INTEGER;
    l_strval VARCHAR2(4000);
    l_retval BINARY_INTEGER;
   BEGIN  
      l_retval := dbms_utility.get_parameter_value(
                    parnam => 'job_queue_processes',
                    intval => l_intval,
                    strval => l_strval
                  );
     RETURN l_intval;  
   END get_max_job_number;

  FUNCTION get_thread_count(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN NUMBER
  AS
    l_result NUMBER;
  BEGIN
    SELECT COUNT(*)
      INTO l_result 
      FROM cort_thread_sql
     WHERE object_owner = in_object_owner
       AND object_name = in_object_name
       AND status <> 'COMPLETED';
    RETURN l_result;  
  END get_thread_count;
  
  PROCEDURE reset_failed_threads(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )  
  AS
  BEGIN
    UPDATE cort_thread_sql
       SET status = 'REGISTERED',
           job_name = NULL,
           start_time = NULL,
           end_time = NULL,
           error_msg = NULL 
     WHERE object_name = in_object_name
       AND object_owner = in_object_owner
       AND status IN ('FAILED', 'CANCELED');
    COMMIT;   
  END reset_failed_threads;

  PROCEDURE check_failed_threads(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  AS
    l_cnt            NUMBER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM cort_thread_sql
     WHERE object_name = in_object_name
       AND object_owner = in_object_owner
       AND status IN ('FAILED', 'CANCELED');
    IF l_cnt > 0 THEN
      cort_exec_pkg.raise_error('Terminated threading due to fail(s)');
    END IF;
  END check_failed_threads;

END cort_thread_job_pkg;
/

