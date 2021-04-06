CREATE OR REPLACE PACKAGE cort_thread_job_pkg 
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
  18.00   | Rustam Kafarov    | Added API for creating threading jobs 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  TYPE gt_thread_rec IS RECORD (
    thread_id           NUMBER,
    thread_rowid        ROWID,
    sql_text            CLOB
  );
  
  -- bulk register threads
  PROCEDURE register_threads(
    in_stmt_arr     IN arrays.gt_clob_arr
  );

  FUNCTION get_next_thread(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_job_name     IN VARCHAR2
  )
  RETURN gt_thread_rec;
  
  PROCEDURE complete_sql(
    in_rowid    IN ROWID
  );  

  PROCEDURE fail_sql(
    in_rowid     IN ROWID,
    in_error_msg IN VARCHAR2
  );

  FUNCTION get_max_job_number
  RETURN NUMBER;
  
  FUNCTION get_thread_count(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN NUMBER;

  PROCEDURE reset_failed_threads(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  );

  PROCEDURE check_failed_threads(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  );

END cort_thread_job_pkg;
/