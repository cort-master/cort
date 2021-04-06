CREATE OR REPLACE PACKAGE cort_thread_exec_pkg 
AUTHID CURRENT_USER
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

  gc_no_threading   CONSTANT PLS_INTEGER := 0;
  gc_auto_threading CONSTANT PLS_INTEGER := 32766;
  gc_max_threading  CONSTANT PLS_INTEGER := 32767;

  FUNCTION get_threading_value
  RETURN NUMBER;
  
  PROCEDURE make_sql_threadable(
    io_sql     IN OUT NOCOPY CLOB
  );  

  PROCEDURE add_threading_sql(
    io_sql_arr IN OUT NOCOPY arrays.gt_clob_arr
  );

  PROCEDURE find_threading_statements(
    in_frwd_alter_stmt_arr  IN arrays.gt_clob_arr,  -- forward alter statements
    in_rlbk_alter_stmt_arr  IN arrays.gt_clob_arr,  -- rollback alter statements
    out_frwd_alter_stmt_arr OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    out_rlbk_alter_stmt_arr OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  );
  
  -- Threading API
  PROCEDURE prepare_for_threading;

  PROCEDURE start_threading(
    in_threading IN PLS_INTEGER
  );

  PROCEDURE process_thread(
     in_job_rec       IN cort_jobs%ROWTYPE
  );  

END cort_thread_exec_pkg;
/