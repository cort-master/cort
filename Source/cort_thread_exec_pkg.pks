CREATE OR REPLACE PACKAGE cort_thread_exec_pkg 
AUTHID CURRENT_USER
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

  gc_no_threading   CONSTANT PLS_INTEGER := 0;
  gc_auto_threading CONSTANT PLS_INTEGER := 32766;
  gc_max_threading  CONSTANT PLS_INTEGER := 32767;

  -- reset all global variables;
  PROCEDURE reset_threading;

  -- init threading values
  PROCEDURE init_threading(
    in_data_source_rec  IN cort_exec_pkg.gt_data_source_rec,
    in_source_table_rec IN cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec
  );

  -- returns TRUE if submitted changes could be run in threads
  FUNCTION is_threadable
  RETURN BOOLEAN;

  -- get number of thread we can run
  FUNCTION get_thread_number
  RETURN PLS_INTEGER;

  -- returns value for parallel hint for DML taking into account threading numbers 
  FUNCTION get_dml_parallel
  RETURN VARCHAR2;
  
  -- Threading API

  -- execute sql changes in threads
  PROCEDURE apply_change(
    io_change_arr   IN OUT NOCOPY cort_exec_pkg.gt_change_arr
  );

  -- Generic execution which is called from job
  PROCEDURE execute_thread(
    in_job_id IN TIMESTAMP
  );

END cort_thread_exec_pkg;
/