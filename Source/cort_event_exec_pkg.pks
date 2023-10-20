CREATE OR REPLACE PACKAGE cort_event_exec_pkg 
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
  17.00   | Rustam Kafarov    | Added package to execute actions on events with current user privileges 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- returns object last modification date
  FUNCTION get_object_last_ddl_time(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN DATE;

  FUNCTION is_object_modified(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_sql_text     IN CLOB
  )
  RETURN BOOLEAN;

  -- Run infinit loop until dbms_scheduler job is done
  PROCEDURE wait_for_job_end(
    in_rec IN cort_jobs%ROWTYPE
  ); 

  -- run action synchronously or asynchronously  
  PROCEDURE run_action(
    in_rec           IN cort_jobs%rowtype,
    in_async         IN BOOLEAN, 
    in_wait          IN BOOLEAN DEFAULT TRUE
  );

  -- process event action via job or directly
  PROCEDURE process_event(
    in_async         IN BOOLEAN,
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB,
    in_new_name      IN VARCHAR2 DEFAULT NULL,
    in_params_rec    IN cort_params_pkg.gt_run_params_rec DEFAULT cort_session_pkg.get_params
  );

  -- resume pending process 
  PROCEDURE resume_process(
    in_rec IN cort_jobs%ROWTYPE
  );

  -- Run create or replace for non recreatable objects
  PROCEDURE instead_of_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  );

  -- Register metadata of recreatable objects
  PROCEDURE before_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  );
  
  -- Test CORT changes and add results into PLAN_TABLE$
  PROCEDURE explain_cort_sql(
    in_statement_id  IN VARCHAR2,
    in_plan_id       IN NUMBER,
    in_timestamp     IN DATE,
    out_sql          OUT NOCOPY CLOB,
    out_last_id      OUT NUMBER
  );
 
  -- Prevent and DDL for object changing by CORT
  PROCEDURE lock_object(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    out_lock_error   OUT VARCHAR2
  );

END cort_event_exec_pkg;
/