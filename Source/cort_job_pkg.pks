CREATE OR REPLACE PACKAGE cort_job_pkg 
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
  14.02   | Rustam Kafarov    | Added procedure for running job in same session
  15.00   | Rustam Kafarov    | Added support for manual execution
  17.00   | Rustam Kafarov    | Changed prefix for cort jobs. Added functions find_active_job, find_pending_job
  19.00   | Rustam Kafarov    | Revised parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  gc_cort_job_name       CONSTANT VARCHAR2(30) := 'cort_job#';

  -- Return job record 
  FUNCTION get_job_rec(
    in_job_id        IN TIMESTAMP
  )
  RETURN cort_jobs%ROWTYPE;

  -- Find pending job for given object
  FUNCTION find_pending_job(
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_jobs%ROWTYPE;

  -- Find running or pending job for given object
  FUNCTION find_active_job(
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_jobs%ROWTYPE;

  -- check if job session still keeps lock  
  FUNCTION is_object_locked(
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_lock_type    IN VARCHAR2
  )
  RETURN BOOLEAN;

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
  RETURN cort_jobs%ROWTYPE;

  -- Return REGISTERED job record and assign it to current session (set status = RUNNING)
  FUNCTION run_job(
    in_job_id        IN TIMESTAMP
  )
  RETURN cort_jobs%ROWTYPE;

  -- resume pending job
  FUNCTION resume_job(
    in_job_rec        IN cort_jobs%ROWTYPE
  )
  RETURN cort_jobs%ROWTYPE;

  -- Finish job
  PROCEDURE complete_job(
    in_rec IN cort_jobs%ROWTYPE
  );
  
  -- Set job to PENDING status
  PROCEDURE suspend_job(
    in_rec IN cort_jobs%ROWTYPE
  );

  -- Cancel running job
  PROCEDURE cancel_job(
    in_rec             IN cort_jobs%ROWTYPE,
    in_error_message   IN VARCHAR2 DEFAULT NULL
  );

  -- Finish job with error
  PROCEDURE fail_job(
    in_rec             IN cort_jobs%ROWTYPE,
    in_error_message   IN VARCHAR2
  );

  -- Update change_params for given job 
  PROCEDURE update_change_params(
    in_rec           IN cort_jobs%ROWTYPE,
    in_change_params IN CLOB
  );

END cort_job_pkg;
/