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

  Description: View returning latest version for cort_objects and details of job applied them
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  17.00   | Rustam Kafarov    | Created view
  ----------------------------------------------------------------------------------------------------------------------  
*/
CREATE OR REPLACE VIEW cort_recent_objects
AS
SELECT a.object_owner, a.object_name, a.object_type, a.job_id, a.last_ddl_text, a.last_ddl_time, a.application, a.release, a.build, a.change_type, a.revert_name, a.prev_synonym, 
       j.job_name, j.run_params, j.change_params, j.output, j.username, j.osuser, j.machine, j.terminal, j.module
  FROM (SELECT a.*, 
               MAX(job_id) OVER (PARTITION BY  object_owner, object_name, object_type) AS last_job_id  
          FROM cort_objects a) a
  LEFT JOIN cort_jobs j
    ON j.job_id = a.job_id
   AND j.object_owner = a.object_owner
   AND j.object_name = a.object_name 
   AND j.object_type = a.object_type  
   AND j.status = 'COMPLETED'        
 WHERE a.job_id = last_job_id; 
  