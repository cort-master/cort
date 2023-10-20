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

  Description: View returning latest version of cort_job for each object 
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  17.00   | Rustam Kafarov    | Created view
  ----------------------------------------------------------------------------------------------------------------------  
*/
CREATE OR REPLACE VIEW cort_recent_jobs
AS
SELECT j.*,
       MAX(decode(job_id, last_job, substr(sid, 1, 12))) OVER(PARTITION BY object_owner, object_name, object_type) as last_sid
  FROM (SELECT j.*,
               MAX(job_id) OVER (PARTITION BY object_owner, object_name, object_type) AS last_job
          FROM cort_jobs j
         WHERE job_owner = USER
       ) j
 WHERE job_id = last_job; 
  