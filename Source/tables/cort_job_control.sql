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
  Description: Script for cort_job_control table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | job control table for setting locks on running job instances
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  21.00   | Rustam Kafarov    | Change structure to spupport 2 way lock by object name 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_JOB_CONTROL ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_JOB_CONTROL') LOOP
    EXECUTE IMMEDIATE 'drop table '||x.table_name;
  END LOOP;
END;
/  


CREATE TABLE cort_job_control(
  object_owner        VARCHAR2(128)   NOT NULL,
  object_name         VARCHAR2(128)   NOT NULL,
  lock_type           VARCHAR2(15)    NOT NULL CHECK(lock_type in ('CONTROL SESSION', 'JOB SESSION')), 
  CONSTRAINT cort_job_control_pk PRIMARY KEY(object_owner, object_name, lock_type)
);

