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
  Description: Script for cort_objects table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing history of all changes made to database objects via cort 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  21.00   | Rustam Kafarov    | Added job_id reference to cort_jobs. Moved forward/backward SQL into cort_sql table 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_OBJECTS ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_OBJECTS') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  


CREATE TABLE cort_objects(
  job_id                         TIMESTAMP(6)  NOT NULL,
  object_owner                   VARCHAR2(128) NOT NULL,
  object_name                    VARCHAR2(128) NOT NULL,
  object_type                    VARCHAR2(30)  NOT NULL,
  change_type                    VARCHAR2(30)  NOT NULL,
  last_ddl_text                  CLOB,
  last_ddl_time                  TIMESTAMP,
  change_params                  CLOB, --XMLTYPE,
  application                    VARCHAR2(20),
  release                        VARCHAR2(20),
  build                          VARCHAR2(20),
  revert_name                    VARCHAR2(128),
  prev_synonym                   VARCHAR2(128),
  CONSTRAINT cort_objects_pk PRIMARY KEY (job_id)
)
;

CREATE INDEX cort_objects_objects_idx ON cort_objects(object_owner, object_name);

CREATE INDEX cort_objects_release_idx ON cort_objects(application, release, object_name); 


