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
  Description: Script for cort_jobs table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing details of all executed jobs
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_JOBS ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_JOBS') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  


CREATE TABLE cort_jobs(
  sid                 VARCHAR2(30)    NOT NULL,
  action              VARCHAR2(30)    NOT NULL,
  status              VARCHAR2(30)    NOT NULL CHECK(status IN ('RUNNING','PENDING','FAILED','COMPLETED','CANCELED')),
  object_type         VARCHAR2(30)    NOT NULL,
  object_owner        VARCHAR2(128)   NOT NULL,
  object_name         VARCHAR2(128)   NOT NULL,
  job_owner           VARCHAR2(128)   NOT NULL,
  job_name            VARCHAR2(128)   NOT NULL,
  job_time            TIMESTAMP(6) WITH TIME ZONE,
  job_sid             VARCHAR2(30),
  sql_text            CLOB,
  new_name            VARCHAR2(128),
  current_schema      VARCHAR2(128),
  application         VARCHAR2(30),
  release             VARCHAR2(30),
  build               VARCHAR2(30),
  session_params      CLOB, --XMLTYPE,
  object_params       CLOB, --XMLTYPE,
  output              CLOB,
  run_time            INTERVAL DAY TO SECOND(9),
  parent_object_type  VARCHAR2(30),
  parent_object_owner VARCHAR2(128),
  parent_object_name  VARCHAR2(128),
  resume_action       VARCHAR2(30),
  session_id          NUMBER,
  username            VARCHAR2(128),
  osuser              VARCHAR2(30),
  machine             VARCHAR2(64),
  terminal            VARCHAR2(16),
  module              VARCHAR2(48),
  error_code          NUMBER,
  error_message       VARCHAR2(4000),
  error_backtrace     VARCHAR2(4000),
  error_stack         VARCHAR2(4000),
  call_stack          VARCHAR2(4000),
  CONSTRAINT cort_jobs_pk PRIMARY KEY (sid, object_owner, object_name) using index
)
;

CREATE UNIQUE INDEX cort_jobs_sid_running_uk ON cort_jobs(decode(status,'RUNNING',sid));

CREATE UNIQUE INDEX cort_jobs_active_objects_uk ON cort_jobs(object_owner, object_name, decode(status,'RUNNING',nvl(substr(sid,13),'0'),'PENDING','X',sid));
