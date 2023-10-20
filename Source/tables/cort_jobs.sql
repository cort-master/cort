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
  Description: Script for cort_jobs table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing details of all executed jobs
  21.00   | Rustam Kafarov    | session_params renamed to run_params, object_params to change_params. Values for application, build, release moved into run_params XML
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_JOBS ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_JOBS') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  



DECLARE 
  l_cnt NUMBER;
BEGIN
  SELECT count(*) 
    INTO l_cnt
    FROM user_sequences 
   WHERE sequence_name = 'CORT_JOB_SEQ';
  IF l_cnt > 0 THEN
    EXECUTE IMMEDIATE 'DROP SEQUENCE CORT_JOB_SEQ';
  END IF;
  EXECUTE IMMEDIATE 'CREATE SEQUENCE CORT_JOB_SEQ START WITH 0 CYCLE MINVALUE 0 MAXVALUE 999 NOCACHE';
END;
/  


CREATE TABLE cort_jobs(
  job_id              TIMESTAMP(6)    NOT NULL, 
  job_owner           VARCHAR2(128)   NOT NULL,
  job_name            VARCHAR2(128)   NOT NULL,
  sid                 VARCHAR2(30)    NOT NULL,
  action              VARCHAR2(30)    NOT NULL,
  status              VARCHAR2(30)    NOT NULL CHECK(status IN ('REGISTERED','RUNNING','PENDING','FAILED','COMPLETED','CANCELED')),
  object_type         VARCHAR2(30)    NOT NULL,
  object_owner        VARCHAR2(128)   NOT NULL,
  object_name         VARCHAR2(128)   NOT NULL,
  sql_text            CLOB,
  new_name            VARCHAR2(128),  
  current_schema      VARCHAR2(128),
  run_params          CLOB, --XMLTYPE,
  change_params       CLOB, --XMLTYPE,
  output              CLOB,
  start_time          TIMESTAMP WITH TIME ZONE,
  end_time            TIMESTAMP WITH TIME ZONE,
  run_time            INTERVAL DAY TO SECOND(9),
  parent_job_id       TIMESTAMP(3),
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
  CONSTRAINT cort_jobs_pk PRIMARY KEY (job_id) using index
)
;

CREATE UNIQUE INDEX cort_jobs_sid_running_uk ON cort_jobs(decode(status,'RUNNING',sid,'REGISTERED',sid));

CREATE UNIQUE INDEX cort_jobs_active_objects_uk ON cort_jobs(object_owner, object_name, decode(status,'RUNNING','R','PENDING','P','REGISTERED','G',TO_CHAR(job_id, 'YYYYMMDDHH24MISSFF9')));
