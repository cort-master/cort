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
  Description: Script for cort_job_log table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for logging all changes during job executin - snapshots of jobs taken when status is changed. 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_JOB_LOG ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_JOB_LOG') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  

CREATE TABLE cort_job_log(
  job_log_time        TIMESTAMP(9)     NOT NULL,
  sid                 VARCHAR2(30)     NOT NULL,
  action              VARCHAR2(30),
  status              VARCHAR2(30),
  object_type         VARCHAR2(30),
  object_owner        VARCHAR2(128),
  object_name         VARCHAR2(128),
  job_owner           VARCHAR2(128),
  job_name            VARCHAR2(128),
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
  parent_object_type  VARCHAR2(30),
  parent_object_owner VARCHAR2(128),
  parent_object_name  VARCHAR2(128),
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
  call_stack          VARCHAR2(4000)
)
;
