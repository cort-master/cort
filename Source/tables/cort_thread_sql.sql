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
  Description: Script for cort_thread_sql table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing threading sql/ddl statements
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_THREAD_SQL ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_THREAD_SQL') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  


CREATE TABLE cort_thread_sql(
  object_owner        VARCHAR2(128)    NOT NULL,
  object_name         VARCHAR2(128)    NOT NULL,
  thread_id           NUMBER           NOT NULL,
  status              VARCHAR2(30)     NOT NULL,
  sql_text            CLOB             NOT NULL,
  start_time          TIMESTAMP,
  end_time            TIMESTAMP,
  job_name            VARCHAR2(128),
  error_msg           VARCHAR2(1000),
  constraint cort_thread_sql_status_chk check (status in ('REGISTERED','RUNNING','FAILED','COMPLETED')),
  constraint cort_thread_sql_pk primary key(object_owner, object_name, thread_id)
);


