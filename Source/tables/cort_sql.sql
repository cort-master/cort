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
  Description: Script for cort_thread_sql table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing threading sql/ddl statements
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  21.00   | Rustam Kafarov    | Table  cort_thread_sql renamed to cort_sql 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_SQL ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_SQL') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  


CREATE TABLE cort_sql(
  job_id              TIMESTAMP(6)     NOT NULL,
  object_type         VARCHAR2(30 )    NOT NULL,
  object_owner        VARCHAR2(128)    NOT NULL,
  object_name         VARCHAR2(128)    NOT NULL,
  seq_num             NUMBER           NOT NULL,
  status              VARCHAR2(20)     NOT NULL,
  group_type          VARCHAR2(50)     NOT NULL,
  change_sql          CLOB,
  revert_sql          CLOB,
  start_time          TIMESTAMP,
  end_time            TIMESTAMP,
  revert_start_time   TIMESTAMP,
  revert_end_time     TIMESTAMP,
  threadable          VARCHAR2(1),
  thread_id           VARCHAR2(30),
  error_msg           VARCHAR2(1000),
  constraint cort_sql_status_chk check (status in ('REGISTERED','RUNNING','FAILED','COMPLETED','TESTED',
                                                   'REVERTING', 'FAILED ON REVERT', 'REVERTED')),
  constraint cort_sql_pk primary key(job_id, seq_num)
);


