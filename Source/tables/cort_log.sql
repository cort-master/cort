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
  Description: Script for cort_log table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for logging all executed sql, debug echo and debug information 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  21.00   | Rustam Kafarov    | Added log_type_attr - additional attributes working as bitmask 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_LOG ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_LOG') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  

CREATE TABLE cort_log(
  log_time        TIMESTAMP(9)    NOT NULL,
  job_id          TIMESTAMP(6),
  action          VARCHAR2(30),
  object_owner    VARCHAR2(128),
  object_name     VARCHAR2(128),
  log_type        VARCHAR2(100),
  echo_flag       VARCHAR2(1), --Y/N
  text            VARCHAR2(4000),
  details         CLOB,
  revert_ddl      CLOB,
  execution_time  INTERVAL DAY TO SECOND(9),
  call_stack      VARCHAR2(4000),
  error_message   VARCHAR2(4000),
  error_stack     VARCHAR2(4000)
)
PARTITION BY RANGE(log_time)
  INTERVAL (INTERVAL '1' MONTH)
( PARTITION p_start VALUES LESS THAN (DATE'2019-01-01') )
;

CREATE INDEX cort_log_idx ON cort_log(job_id) LOCAL;