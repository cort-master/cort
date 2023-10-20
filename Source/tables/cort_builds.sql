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
  Description: Script for cort_builds table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing builds - instances of deployments all code fore given release 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_BUILDS ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_BUILDS') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  

CREATE TABLE cort_builds(
  application                    VARCHAR2(20)  NOT NULL,
  build                          VARCHAR2(30)  NOT NULL,  
  status                         VARCHAR2(10)  NOT NULL CHECK (status in ('RUNNING','STALE','COMPLETED','REVERTED','FAILED')),
  session_id                     VARCHAR2(30)  NOT NULL,
  release                        VARCHAR2(20),
  start_time                     DATE,
  end_time                       DATE,
  username                       VARCHAR2(128),
  osuser                         VARCHAR2(30),
  machine                        VARCHAR2(64),
  terminal                       VARCHAR2(16),
  module                         VARCHAR2(48),
  CONSTRAINT cort_builds_pk
    PRIMARY KEY (build)
);
