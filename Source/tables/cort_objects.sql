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
  Description: Script for cort_objects table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing history of all changes made to database objects via cort 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
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
  object_owner                   VARCHAR2(128) NOT NULL,
  object_name                    VARCHAR2(128) NOT NULL,
  object_type                    VARCHAR2(30)  NOT NULL,
  exec_time                      TIMESTAMP(9)  NOT NULL,
  sid                            VARCHAR2(30)  NOT NULL,
  sql_text                       CLOB,
  last_ddl_time                  DATE,
  application                    VARCHAR2(20),
  release                        VARCHAR2(20),
  build                          VARCHAR2(20),
  change_type                    VARCHAR2(30), 
  metadata                       CLOB, --XMLTYPE
  forward_ddl                    CLOB, --XMLTYPE,
  revert_ddl                     CLOB, --XMLTYPE,
  revert_name                    VARCHAR2(128),
  last_ddl_index                 NUMBER(9),
  rename_name                    VARCHAR2(128),
  CONSTRAINT t$cort_objects_pk PRIMARY KEY (object_owner, object_name, object_type, exec_time)
    USING INDEX COMPRESS
)
;

CREATE INDEX cort_objects_release_idx ON cort_objects(application, release, object_name); 

