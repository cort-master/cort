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
  Description: Script for cort_applications table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | Configuration table for storing application names.  
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_APPLICATIONS ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_APPLICATIONS') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  

CREATE TABLE cort_applications(
  application                    VARCHAR2(20)  NOT NULL,
  release_regexp                 VARCHAR2(100),
  application_name               VARCHAR2(250),
  CONSTRAINT cort_applications_pk
    PRIMARY KEY (application)
)
ORGANIZATION INDEX
;


BEGIN
  INSERT INTO cort_applications VALUES('DEFAULT', '.*', 'DEFAULT');
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    NULL;  
END;
/  
