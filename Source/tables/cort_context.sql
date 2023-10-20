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
  Description: Script for cort_context table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | Temp table for emulation context work  
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_CONTEXT ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_CONTEXT') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  

CREATE GLOBAL TEMPORARY TABLE cort_context (
  name              VARCHAR2(30)   NOT NULL,
  value             VARCHAR2(4000),
  CONSTRAINT cort_context_pk PRIMARY KEY(name)
)
ON COMMIT PRESERVE ROWS
;

