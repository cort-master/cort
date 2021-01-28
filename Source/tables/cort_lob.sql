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
  Description: Script for cort_lob table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | Help temp table for reading LONG values  
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_LOB ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_LOB') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  

CREATE GLOBAL TEMPORARY TABLE cort_lob(
  text   clob
)
;
