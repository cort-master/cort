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
  Description: Script for cort_stat table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | Help table for copying stats across tables  
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_STAT ----

BEGIN
  FOR X IN (SELECT *
              FROM user_all_tables
             WHERE table_name = 'CORT_STAT') 
  LOOP
    EXECUTE IMMEDIATE 'DROP TABLE CORT_STAT';
  END LOOP; 
  dbms_stats.create_stat_table(
    ownname          => USER,
    stattab          => 'CORT_STAT',
    global_temporary => TRUE
  );
END;
/

DROP INDEX CORT_STAT;

GRANT SELECT, INSERT, UPDATE, DELETE ON cort_stat TO PUBLIC;

