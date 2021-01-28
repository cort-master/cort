CREATE OR REPLACE PACKAGE BODY cort_prev_exec_pkg
AS

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
  Description: Main package executing table recreation and rollback with current user privileges
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  16.00   | Rustam Kafarov    | Executes all statements under prev schema
  ----------------------------------------------------------------------------------------------------------------------  
*/


  PROCEDURE create_prev_synonym(
    in_synonym_name IN VARCHAR2,
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2
  )
  AS
    l_ddl varchar2(4000);
  BEGIN
    l_ddl := 'CREATE OR REPLACE SYNONYM "'||in_synonym_name||'" FOR "'||in_table_owner||'"."'||in_table_name||'"';
    execute immediate l_ddl;
  END create_prev_synonym;

END cort_prev_exec_pkg;
/