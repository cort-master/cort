CREATE OR REPLACE PACKAGE cort_trg_pkg
AUTHID CURRENT_USER
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
  Description: functionality called from create trigger     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support for new objects types and explain plan functionality extension
  15.00   | Rustam Kafarov    | Added alternative to CONTEXT using
  17.00   | Rustam Kafarov    | Added lock_object procedure
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- parses main CORT hint
  FUNCTION is_replace_mode(
    in_sql         IN CLOB
  )
  RETURN BOOLEAN;

  -- Function returns currently executing ddl statement. It could be called only from DDL triggers
  FUNCTION ora_dict_ddl
  RETURN CLOB;

  -- Returns 'REPLACE' if there is #OR REPLACE hint in given DDL or if this parameter is turned on for session.
  -- Otherwise returns 'CREATE'. It could be called only from DDL triggers
  FUNCTION get_execution_mode(
    in_object_type IN VARCHAR2
  )
  RETURN VARCHAR2;
  
  -- Return ENABLED/DISABLED
  FUNCTION get_status
  RETURN VARCHAR2;

  -- called from instead of trigger 
  PROCEDURE instead_of_create; 
   
  -- called from before create trigger
  PROCEDURE before_create;
  
  -- called from row-level trigger on plan table
  PROCEDURE before_insert_xplan(
    io_id           IN OUT INTEGER,
    io_parent_id    IN OUT INTEGER,
    io_depth        IN OUT INTEGER,  
    io_operation    IN OUT VARCHAR2, 
    in_statement_id IN VARCHAR2,
    in_plan_id      IN NUMBER,
    in_timestamp    IN TIMESTAMP, 
    out_other_xml   OUT NOCOPY CLOB  
  );

  -- called from before DDL trigger
  PROCEDURE lock_object;

END cort_trg_pkg;
/