CREATE OR REPLACE PACKAGE cort_log_pkg 
AS

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
  Description: Logging API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added get_last_log function
  17.00   | Rustam Kafarov    | Added columns current_schema, new_name; define job_time as timestamp_tz_unconstrained
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- execute attributes
  gc_execure constant pls_integer := 1; 
  gc_echo    constant pls_integer := 2; 
  gc_test    constant pls_integer := 4; 

  PROCEDURE init_log(
    in_job_rec  IN cort_jobs%ROWTYPE
  );
  
  -- returns error stack
  FUNCTION get_error_stack
  RETURN CLOB;

  PROCEDURE execute(
    in_sql            IN CLOB,
    in_echo           IN BOOLEAN
  );

  PROCEDURE update_exec_time;


  PROCEDURE test(
    in_ddl            IN CLOB,
    in_revert_ddl     IN CLOB DEFAULT NULL
  );

  PROCEDURE echo(
    in_text      IN CLOB
  );

  PROCEDURE echo_sql(
    in_sql      IN CLOB
  );

  PROCEDURE error(
    in_text      IN VARCHAR2,
    in_details   IN CLOB DEFAULT NULL,
    in_echo      IN BOOLEAN DEFAULT TRUE
  );

  PROCEDURE warning(
    in_text      IN VARCHAR2,
    in_details   IN CLOB DEFAULT NULL
  );

  PROCEDURE debug(
    in_text      IN VARCHAR2,
    in_details   IN CLOB DEFAULT NULL
  );
  
  PROCEDURE start_timer(
    in_text      IN VARCHAR2
  );
  
  PROCEDURE stop_timer(
    in_text      IN VARCHAR2
  );

END cort_log_pkg;
/