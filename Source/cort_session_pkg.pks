CREATE OR REPLACE PACKAGE cort_session_pkg
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
  Description: session level API for end-user.  
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Added API to read/change params for current session only
  17.00   | Rustam Kafarov    | Replaced TIMESTAMP with timestamp_tz_unconstrained
  18.01   | Rustam Kafarov    | Added set_params
  19.00   | Rustam Kafarov    | Revised parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  /* This package will be granted to public */
  
  PROCEDURE reset_params;

  FUNCTION get_params
  RETURN cort_params_pkg.gt_params_rec;

  FUNCTION get_param(
    in_param_name   IN VARCHAR2
  )
  RETURN cort_param_obj;

  -- getter for session params
  FUNCTION get_param_value(
    in_param_name   IN VARCHAR2
  )
  RETURN VARCHAR2;
  
  -- setter for session params
  PROCEDURE set_param_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN VARCHAR2
  );
  
  -- functionm wrapper for TEST param
  FUNCTION test RETURN BOOLEAN;

  -- enable CORT for session
  PROCEDURE enable;
  
  -- disable CORT for session
  PROCEDURE disable;

  -- get CORT status (ENABLED/DISABLED)
  FUNCTION get_status
  RETURN VARCHAR2;
  
END cort_session_pkg;
/