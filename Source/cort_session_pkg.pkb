CREATE OR REPLACE PACKAGE BODY cort_session_pkg
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
  Description: Session level API for end-user.  
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Added API to read/change params for current session only
  17.00   | Rustam Kafarov    | Replaced TIMESTAMP with timestamp_tz_unconstrained
  18.01   | Rustam Kafarov    | Added set_params. Removed init_params
  19.00   | Rustam Kafarov    | Revised parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_session_params_rec      cort_params_pkg.gt_params_rec;

  /* Public */

  PROCEDURE reset_params
  AS
    l_default_params_rec      cort_params_pkg.gt_params_rec;
    l_param_arr               arrays.gt_str_arr; 
    l_default_arr             arrays.gt_lstr_arr;
    l_param_names_indx        arrays.gt_str_indx;
  BEGIN
    SELECT param_name, default_value
      BULK COLLECT 
      INTO l_param_arr, l_default_arr 
      FROM cort_params
     WHERE user_name = user;
     
    l_param_names_indx := cort_params_pkg.get_param_names_indx;
     
    FOR i IN 1..l_param_arr.COUNT LOOP
      IF l_param_names_indx.EXISTS(l_param_arr(i)) THEN
        cort_params_pkg.set_param_value(l_default_params_rec, l_param_arr(i), l_default_arr(i));
      END IF;  
    END LOOP;
    g_session_params_rec := l_default_params_rec;
  END reset_params;
  
  FUNCTION get_params RETURN cort_params_pkg.gt_params_rec
  AS 
  BEGIN
    RETURN g_session_params_rec;
  END get_params;

  FUNCTION get_param(
    in_param_name   IN VARCHAR2
  )
  RETURN cort_param_obj
  AS
  BEGIN
    RETURN cort_params_pkg.get_param(g_session_params_rec, in_param_name);  
  END get_param;
  
  -- getter for session params
  FUNCTION get_param_value(
    in_param_name   IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN cort_params_pkg.get_param_value(g_session_params_rec, in_param_name);  
  END get_param_value;
  
  -- setter for session params
  PROCEDURE set_param_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN VARCHAR2
  )
  AS
  BEGIN
    cort_params_pkg.set_param_value(g_session_params_rec, in_param_name, in_param_value);  
  END set_param_value;

  -- functionm wrapper for TEST param
  FUNCTION test RETURN BOOLEAN
  AS
  BEGIN
    RETURN g_session_params_rec.test.get_bool_value; 
  END test;

  PROCEDURE enable
  AS
  BEGIN
    cort_aux_pkg.set_context('DISABLED_FOR_SESSION',NULL);
  END enable;
  
  PROCEDURE disable
  AS
  BEGIN
    cort_aux_pkg.set_context('DISABLED_FOR_SESSION','TRUE');
  END disable;

  -- get CORT status (ENABLED/DISABLED)
  FUNCTION get_status
  RETURN VARCHAR2
  AS
    l_status VARCHAR2(20);
  BEGIN
    l_status := cort_pkg.get_status;
    IF l_status = 'ENABLED' THEN
      IF cort_aux_pkg.get_context('DISABLED_FOR_SESSION') = 'TRUE' THEN
        l_status := 'DISABLED';
      END IF;  
    END IF;
    RETURN l_status;  
  END get_status;


BEGIN
  reset_params;   
END cort_session_pkg;
/