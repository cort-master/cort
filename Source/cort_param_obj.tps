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
  Description: job execution API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | Created new SQL type to store parameters. Added array type of parameters 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/



whenever sqlerror continue

BEGIN
  FOR X IN (SELECT * FROM user_types WHERE type_name = 'CORT_PARAMS_ARR') LOOP
    EXECUTE IMMEDIATE 'DROP TYPE '||x.type_name||' FORCE';
  END LOOP;
END;
/  

BEGIN
  FOR X IN (SELECT * FROM user_types WHERE type_name = 'CORT_PARAM_VALUE_OBJ') LOOP
    EXECUTE IMMEDIATE 'DROP TYPE '||x.type_name||' FORCE';
  END LOOP;
END;
/  

BEGIN
  FOR X IN (SELECT * FROM user_types WHERE type_name = 'CORT_PARAM_OBJ') LOOP
    EXECUTE IMMEDIATE 'DROP TYPE '||x.type_name||' FORCE';
  END LOOP;
END;
/  


BEGIN
  FOR X IN (SELECT * FROM user_types WHERE type_name = 'ARRAY') LOOP
    EXECUTE IMMEDIATE 'DROP TYPE '||x.type_name||' FORCE';
  END LOOP;
END;
/  


whenever sqlerror exit failure

-------------------

create or replace type array as table of varchar2(30)
/

create or replace type cort_param_obj as object(
  name   varchar2(30),
  static function bool_to_str(in_value in boolean) return varchar2,
  static function str_to_bool(in_value in varchar2) return boolean,
  static function array_to_str(in_value in array) return varchar2,
  static function str_to_array(in_value in varchar2) return array,
  
  static function init(in_name in varchar2, in_default in varchar, in_regexp in varchar2, in_casesensitive in boolean default false) return cort_param_obj,
  static function init(in_name in varchar2, in_default in boolean) return cort_param_obj,
  static function init(in_name in varchar2, in_default in number, in_regexp in varchar2) return cort_param_obj,
  static function init(in_name in varchar2, in_default in array, in_regexp in varchar2) return cort_param_obj,
  
  not final not instantiable member function get_type return varchar2,
  not final not instantiable member function get_regexp return varchar2,
  not final not instantiable member function get_value return varchar2,
  not final not instantiable member function get_bool_value return boolean,
  not final not instantiable member function get_num_value return number,
  not final not instantiable member function get_array_value return array,
  not final not instantiable member function value_exists(in_value in varchar) return boolean,
  not final not instantiable member function is_empty return boolean,
  not final not instantiable member procedure set_value(in_value in varchar2),
  not final not instantiable member procedure set_value(in_bool_value in boolean),
  not final not instantiable member procedure set_value(in_num_value in number),
  not final not instantiable member procedure set_value(in_array_value in array)
)
not final
not instantiable
/

-- !!! don't use this type directly !!!
create or replace type cort_param_value_obj under cort_param_obj (
  type                      varchar2(30),
  value                     varchar2(4000),
  regexp                    varchar2(4000),
  casesensitive             varchar2(1), -- i - ignore case, null - leave case as is
  overriding instantiable member function get_type return varchar2,
  overriding instantiable member function get_regexp return varchar2,
  overriding instantiable member function get_value return varchar2,
  overriding instantiable member function get_bool_value return boolean,
  overriding instantiable member function get_num_value return number,
  overriding instantiable member function get_array_value return array,
  overriding instantiable member function value_exists(in_value in varchar) return boolean,
  overriding instantiable member function is_empty return boolean,
  overriding instantiable member procedure set_value(in_value in varchar2),
  overriding instantiable member procedure set_value(in_bool_value in boolean),
  overriding instantiable member procedure set_value(in_num_value in number),
  overriding instantiable member procedure set_value(in_array_value in array)
)
instantiable
not final
/

create or replace type cort_params_arr as table of cort_param_obj
/
