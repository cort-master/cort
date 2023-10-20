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
  Description: job execution API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | Created new SQL type to store parameters. Added array type of parameters 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  21.00   | Rustam Kafarov    | Added support for dictionary: array of key=value strings. Reading default value from cort_params table
  ----------------------------------------------------------------------------------------------------------------------  
*/
create or replace type cort_param_obj force as object(
  name   varchar2(30),
  -- init for varchar2 and set types
  static function init(in_name in varchar2, in_default in varchar, in_regexp in varchar2, in_case_sensitive in boolean default false) return cort_param_obj,
  -- init for boolean type
  static function init(in_name in varchar2, in_default in boolean) return cort_param_obj,
  -- init for numeric type 
  static function init(in_name in varchar2, in_default in number, in_regexp in varchar2) return cort_param_obj,
  -- init for dictionary type 
  static function init(in_name in varchar2, in_default in varchar2_array) return cort_param_obj,
  not final not instantiable member function get_type return varchar2,
  not final not instantiable member function get_regexp return varchar2,
  not final not instantiable member function get_value return varchar2, -- get value as string. Does not work for dictionaries
  not final not instantiable member function get_value(in_key in varchar2) return varchar2, -- for dictionaries returns value by key
  not final not instantiable member function get_bool_value return boolean, -- only for boolean params
  not final not instantiable member function get_num_value return number,  -- only for numeric params
  not final not instantiable member function exist(in_value in varchar) return boolean, -- for enumeration check value existance, for dictionaries checks key existance
  not final not instantiable member function is_empty return boolean, -- check if enumeration is empty
  not final not instantiable member procedure set_value(in_value in varchar2), -- set value. For dictionaries adds 1 key-value element into dictionary
  not final not instantiable member procedure set_value(in_value in boolean),  -- set value. overload for for Boolean types
  not final not instantiable member function get_xml_value return clob,
  not final not instantiable member function get_count return pls_integer,
  not final not instantiable member function get_key(n in pls_integer) return varchar2,
  not final not instantiable member function get_value_by_pos(n in pls_integer) return varchar2
)
not final
not instantiable
/

-- !!! don't use this type directly !!!
create or replace type cort_param_value_obj force under cort_param_obj (
  type                      varchar2(15),
  value                     varchar2(32767),
  regexp                    varchar2(4000),
  case_sensitive            varchar2(1), -- i - ignore case, null - leave case as is
  value_array               varchar2_array,
  key_array                 varchar2_array,
  member function find_key(in_key in varchar2, out_index out pls_integer) return boolean,
  overriding instantiable member function get_type return varchar2,
  overriding instantiable member function get_regexp return varchar2,
  overriding instantiable member function get_value return varchar2,
  overriding instantiable member function get_value(in_key in varchar2) return varchar2, 
  overriding instantiable member function get_bool_value return boolean,
  overriding instantiable member function get_num_value return number,
  overriding instantiable member function exist(in_value in varchar) return boolean,
  overriding instantiable member function is_empty return boolean,
  overriding instantiable member procedure set_value(in_value in varchar2),
  overriding instantiable member procedure set_value(in_value in boolean),
  overriding instantiable member function get_xml_value return clob,
  overriding instantiable member function get_count return pls_integer,
  overriding instantiable member function get_key(n in pls_integer) return varchar2,
  overriding instantiable member function get_value_by_pos(n in pls_integer) return varchar2
)  
instantiable
not final
/


