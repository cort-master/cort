create or replace type dictionary force as object(
/*
PL/SQL Utilities - Dictionary objects 

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
  Description: SQL types to work with key=value arrays     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  21.00   | Rustam Kafarov    | Define Dictionary SQL type for working with key=value arrays
  ----------------------------------------------------------------------------------------------------------------------  
*/

  key_array            varchar2_array,
  value_array          varchar2_array,
  case_sensitive       char(1),
  dup_key_action       number(1),
  -- default constructor
  -- in_dup_key_action: -1 - raise exception, 0 - ignore, 1 - update value
  constructor function dictionary(in_case_sensitive in boolean default true, in_dup_key_action in pls_integer default 1) return self as result,
  constructor function dictionary(key_values in varchar2_array, in_case_sensitive in boolean default true, in_dup_key_action in pls_integer default 1) return self as result,
  -- return number of elements
  member function get_count return number,
  -- find key in array and return it's position
  member function find_key(in_key in varchar2, out_index out pls_integer) return boolean,
  -- find value by key. If key not found returns null
  member function get_value(in_index in pls_integer) return varchar2,
  -- find value by key. If key not found raises exception 
  member function value(in_key in varchar2) return varchar2,
  -- add new pair of key-value
  member procedure add_value(in_key in varchar2, in_value in varchar2),
  -- add value from key=value string
  member procedure add_value(in_key_value in varchar2),
  -- add array of keys and values
  member procedure add_values(in_keys in varchar2_array, in_values in varchar2_array),
  -- add array of keys=value strings
  member procedure add_values(in_key_values in varchar2_array)
)
/

