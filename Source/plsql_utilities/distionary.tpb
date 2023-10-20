create or replace type body dictionary as

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


  constructor function dictionary(in_case_sensitive in boolean default true, in_dup_key_action in pls_integer default 1) return self as result
  as 
  begin
    if in_case_sensitive 
      then self.case_sensitive := 'Y';
      else self.case_sensitive := 'N';
    end if;  
    if in_dup_key_action in (-1, 0, 1) then
      self.dup_key_action := in_dup_key_action;
    else    
      raise_application_error(-20000, 'Init: invalid value for in_dup_key_action: '||in_dup_key_action||'. Expected to be one of (-1, 0, 1)');  
    end if;  
    self.key_array := varchar2_array();
    self.value_array := varchar2_array();
    return;
  end;
  
  constructor function dictionary(key_values in varchar2_array, in_case_sensitive in boolean default true, in_dup_key_action in pls_integer default 1) return self as result
  as
  begin
    if in_case_sensitive 
      then self.case_sensitive := 'Y';
      else self.case_sensitive := 'N';
    end if;  
    if in_dup_key_action in (-1, 0, 1) then
      self.dup_key_action := in_dup_key_action;
    else
      raise_application_error(-20000, 'Init: invalid value for in_dup_key_action: '||in_dup_key_action||'. Expected to be one of (-1, 0, 1)');  
    end if;  
    self.key_array := varchar2_array();
    self.value_array := varchar2_array();
    self.add_values(key_values);
    return;
  end;

  member function get_count return number
  as
  begin
    return nvl(self.key_array.count,0);
  end;

  member function find_key(in_key in varchar2, out_index out pls_integer) return boolean
  as
    l_key   varchar2(32767);
    l_indx  pls_integer;
    l_left  pls_integer;
    l_right pls_integer;
  begin
    if in_key is null then 
      raise_application_error(-20000, 'find_key: key could not be null');
    end if;
    if self.case_sensitive = 'Y' then
      l_left := 1;
      l_right := key_array.count;
      out_index := 1;
      loop
        l_indx := trunc((l_right-l_left)/2) + l_left;
        exit when l_left > l_right;
        case 
          when in_key = key_array(l_indx) then  
            out_index := l_indx; 
            return true;
          when in_key < key_array(l_indx) then
            l_right := l_indx-1; 
            out_index := l_indx;
          when in_key > key_array(l_indx) then 
            l_left := l_indx+1;
            out_index := l_indx + 1;
        end case;  
      end loop;
    else
      l_key := upper(in_key);
      l_left := 1;
      l_right := key_array.count;
      out_index := 1;
      loop
        l_indx := trunc((l_right-l_left)/2) + l_left;
        exit when l_left > l_right;
        case 
          when l_key = key_array(l_indx) then  
            return true;
          when l_key < key_array(l_indx) then
            l_right := l_indx-1; 
            out_index := l_indx;
          when l_key > key_array(l_indx) then 
            l_left := l_indx+1;
            out_index := l_indx + 1;
        end case;  
      end loop;
    end if;

    return false; 
  end;
  
  member function get_value(in_index in pls_integer) return varchar2
  as
  begin
    if in_index between 1 and value_array.count then
      return value_array(in_index);
    else
      return null;
    end if;
  end;

  member function value(in_key in varchar2) return varchar2
  as
    l_indx pls_integer;
  begin
    if find_key(in_key, l_indx) then
      return value_array(l_indx);
    else
      raise_application_error(-20000, 'Key '||in_key||' not found');
    end if;
  end;
  
  member procedure add_value(in_key in varchar2, in_value in varchar2)
  as
    l_indx    pls_integer;
    l_nearest pls_integer;
    l_key     varchar2(32767);
  begin
    if in_key is null then 
      raise_application_error(-20000, 'add_value: key could not be null');
    end if;

    if self.case_sensitive = 'Y' then
      l_key := in_key;
    else
      l_key := upper(in_key);
    end if;

    if find_key(l_key, l_indx) then
      case self.dup_key_action
      when -1 then
        raise_application_error(-20000, 'add_value: key '||l_key||' already has value');
      when 0 then 
        null;  
      else  
        value_array(l_indx) := in_value;
      end case;    
    else
      key_array.extend;
      value_array.extend;
      for i in reverse l_indx..key_array.count-1 loop
        key_array(i+1) := key_array(i);
        value_array(i+1) := value_array(i);
      end loop;   
      key_array(l_indx) := l_key;
      value_array(l_indx) := in_value;
    end if;
  end;
  
  member procedure add_value(in_key_value in varchar2)
  as
    l_key   varchar2(32767);
    l_value varchar2(32767);
    l_pos    pls_integer;
  begin
    if in_key_value is null then 
      raise_application_error(-20000, 'add_value: key could not be null');
    end if;
    l_pos := instr(in_key_value, '=');
    if l_pos > 0 then
      l_key := substr(in_key_value, 1, l_pos-1);
      l_value := substr(in_key_value, l_pos+1);
    else
      raise_application_error(-20000, 'add_value: equal sign (=) is not found');
    end if;  
    add_value(l_key, l_value);
  end;


  member procedure add_values(in_keys in varchar2_array, in_values in varchar2_array)
  as
    l_str_indx   arrays.gt_xlstr_indx;
    l_indx       varchar2(32767);
    l_cnt        pls_integer;
  begin
    if in_keys is null or in_values is null then 
      return; 
    end if;
    if in_keys.count <> in_values.count then 
      raise_application_error(-20000,'add_values: input arrays must have same number of elements'); 
    end if;
    
    for k in 1..key_array.count loop
      l_str_indx(key_array(k)) := value_array(k);
    end loop;
    
    for k in 1..in_keys.count loop
      if case_sensitive = 'Y' then 
        l_indx := in_keys(k);
      else
        l_indx := upper(in_keys(k));
      end if;  
      if l_indx is null then
        raise_application_error(-20000, 'add_values: null key found on position '||k);
      end if;
      if self.dup_key_action in (-1, 0) then
        if l_str_indx.exists(l_indx) then
          if self.dup_key_action = -1 then
            raise_application_error(-20000, 'add_values: duplicated key found on position '||k||' : '||in_keys(k));
          end if;
        else
          l_str_indx(l_indx) := in_values(k);
        end if;
      else  
        l_str_indx(l_indx) := in_values(k);
      end if;    
    end loop;

    key_array.extend(l_str_indx.count - key_array.count);
    value_array.extend(l_str_indx.count - value_array.count);
   
    l_indx := l_str_indx.first;
    l_cnt := 1;
    while l_indx is not null loop
      key_array(l_cnt) := l_indx;
      value_array(l_cnt) := l_str_indx(l_indx);
      l_cnt := l_cnt + 1;
      l_indx := l_str_indx.next(l_indx); 
    end loop;  

  end;

  member procedure add_values(in_key_values in varchar2_array)
  as
    l_keys   varchar2_array := varchar2_array(); 
    l_values varchar2_array := varchar2_array();
    l_pos    pls_integer;
  begin
    if in_key_values is null then 
      return; 
    end if;
    
    l_keys.extend(in_key_values.count); 
    l_values.extend(in_key_values.count);

    for i in 1..in_key_values.count loop
      if in_key_values(i) is null then 
        raise_application_error(-20000, 'add_values: null found on position '||i);
      end if;
      l_pos := instr(in_key_values(i), '=');
      if l_pos > 0 then
        l_keys(i) := substr(in_key_values(i), 1, l_pos-1);
        l_values(i) := substr(in_key_values(i), l_pos+1);
      else
        raise_application_error(-20000, 'split: equal sign (=) is not found on position '||i);
      end if;  
    end loop;
    
    add_values(l_keys, l_values);
  end;
  
end;
/
