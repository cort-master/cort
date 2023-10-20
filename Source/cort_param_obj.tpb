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
  19.00   | Rustam Kafarov    | Created new SQL type bodies to wotj with parameter values 
  ----------------------------------------------------------------------------------------------------------------------  
*/

create or replace type body cort_param_obj as 

  static function init(in_name in varchar2, in_default in varchar, in_regexp in varchar2, in_case_sensitive in boolean default false) return cort_param_obj
  as
    l_case_sensitive varchar2(1);
    l_default        varchar2(4000);
    l_regexp         varchar2(4000);
    l_type           varchar2(15);
  begin
    if in_case_sensitive then
      l_case_sensitive := null;
    else
      l_case_sensitive := 'i';
    end if;     

    if in_default like '(%)' and in_regexp is not null then
      l_regexp := '('||in_regexp||')';
      l_regexp := '(\('||l_regexp||'(,'||l_regexp||')*\))|(\(\))';
      l_case_sensitive := 'i';
      l_type := 'SET';
    else
      l_regexp := in_regexp;
      l_type := 'VARCHAR2';
    end if;  
    
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;

    l_default := in_default;

    if l_case_sensitive = 'i' then
      l_default := upper(l_default);
    end if;     

    if l_regexp is not null and not regexp_like(l_default, l_regexp, 'i') then
      raise_application_error(-20000, 'Invalid default value: '||l_default||'.  Expected to match following regexp '||l_regexp);
    end if;  

    return cort_param_value_obj(in_name, l_type, l_default, l_regexp, l_case_sensitive, null, null);
  end;

  static function init(in_name in varchar2, in_default in boolean) return cort_param_obj
  as
    l_default       varchar2(4000);
  begin
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;

    l_default := case 
                   when in_default then 'TRUE'
                   when not in_default then 'FALSE'
                   else 'NULL'
                 end;  
    
    if not regexp_like(l_default, 'TRUE|FALSE', 'i') then
      raise_application_error(-20000, 'Invalid default value: '||l_default||'.  Expected to match following regexp TRUE|FALSE');
    end if;  

    return cort_param_value_obj(in_name, 'BOOLEAN', l_default, 'TRUE|FALSE', 'i', null, null);
  end;

  static function init(in_name in varchar2, in_default in number, in_regexp in varchar2) return cort_param_obj
  as
    l_default       varchar2(4000);
  begin
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;

    l_default := to_char(in_default);

    if not regexp_like(l_default, in_regexp, 'i') then
      raise_application_error(-20000, 'Invalid default value: '||l_default||'.  Expected to match following regexp '||in_regexp);
    end if;  

    return cort_param_value_obj(in_name, 'NUMBER', to_char(l_default), in_regexp, 'i', null, null); 
  end;

  static function init(in_name in varchar2, in_default in varchar2_array) return cort_param_obj
  as
    l_pos       pls_integer;
    l_str_indx  arrays.gt_xlstr_indx;
    l_key       varchar2(32767);
    value_array varchar2_array;
    key_array   varchar2_array;
  begin
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;

    value_array := varchar2_array();
    key_array := varchar2_array();

    if in_default is not null then 
      for i in 1..in_default.count loop
        if in_default(i) is null then 
          raise_application_error(-20000, 'Invalid default value: null element #'||i);
        end if;
        l_pos := instr(in_default(i), '=');
        if l_pos > 0 then
          l_key := substr(in_default(i), 1, l_pos-1);
          l_str_indx(l_key) := substr(in_default(i), l_pos+1);
        else
          raise_application_error(-20000, 'Invalid default value: equal sign (=) is not found for elemnt #'||i);
        end if;  
      end loop;
    end if;  
    
    key_array.extend(l_str_indx.count);
    value_array.extend(l_str_indx.count);
   
    l_key := l_str_indx.first;
    l_pos := 1;
    while l_key is not null loop
      key_array(l_pos) := l_key;
      value_array(l_pos) := l_str_indx(l_key);
      l_pos := l_pos + 1;
      l_key := l_str_indx.next(l_key); 
    end loop;  

    return cort_param_value_obj(in_name, 'DICTIONARY', null, cort_params_pkg.gc_ditcitonary_regexp, null, key_array, value_array); 
  end;

end;
/



create or replace type body cort_param_value_obj as 

  member function find_key(in_key in varchar2, out_index out pls_integer) return boolean
  as
    l_indx  pls_integer;
    l_left  pls_integer;
    l_right pls_integer;
  begin
    if in_key is null then 
      raise_application_error(-20000, 'find_key: key could not be null');
    end if;
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
    return false; 
  end;

  overriding member function get_type return varchar2
  as
  begin
    return self.type;
  end;

  overriding member function get_regexp return varchar2
  as
  begin
    return self.regexp;
  end;

  overriding member function get_value return varchar2
  as
    l_value varchar2(32767);
  begin
    if type = 'DICTIONARY' then  
      for i in 1..key_array.count loop
        l_value := substr(l_value||key_array(i)||'='||value_array(i)||chr(13), 1, 32767);
      end loop;
      return trim(l_value);
    else
      return self.value;
    end if;  
  end;
  
  overriding member function get_value(in_key in varchar2) return varchar2
  as
    l_indx    pls_integer;
  begin
    if type = 'DICTIONARY' then  
      if find_key(in_key, l_indx) then
        return value_array(l_indx);
      else  
        return null;
      end if;  
    else
      raise_application_error(-20000, 'This method is not implemeted for '||type||' type');
    end if;  
  end;
  
  overriding member function get_bool_value return boolean
  as
  begin
    if type = 'BOOLEAN' then
      if upper(value) = 'TRUE' then
        return true;
      elsif upper(value) = 'FALSE' then
        return false;
      elsif upper(value) = 'NULL' then
        return null;
      elsif value is null then 
        return null;
      else  
        raise_application_error(-20000, 'Invalid boolean value: '||value);
      end if;  
    else
      raise_application_error(-20000, 'Invalid param type : '||name||' type is '||type);
    end if;  
  end;

  overriding member function get_num_value return number
  as
  begin
    if self.type = 'NUMBER' then
      return to_number(self.value);
    else
      raise_application_error(-20000, 'Invalid param type : '||self.name||' type is '||self.type);
    end if;  
  end;

  overriding member function exist(in_value in varchar) return boolean
  as
    l_indx pls_integer;
  begin
    case type 
    when 'SET' then
      return nvl(regexp_substr(value, '(\(|,)('||in_value||')(,|\))', 1, 1, null, 2), '?') = in_value;
    when 'DICTIONARY' then
      return find_key(in_value, l_indx);
    else
      raise_application_error(-20000, 'Invalid param type : '||self.name||' type is '||self.type);
    end case;  
  end;

  overriding member function is_empty return boolean
  as
  begin
    case type 
    when 'SET' then
      return value = '()' or value is null;
    when 'DICTIONARY' then
      return key_array.count = 0;
    else
      raise_application_error(-20000, 'Invalid param type : '||self.name||' type is '||self.type);
    end case;  
  end;

  overriding member procedure set_value(in_value in varchar2)
  as
    l_indx       pls_integer;
    l_key        varchar2(32767);
    l_value      varchar2(32767);
    l_norm_input varchar2(32767);
    l_pos        pls_integer;
    l_start_pos  pls_integer;
    l_end_pos    pls_integer;
  begin
    if self.case_sensitive = 'i' then
      l_value := upper(in_value);
    else 
      l_value := in_value;   
    end if;
     
    if type = 'DICTIONARY' then
      -- cleanup
      if in_value is null then
        key_array.delete;
        value_array.delete;
        return;
      end if;
      
      -- normalize input string
      l_pos := 1;
      loop
        l_start_pos := regexp_instr(in_value, '("[^"]+")', l_pos, 1, 0);
        exit when l_start_pos = 0;
        l_end_pos := regexp_instr(in_value, '("[^"]+")', l_pos, 1, 1);
        exit when l_end_pos = 0; 
        l_norm_input := l_norm_input ||substr(in_value, l_pos, l_start_pos-l_pos)||'"'||lpad('x', l_end_pos - l_start_pos - 2, 'x')||'"'; 
        l_pos := l_end_pos;
      end loop;
      l_norm_input := l_norm_input ||substr(in_value, l_pos); 
      
      l_indx := instr(l_norm_input, '=');
      if l_indx > 0 then
        dbms_utility.canonicalize(substr(in_value, 1, l_indx-1), l_key, 32767);
        l_value := substr(in_value, l_indx+1);
      else
        raise_application_error(-20000, 'set_value: equal sign (=) is not found');
      end if;  

      if find_key(l_key, l_indx) then
        value_array(l_indx) := l_value;
      else
        key_array.extend;
        value_array.extend;
        for i in reverse l_indx..key_array.count-1 loop
          key_array(i+1) := key_array(i);
          value_array(i+1) := value_array(i);
        end loop;   
        key_array(l_indx) := l_key;
        value_array(l_indx) := l_value;
      end if;
    else
      if self.regexp is not null then
        if not regexp_like(l_value, regexp, case_sensitive) then
          raise_application_error(-20000, 'Invalid param value: '||in_value||'.  Expected to match following regexp '||regexp);
        end if;
      end if;

      value := l_value;
    end if;  
  end;

  overriding member procedure set_value(in_value in boolean)
  as
  begin
    if type = 'BOOLEAN' then
      value := case when in_value then 'TRUE' when not in_value then 'FALSE' else 'NULL' end;
    else 
      raise_application_error(-20000, 'This method is not implemeted for '||type||' type');
    end if; 
  end;
  
  overriding member function get_xml_value return clob
  as
    l_result clob;
  begin
    if type = 'DICTIONARY' then
      l_result := '<'||name||'/>';
      for i in 1..key_array.count loop
        l_result := l_result||chr(10)||'   <'||name||'>'||dbms_xmlgen.convert(key_array(i)||'='||value_array(i), dbms_xmlgen.entity_encode)||'</'||name||'>';
      end loop;
      return l_result;      
    else
      return '<'||name||'>'||dbms_xmlgen.convert(value, dbms_xmlgen.entity_encode)||'</'||name||'>';      
    end if;  
  end;

  overriding member function get_count return pls_integer
  as
  begin
    case type 
    when 'SET' then
      return case when value = '()' or value is null then 0 else regexp_count(value, ',')+1 end;
    when 'DICTIONARY' then
      return key_array.count;
    else
      raise_application_error(-20000, 'Invalid param type : '||self.name||' type is '||self.type);
    end case;  
  end;
  
  overriding member function get_key(n in pls_integer) return varchar2
  as
  begin
    if type = 'DICTIONARY' then
      return key_array(n);
    else 
      raise_application_error(-20000, 'Invalid param type : '||self.name||' type is '||self.type);
    end if;  
  end;

  overriding member function get_value_by_pos(n in pls_integer) return varchar2
  as
  begin
    if type = 'DICTIONARY' then
      return value_array(n);
    else 
      raise_application_error(-20000, 'Invalid param type : '||self.name||' type is '||self.type);
    end if;  
  end;

end;
/
