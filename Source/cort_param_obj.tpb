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
  19.00   | Rustam Kafarov    | Created new SQL type bodies to wotj with parameter values 
  ----------------------------------------------------------------------------------------------------------------------  
*/

create or replace type body cort_param_obj as 

  static function bool_to_str(in_value in boolean) return varchar2
  as
  begin
    if in_value then
      return 'TRUE';
    elsif not in_value then
      return 'FALSE';
    else
      return 'NULL';
    end if;  
  end;
  
  static function str_to_bool(in_value in varchar2) return boolean
  as
  begin
    if upper(in_value) = 'TRUE' then
      return true;
    elsif upper(in_value) = 'FALSE' then
      return false;
    elsif upper(in_value) = 'NULL' then
      return null;
    elsif in_value is null then 
      return null;
    else  
      raise_application_error(-20000, 'Invalid boolean value: '||in_value);
    end if;  
  end;

  static function array_to_str(in_value in array) return varchar2
  as
    l_result varchar2(4000);
  begin
    if in_value is not null then
      for i in 1..in_value.count loop
        l_result := l_result||in_value(i);
        if i < in_value.count then
          l_result := l_result||',';
        end if;
      end loop;
    end if;
    l_result := '('||upper(l_result)||')';
    return l_result; 
  end;
  
  static function str_to_array(in_value in varchar2) return array
  as
    l_result array;
  begin
    select regexp_substr (upper(in_value),'[^,]+',1,level)
      bulk collect 
      into l_result
      from dual
     connect by regexp_substr (upper(in_value),'[^,]+',1,level) is not null;
    return l_result; 
  end;

  
  static function init(in_name in varchar2, in_default in varchar, in_regexp in varchar2, in_casesensitive in boolean default false) return cort_param_obj
  as
    l_casesensitive varchar2(1);
    l_default       varchar2(4000);
  begin
    if in_casesensitive then
      l_casesensitive := null;
    else
      l_casesensitive := 'i';
    end if;     
    
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9$#]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;
    
    l_default := in_default;
    
    if not regexp_like(l_default, in_regexp, l_casesensitive) then
      raise_application_error(-20000, 'Invalid default value: '||l_default||'.  Expected to match following regexp '||in_regexp);
    end if;  
    return cort_param_value_obj(in_name, 'VARCHAR2', l_default, in_regexp, l_casesensitive); 
  end;

  static function init(in_name in varchar2, in_default in boolean) return cort_param_obj
  as
    l_default       varchar2(4000);
  begin
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9$#]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;

    l_default := bool_to_str(in_default);

    if not regexp_like(l_default, 'TRUE|FALSE', 'i') then
      raise_application_error(-20000, 'Invalid default value: '||l_default||'.  Expected to match following regexp TRUE|FALSE');
    end if;  

    return cort_param_value_obj(in_name, 'BOOLEAN', l_default, 'TRUE|FALSE', 'i'); 
  end;

  static function init(in_name in varchar2, in_default in number, in_regexp in varchar2) return cort_param_obj
  as
    l_default       varchar2(4000);
  begin
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9$#]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;

    l_default := to_char(in_default);

    if not regexp_like(l_default, in_regexp, 'i') then
      raise_application_error(-20000, 'Invalid default value: '||l_default||'.  Expected to match following regexp '||in_regexp);
    end if;  

    return cort_param_value_obj(in_name, 'NUMBER', to_char(l_default), in_regexp, 'i'); 
  end;

  static function init(in_name in varchar2, in_default in array, in_regexp in varchar2) return cort_param_obj
  as
    l_regexp   varchar2(4000);
    l_default  varchar2(4000);
  begin
    l_regexp := '('||in_regexp||')';
    l_regexp := '(\('||l_regexp||'(,'||l_regexp||')*\))|(\(\))';
    
    l_default := array_to_str(in_default);
    
    -- check name is valid identifier
    if not regexp_like(in_name, '[A-Z_][A-Z_0-9$#]{0,29}') then
      raise_application_error(-20000, 'Invalid name: '||in_name||'.  Expected to be valid identifier');
    end if;

    if not regexp_like(l_default, l_regexp, 'i') then
      raise_application_error(-20000, 'Invalid default value: '||l_default||'.  Expected to match following regexp '||l_regexp);
    end if;  
    
    return cort_param_value_obj(in_name, 'ARRAY', l_default, l_regexp, 'i'); 
  end;

end;
/



create or replace type body cort_param_value_obj as 

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
  begin
    return self.value;
  end;
  
  overriding member function get_bool_value return boolean
  as
  begin
    if self.type = 'BOOLEAN' then
      return cort_param_obj.str_to_bool(self.value);  
    else
      raise_application_error(-20000, 'Invalid param type : '||self.value||' type is '||self.type);
    end if;  
  end;

  overriding member function get_num_value return number
  as
  begin
    if self.type = 'NUMBER' then
      return to_number(self.value);
    else
      raise_application_error(-20000, 'Invalid param type : '||self.value||' type is '||self.type);
    end if;  
  end;

  overriding member function get_array_value return array
  as
  begin
    if self.type = 'ARRAY' then
      return cort_param_obj.str_to_array(self.value);
    else
      raise_application_error(-20000, 'Invalid param type : '||self.value||' type is '||self.type);
    end if;  
  end;

  overriding member function value_exists(in_value in varchar) return boolean
  as
  begin
    if self.type = 'ARRAY' then
      return regexp_instr(self.value, '\W'||in_value||'\W') > 0;
    else
      raise_application_error(-20000, 'Invalid param type : '||self.value||' type is '||self.type);
    end if;  
  end;

  overriding member function is_empty return boolean
  as
  begin
    if self.type = 'ARRAY' then
      return self.value = '()';
    else
      raise_application_error(-20000, 'Invalid param type : '||self.value||' type is '||self.type);
    end if;  
  end;

  overriding member procedure set_value(in_value in varchar2)
  as
  begin
    if self.regexp is not null then
      if not regexp_like(in_value, self.regexp, self.casesensitive) then
        raise_application_error(-20000, 'Invalid param value: '||in_value||'.  Expected to match following regexp '||self.regexp);
      else
        self.value := upper(in_value);
      end if;  
    else  
      self.value := in_value;
    end if;  
  end;

  overriding instantiable member procedure set_value(in_bool_value in boolean)
  as
  begin
    self.set_value(cort_param_obj.bool_to_str(in_bool_value));
  end;
  
  overriding instantiable member procedure set_value(in_num_value in number)
  as
  begin
    self.set_value(to_char(in_num_value));
  end;
  
  overriding instantiable member procedure set_value(in_array_value in array)
  as
  begin
    self.set_value(cort_param_obj.array_to_str(in_array_value));
  end;


end;
/

