CREATE OR REPLACE PACKAGE BODY cort_params_pkg 
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
  Description: Type and API for main application parameters
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added API to read/write param default values
  15.00   | Rustam Kafarov    | Added IGNORE_ORDER param
  18.03   | Rustam Kafarov    | Added STATS param instead of keep_stats
  19.00   | Rustam Kafarov    | Used SQL type to work with parameters 
  21.00   | Rustam Kafarov    | Simplified API implementation. Used generated routines instead of dynamic sql. Split params into run and change specific
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- global variable for passing record into dynamic SQL
  gc_package_name          arrays.gt_name := $$plsql_unit;

  -- metadata variables 
  g_param_names_arr        arrays.gt_str_arr;
  g_param_names_indx       arrays.gt_int_indx;

  g_run_param_names_arr    arrays.gt_str_arr;
  g_run_param_names_indx   arrays.gt_int_indx;
  
  -- internal function used for Oracle version < 12.
  $IF dbms_db_version.version < 12 $THEN
  
  g_params_rec     gt_params_rec;
  g_run_params_rec gt_run_params_rec;
  
  
  FUNCTION get_params_rec RETURN gt_params_rec
  AS
  BEGIN
    RETURN g_params_rec;
  END get_params_rec;
  
  PROCEDURE set_params_rec(in_value IN gt_params_rec)
  AS
  BEGIN
    g_params_rec := in_value;
  END set_params_rec;


  FUNCTION get_run_params_rec RETURN gt_run_params_rec
  AS
  BEGIN
    RETURN g_run_params_rec;
  END get_run_params_rec;
  
  PROCEDURE set_run_params_rec(in_value IN gt_run_params_rec)
  AS
  BEGIN
    g_run_params_rec := in_value;
  END set_run_params_rec;
  $END


  PROCEDURE init_session_variables
  AS
    l_result                   PLS_INTEGER;
    l_dummy                    VARCHAR2(4000);
    l_cpu_count                PLS_INTEGER;
    l_parallel_threads_per_cpu PLS_INTEGER;
  BEGIN
    g_param_names_arr.DELETE;
    g_param_names_indx.DELETE;
    g_run_param_names_arr.DELETE;
    g_run_param_names_indx.DELETE;
    
    $IF dbms_db_version.version >= 18 $THEN
    SELECT attr_name as argument_name
      BULK COLLECT 
      INTO g_param_names_arr
      FROM user_plsql_type_attrs
     WHERE package_name = gc_package_name
       AND type_name = 'GT_PARAMS_REC'
     ORDER BY attr_no;     

    SELECT attr_name as argument_name
      BULK COLLECT 
      INTO g_run_param_names_arr
      FROM user_plsql_type_attrs
     WHERE package_name = gc_package_name
       AND type_name = 'GT_RUN_PARAMS_REC'
     ORDER BY attr_no;    
    $ELSE
    SELECT argument_name
      BULK COLLECT 
      INTO g_param_names_arr
      FROM user_arguments
     WHERE package_name = gc_package_name   
       AND object_name = 'GET_PARAM'
       AND data_level = 1
     ORDER BY sequence; 

    SELECT argument_name
      BULK COLLECT 
      INTO g_run_param_names_arr
      FROM user_arguments
     WHERE package_name = gc_package_name   
       AND object_name = 'GET_RUN_PARAM'
       AND data_level = 1
     ORDER BY sequence; 
    $END
            
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      g_param_names_indx(g_param_names_arr(i)) := i;
    END LOOP;

    FOR i IN 1..g_run_param_names_arr.COUNT LOOP
      g_run_param_names_indx(g_run_param_names_arr(i)) := i;
    END LOOP;
        
    l_result := dbms_utility.get_parameter_value('cpu_count', l_cpu_count, l_dummy);
    l_result := dbms_utility.get_parameter_value('parallel_threads_per_cpu', l_parallel_threads_per_cpu, l_dummy);
    gc_max_thread_number := NVL(l_cpu_count*l_parallel_threads_per_cpu, 64);

  END init_session_variables;
  

  FUNCTION get_name(in_line IN NUMBER) RETURN VARCHAR2
  AS
    l_name VARCHAR2(30);
    l_text varchar2(4000);
  BEGIN
    BEGIN
      SELECT text, UPPER(REGEXP_SUBSTR(text, '^ +([A-z0-9_]+) ', 1, 1, NULL, 1))
        INTO l_text, l_name  
        FROM user_source
       WHERE name = gc_package_name
         AND type = 'PACKAGE'
         AND line = in_line;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_name := null;
    END;  
    
    RETURN l_name;  
  END get_name;

  -- global declarations
  
  FUNCTION init(in_line IN NUMBER, in_default IN VARCHAR, in_regexp in VARCHAR2, in_case_sensitive IN BOOLEAN DEFAULT FALSE) RETURN cort_param_obj
  AS
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), in_default, in_regexp, in_case_sensitive);
  END init;
  
  FUNCTION init(in_line IN NUMBER, in_default IN BOOLEAN) RETURN cort_param_obj
  AS
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), in_default);
  END init;
  
  FUNCTION init(in_line IN NUMBER, in_default IN NUMBER, in_regexp in VARCHAR2) RETURN cort_param_obj
  AS
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), in_default, in_regexp);
  END init;

  FUNCTION init_dictionary(in_line IN NUMBER) RETURN cort_param_obj
  AS
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), varchar2_array());
  END init_dictionary;

  FUNCTION get_param(in_params_rec IN gt_params_rec, in_param_name IN VARCHAR2) RETURN cort_param_obj
  AS
    l_result cort_param_obj;
    l_sql    varchar2(1000);
  BEGIN
    IF param_exists(in_param_name) THEN 
      $IF dbms_db_version.version < 12 $THEN
      g_params_rec := in_params_rec;
      l_sql := '
      declare
        in_param_rec '||gc_package_name||'.gt_params_rec := '||gc_package_name||'.get_params_rec;
      begin 
        :result := in_param_rec.'||in_param_name||'; 
      end;';
      EXECUTE IMMEDIATE l_sql USING OUT l_result;
      $ELSE
      l_sql := '
      declare
        in_param_rec '||gc_package_name||'.gt_params_rec := :in_param_rec;
      begin 
        :result := in_param_rec.'||in_param_name||'; 
      end;';
      EXECUTE IMMEDIATE l_sql USING IN in_params_rec, OUT l_result;
      $END
    ELSE 
      raise_application_error(-20000, 'Invalid param name: '||in_param_name); 
    END IF;       
    RETURN l_result;
  END get_param;

  FUNCTION get_param_value(in_params_rec IN gt_params_rec, in_param_name IN VARCHAR2) RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_param(in_params_rec, in_param_name).get_value();
  END get_param_value;

  PROCEDURE set_param_value(io_params_rec IN OUT NOCOPY gt_params_rec, in_param_name IN VARCHAR2, in_param_value IN VARCHAR2)
  AS
    l_sql    varchar2(1000);
  BEGIN
    IF param_exists(in_param_name) THEN 
      $IF dbms_db_version.version < 12 $THEN
      g_params_rec := io_params_rec;
      l_sql := '
      declare
        io_param_rec '||gc_package_name||'.gt_params_rec := '||gc_package_name||'.get_params_rec;
      begin 
        io_param_rec.'||in_param_name||'.set_value(:in_value); 
        '||gc_package_name||'.set_param_rec(io_param_rec);
      end;';
      EXECUTE IMMEDIATE l_sql USING in_param_value;
      io_params_rec := g_params_rec;      
      $ELSE
      l_sql := '
      declare
        io_param_rec '||gc_package_name||'.gt_params_rec := :in_param_rec;
      begin 
        io_param_rec.'||in_param_name||'.set_value(:in_value);
        :out_param_rec := io_param_rec;  
      end;';  
      EXECUTE IMMEDIATE l_sql USING IN io_params_rec, IN in_param_value, OUT io_params_rec;
      $END
    ELSE     
      raise_application_error(-20000, 'Invalid param name: '||in_param_name); 
    END IF;       
  END set_param_value;

  FUNCTION get_run_param(in_params_rec IN gt_run_params_rec, in_param_name IN VARCHAR2) RETURN cort_param_obj
  AS
    l_result cort_param_obj;
    l_sql    varchar2(1000);
  BEGIN
    IF run_param_exists(in_param_name) THEN 
      $IF dbms_db_version.version < 12 $THEN
      g_run_params_rec := in_params_rec;
      l_sql := '
      declare
        in_param_rec '||gc_package_name||'.gt_run_params_rec := '||gc_package_name||'.get_run_params_rec;
      begin 
        :result := in_param_rec.'||in_param_name||'; 
      end;';
      EXECUTE IMMEDIATE l_sql USING OUT l_result;
      $ELSE
      l_sql := '
      declare
        in_param_rec '||gc_package_name||'.gt_run_params_rec := :in_param_rec;
      begin 
        :result := in_param_rec.'||in_param_name||'; 
      end;';
      EXECUTE IMMEDIATE l_sql USING IN in_params_rec, OUT l_result;
      $END
    ELSE 
      raise_application_error(-20000, 'Invalid param name: '||in_param_name); 
    END IF;       
    RETURN l_result;
  END get_run_param;

  FUNCTION get_run_param_value(in_params_rec IN gt_run_params_rec, in_param_name IN VARCHAR2) RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_run_param(in_params_rec, in_param_name).get_value();
  END get_run_param_value;

  PROCEDURE set_run_param_value(io_params_rec IN OUT NOCOPY gt_run_params_rec, in_param_name IN VARCHAR2, in_param_value IN VARCHAR2)
  AS
    l_sql    varchar2(1000);
  BEGIN
    IF run_param_exists(in_param_name) THEN
      $IF dbms_db_version.version < 12 $THEN
      g_run_params_rec := io_params_rec;
      l_sql := '
      declare
        io_param_rec '||gc_package_name||'.gt_run_params_rec := '||gc_package_name||'.get_run_params_rec;
      begin 
        io_param_rec.'||in_param_name||'.set_value(:in_value); 
        '||gc_package_name||'.set_run_params_rec(io_param_rec);
      end;';
      EXECUTE IMMEDIATE l_sql USING in_param_value;
      io_params_rec := g_run_params_rec;      
      $ELSE
      l_sql := '
      declare
        io_param_rec '||gc_package_name||'.gt_run_params_rec := :in_param_rec;
      begin 
        io_param_rec.'||in_param_name||'.set_value(:in_value);
        :out_param_rec := io_param_rec;  
      end;';  
      EXECUTE IMMEDIATE l_sql USING IN io_params_rec, IN in_param_value, OUT io_params_rec;
      $END
    ELSE 
      raise_application_error(-20000, 'Invalid param name: '||in_param_name); 
    END IF;       
  END set_run_param_value;
 

  -- reset all params to default values
  PROCEDURE reset_params(
    io_params_rec IN OUT NOCOPY gt_params_rec
  )
  AS
    l_default_params_rec      gt_params_rec;
    l_param_arr               arrays.gt_str_arr; 
    l_default_arr             arrays.gt_lstr_arr;
  BEGIN
    io_params_rec := l_default_params_rec;
    SELECT param_name, default_value
      BULK COLLECT 
      INTO l_param_arr, l_default_arr 
      FROM cort_user_params;
     
    FOR i IN 1..l_param_arr.COUNT LOOP
      IF param_exists(l_param_arr(i)) THEN
        set_param_value(io_params_rec, l_param_arr(i), l_default_arr(i));
      END IF;  
    END LOOP;
  END reset_params;

  -- reset all params to default values
  PROCEDURE reset_run_params(
    io_params_rec IN OUT NOCOPY gt_run_params_rec
  )
  AS
    l_default_params_rec      gt_run_params_rec;
    l_param_arr               arrays.gt_str_arr; 
    l_default_arr             arrays.gt_lstr_arr;
  BEGIN
    io_params_rec := l_default_params_rec;
    SELECT param_name, default_value
      BULK COLLECT 
      INTO l_param_arr, l_default_arr 
      FROM cort_user_params;
     
    FOR i IN 1..l_param_arr.COUNT LOOP
      IF run_param_exists(l_param_arr(i)) THEN
        set_run_param_value(io_params_rec, l_param_arr(i), l_default_arr(i));
      END IF;  
    END LOOP;
  END reset_run_params;


  -- return arrays of param names
  FUNCTION get_param_names 
  RETURN arrays.gt_str_arr
  AS
  BEGIN
    RETURN g_param_names_arr;
  END get_param_names;  

  -- return arrays of param names
  FUNCTION get_run_param_names 
  RETURN arrays.gt_str_arr
  AS
  BEGIN
    RETURN g_run_param_names_arr;
  END get_run_param_names;  

  FUNCTION param_exists(in_param_name IN VARCHAR2) 
  RETURN BOOLEAN
  AS
  BEGIN
    RETURN g_param_names_indx.EXISTS(upper(in_param_name));
  END param_exists ;  

  FUNCTION run_param_exists(in_param_name IN VARCHAR2) 
  RETURN BOOLEAN
  AS
  BEGIN
    RETURN g_run_param_names_indx.EXISTS(upper(in_param_name));
  END run_param_exists ;  


  FUNCTION write_to_xml(
    in_params_rec IN gt_params_rec 
  )
  RETURN CLOB
  AS
    l_result CLOB;
  BEGIN
    l_result := '<PARAMS>'||chr(10);
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      l_result := l_result||get_param(in_params_rec, g_param_names_arr(i)).get_xml_value||chr(10);
    END LOOP;
    l_result := l_result||'</PARAMS>';
    RETURN l_result;
  END write_to_xml;

  FUNCTION write_to_xml(
    in_params_rec IN gt_run_params_rec 
  )
  RETURN CLOB
  AS
    l_result CLOB;
  BEGIN
    l_result := '<PARAMS>'||chr(10);
    FOR i IN 1..g_run_param_names_arr.COUNT LOOP
      l_result := l_result||get_run_param(in_params_rec, g_run_param_names_arr(i)).get_xml_value||chr(10);
    END LOOP;
    l_result := l_result||'</PARAMS>';
    RETURN l_result;
  END write_to_xml;


  PROCEDURE read_from_xml(
    io_params_rec IN OUT NOCOPY gt_params_rec,
    in_xml        IN XMLType
  )
  AS
    l_domdoc           dbms_xmldom.DOMDocument;
    l_domnode          dbms_xmldom.DOMNode;
    l_node             xml_utils.gt_node_rec;
    l_nodes            xml_utils.gt_nodes_rec;
  BEGIN
    l_domdoc := dbms_xmldom.newDomDocument(in_xml);
    BEGIN
      l_domnode := dbms_xmldom.makeNode(l_domdoc);
      l_node := xml_utils.get_node(l_domnode);
      l_nodes := xml_utils.get_children_nodes(l_node);
      -- ret root node (PARAMS)
      l_node := l_nodes.children_nodes(1);
      -- get params nodes
      l_nodes := xml_utils.get_children_nodes(l_node);
      FOR i IN 1..l_nodes.children_nodes.COUNT LOOP
        l_node := l_nodes.children_nodes(i);
        -- cort_log_pkg.debug('read param '||l_node.name||' from XML = ['||l_node.value||']');
        IF param_exists(l_node.name) THEN
          set_param_value(io_params_rec, l_node.name, cast(l_node.value as varchar2));
        END IF;
      END LOOP;  
    EXCEPTION
      WHEN OTHERS THEN
        dbms_xmldom.freeDocument(l_domdoc);
        RAISE;
    END;
    dbms_xmldom.freeDocument(l_domdoc);
  END read_from_xml;

  PROCEDURE read_from_xml(
    io_params_rec IN OUT NOCOPY gt_run_params_rec,
    in_xml        IN XMLType
  )
  AS
    l_domdoc           dbms_xmldom.DOMDocument;
    l_domnode          dbms_xmldom.DOMNode;
    l_node             xml_utils.gt_node_rec;
    l_nodes            xml_utils.gt_nodes_rec;
  BEGIN
    l_domdoc := dbms_xmldom.newDomDocument(in_xml);
    BEGIN
      l_domnode := dbms_xmldom.makeNode(l_domdoc);
      l_node := xml_utils.get_node(l_domnode);
      l_nodes := xml_utils.get_children_nodes(l_node);
      -- ret root node (PARAMS)
      l_node := l_nodes.children_nodes(1);
      -- get params nodes
      l_nodes := xml_utils.get_children_nodes(l_node);
      FOR i IN 1..l_nodes.children_nodes.COUNT LOOP
        l_node := l_nodes.children_nodes(i);
        -- cort_log_pkg.debug('read param '||l_node.name||' from XML = ['||l_node.value||']');
        IF run_param_exists(l_node.name) THEN
          set_run_param_value(io_params_rec, l_node.name, cast(l_node.value as varchar2));
        END IF;
      END LOOP;  
    EXCEPTION
      WHEN OTHERS THEN
        dbms_xmldom.freeDocument(l_domdoc);
        RAISE;
    END;
    dbms_xmldom.freeDocument(l_domdoc);
  END read_from_xml;

  PROCEDURE print_params(in_params_rec IN gt_params_rec)
  AS
  BEGIN
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      dbms_output.put_line(g_param_names_arr(i)||' = '||get_param_value(in_params_rec, g_param_names_arr(i)));
    END LOOP;
  END print_params;

  PROCEDURE print_params(in_params_rec IN gt_run_params_rec)
  AS
  BEGIN
    FOR i IN 1..g_run_param_names_arr.COUNT LOOP
      dbms_output.put_line(g_run_param_names_arr(i)||' = '||get_run_param_value(in_params_rec, g_run_param_names_arr(i)));
    END LOOP;
  END print_params;

BEGIN
  init_session_variables;
END cort_params_pkg;
/

