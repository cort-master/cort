CREATE OR REPLACE PACKAGE BODY cort_params_pkg 
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
  Description: Type and API for main application parameters
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added API to read/write param default values
  15.00   | Rustam Kafarov    | Added IGNORE_ORDER param
  18.03   | Rustam Kafarov    | Added STATS param instead of keep_stats
  19.00   | Rustam Kafarov    | Used SQL type to work with parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- global variable for passing record into dynamic SQL
  gc_package_name          VARCHAR2(30) := $$plsql_unit; 

  -- for temp internal use only
  g_params_rec             gt_params_rec;
  g_params_arr             gt_params_arr; 

  -- metadata variables 
  g_param_names_arr        arrays.gt_str_arr;
  g_param_names_indx       arrays.gt_str_indx;
  g_rec_to_array_sql       clob;
  g_array_to_rec_sql       clob;


  -- load into array names and types of CORT parameters record
  PROCEDURE init_session_variables
  AS
  BEGIN
    g_param_names_arr.DELETE;
    g_param_names_indx.DELETE;
    
    -- init only once
    SELECT argument_name
      BULK COLLECT 
      INTO g_param_names_arr  
      FROM all_arguments
     WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER') 
       AND package_name = gc_package_name   
       AND object_name = 'GET_PARAMS_REC'
       AND data_level = 1;
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      g_param_names_indx(g_param_names_arr(i)) := g_param_names_arr(i);
    END LOOP;  
    
    g_rec_to_array_sql := '
    DECLARE
      l_rec    '||gc_package_name||'.gt_params_rec;
      l_arr    '||gc_package_name||'.gt_params_arr;
    BEGIN
      l_rec := '||gc_package_name||'.get_params_rec;
      ';
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      g_rec_to_array_sql := g_rec_to_array_sql||'l_arr('''||g_param_names_arr(i)||''') := l_rec.'||g_param_names_arr(i)||';
      ';
    END LOOP;    
    g_rec_to_array_sql := g_rec_to_array_sql||'
      '||gc_package_name||'.set_params_arr(l_arr);
    END;';
    
    g_array_to_rec_sql := '
    DECLARE
      l_rec    '||gc_package_name||'.gt_params_rec;
      l_arr    '||gc_package_name||'.gt_params_arr;
    BEGIN
      l_arr := '||gc_package_name||'.get_params_arr;
      ';
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      g_array_to_rec_sql := g_array_to_rec_sql||'l_rec.'||g_param_names_arr(i)||' := l_arr('''||g_param_names_arr(i)||''');
      ';
    END LOOP;    
    g_array_to_rec_sql := g_array_to_rec_sql||'
      '||gc_package_name||'.set_params_rec(l_rec);
    END;';

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
  
  -- setter for dynamic SQL - for intenal use only!!!
  PROCEDURE set_params_rec(in_params_rec IN gt_params_rec)
  AS
  BEGIN
    g_params_rec := in_params_rec;
  END set_params_rec;
  
  -- getter for dynamic SQL. It is also used to get dynamicly definition of PL/SQL recrod - for intenal use only!!! 
  FUNCTION get_params_rec
  RETURN gt_params_rec
  AS
  BEGIN
    RETURN g_params_rec;
  END get_params_rec;
  
  PROCEDURE set_params_arr(in_params_arr IN gt_params_arr)
  AS
  BEGIN
    g_params_arr := in_params_arr;
  END set_params_arr;
  
  FUNCTION get_params_arr
  RETURN gt_params_arr
  AS
  BEGIN
    RETURN g_params_arr;
  END get_params_arr;

  FUNCTION init(in_line IN NUMBER, in_default IN VARCHAR, in_regexp in VARCHAR2, in_case_sensitive IN BOOLEAN DEFAULT FALSE) RETURN cort_param_obj
  AS
    l_param  cort_param_obj;
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), in_default, in_regexp, in_case_sensitive);
  END init;
  
  FUNCTION init(in_line IN NUMBER, in_default IN BOOLEAN) RETURN cort_param_obj
  AS
    l_param  cort_param_obj;
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), in_default);
  END init;
  
  FUNCTION init(in_line IN NUMBER, in_default IN NUMBER, in_regexp in VARCHAR2) RETURN cort_param_obj
  AS
    l_param  cort_param_obj;
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), in_default, in_regexp);
  END init;

  FUNCTION init(in_line IN NUMBER, in_default IN ARRAY, in_regexp in VARCHAR2) RETURN cort_param_obj
  AS
    l_param  cort_param_obj;
  BEGIN
    RETURN cort_param_obj.init(get_name(in_line), in_default, in_regexp);
  END init;

  FUNCTION get_param(
    in_params_rec IN gt_params_rec,
    in_param_name IN VARCHAR2
  )
  RETURN cort_param_obj
  AS
    l_result cort_param_obj; 
    l_sql    VARCHAR2(32767);
  BEGIN
    IF g_param_names_indx.EXISTS(UPPER(in_param_name)) THEN
      g_params_rec := in_params_rec;
      
      l_sql := '
      DECLARE
        l_rec    '||gc_package_name||'.gt_params_rec;
      BEGIN
        l_rec := '||gc_package_name||'.get_params_rec;
        :out_result := l_rec.'||in_param_name||';
      END;';
      
      EXECUTE IMMEDIATE l_sql USING OUT l_result;
      
      RETURN l_result;
    ELSE
      cort_exec_pkg.raise_error('Invalid param name - '||in_param_name, -20001);
    END IF;
  END get_param;

  -- return single parameter value by name 
  FUNCTION get_param_value(
    in_params_rec IN gt_params_rec,
    in_param_name IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(4000); 
    l_sql    VARCHAR2(32767);
  BEGIN
    IF g_param_names_indx.EXISTS(UPPER(in_param_name)) THEN
      g_params_rec := in_params_rec;
      
      l_sql := '
      DECLARE
        l_rec    '||gc_package_name||'.gt_params_rec;
      BEGIN
        l_rec := '||gc_package_name||'.get_params_rec;
        :out_result := l_rec.'||in_param_name||'.get_value;
      END;';
      
      EXECUTE IMMEDIATE l_sql USING OUT l_result;
      
      RETURN l_result;
    ELSE
      cort_exec_pkg.raise_error('Invalid param name - '||in_param_name, -20001);
    END IF;
  END get_param_value;
  
  -- set single parameter value by name 
  PROCEDURE set_param_value(
    io_params_rec  IN OUT NOCOPY gt_params_rec,
    in_param_name  IN VARCHAR2,
    in_param_value IN VARCHAR2
  )
  AS
    l_sql    VARCHAR2(32767);
  BEGIN
    IF g_param_names_indx.EXISTS(UPPER(in_param_name)) THEN

      g_params_rec := io_params_rec;

      IF LENGTH(in_param_value) > 4000 THEN
        cort_exec_pkg.raise_error('Param '||in_param_name||' value is too long', -20001);
      END IF;
      -- generic processing
      
      l_sql := '
      DECLARE
        l_rec    '||gc_package_name||'.gt_params_rec;
      BEGIN
        l_rec := '||gc_package_name||'.get_params_rec;
        l_rec.'||in_param_name||'.set_value(in_value => :in_value);
        '||gc_package_name||'.set_params_rec(l_rec);
      END;';
      
      EXECUTE IMMEDIATE l_sql USING IN in_param_value;

      io_params_rec := g_params_rec;
    ELSE
      cort_exec_pkg.raise_error('Invalid param name : '||in_param_name, -20001);
    END IF;
  END set_param_value;
  
  -- return arrays of param names
  FUNCTION get_param_names 
  RETURN arrays.gt_str_arr
  AS
  BEGIN
    RETURN g_param_names_arr;
  END get_param_names;  

  FUNCTION get_param_names_indx 
  RETURN arrays.gt_str_indx
  AS
  BEGIN
    RETURN g_param_names_indx;
  END get_param_names_indx;  

  FUNCTION rec_to_array(in_params_rec IN gt_params_rec)
  RETURN gt_params_arr
  AS
  BEGIN
    g_params_rec := in_params_rec;

    EXECUTE IMMEDIATE g_rec_to_array_sql;
    
    RETURN g_params_arr;
  END rec_to_array;

  FUNCTION array_to_rec(in_params_arr IN gt_params_arr)
  RETURN gt_params_rec
  AS
  BEGIN
    g_params_arr := in_params_arr;

    EXECUTE IMMEDIATE g_array_to_rec_sql;
    
    RETURN g_params_rec;
  END array_to_rec;

  FUNCTION array_to_xml(
    in_params_arr IN gt_params_arr
  )
  RETURN CLOB
  AS
    l_result CLOB;
  BEGIN
    l_result := '<PARAMS>'||chr(10);
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      l_result := l_result||'<'||g_param_names_arr(i)||'>'||dbms_xmlgen.convert(in_params_arr(g_param_names_arr(i)).get_value, dbms_xmlgen.entity_encode)||'</'||g_param_names_arr(i)||'>'||chr(10);
    END LOOP;
    l_result := l_result||'</PARAMS>';
    RETURN l_result;
  END array_to_xml;

  FUNCTION rec_to_xml(
    in_params_rec IN gt_params_rec 
  )
  RETURN CLOB
  AS
  BEGIN
    RETURN array_to_xml(rec_to_array(in_params_rec));
  END rec_to_xml;

  PROCEDURE read_from_xml(
    in_xml        IN XMLType,
    io_params_arr IN OUT NOCOPY gt_params_arr 
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
        IF io_params_arr.exists(l_node.name) THEN
          io_params_arr(l_node.name).set_value(cast(l_node.value as varchar2));
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
    in_xml        IN XMLType,
    io_params_rec IN OUT NOCOPY gt_params_rec
  )
  AS
    l_params_arr  gt_params_arr;
  BEGIN
    l_params_arr := rec_to_array(io_params_rec);
    read_from_xml(in_xml, l_params_arr);
    io_params_rec := array_to_rec(l_params_arr);
  END read_from_xml;

  PROCEDURE print_params(in_params_rec IN gt_params_rec)
  AS
    l_params_arr  gt_params_arr;
  BEGIN
    l_params_arr := rec_to_array(in_params_rec);
    FOR i IN 1..g_param_names_arr.COUNT LOOP
      dbms_output.put_line(g_param_names_arr(i)||' = '||l_params_arr(g_param_names_arr(i)).get_value);
    END LOOP;
  END print_params;

BEGIN
  init_session_variables;
END cort_params_pkg;
/
