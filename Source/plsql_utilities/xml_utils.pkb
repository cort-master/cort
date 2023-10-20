CREATE OR REPLACE PACKAGE BODY xml_utils 
AS

/*
PL/SQL Utilities - export/import PL/SQL Data Types to/from XML structure 

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
  Description: Export/import data from any custom PL/SQL data type to/from XML structure.     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Increased length for l_index_type in get_field_value_sql
  17.00   | Rustam Kafarov    | Added support for big CLOB xml nodes
  21.00   | Rustam Kafarov    | Added support for Oracle versions 18, 19. Added support for intervals 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  CURSOR g_arg_cur(                                   
    in_package_owner  IN VARCHAR,
    in_package_name   IN VARCHAR,
    in_procedure_name IN VARCHAR2
  )  
  RETURN gt_type_desc_rec
  IS  
  $IF dbms_db_version.version >=18 $THEN
  SELECT ROWNUM AS sequence,
         LEVEL-1 AS data_level,
         argument_name,
         position,
         data_type,
         sub_type_owner AS type_owner,
         sub_type_package AS type_name,
         sub_type_name AS type_subname,
         CASE
           WHEN pls_type LIKE 'PL/SQL %' THEN REPLACE (REPLACE(pls_type, 'PL/SQL ', NULL), ' ', '_')
           ELSE pls_type
         END AS pls_type,
         NULL AS full_name
    FROM (SELECT pta.owner,
                 pta.package_name,
                 pta.type_name,
                 attr_name AS argument_name,
                 attr_no AS position,
                 CASE
                   WHEN attr_type_owner IS NULL THEN attr_type_name
                   WHEN attr_type_owner = 'PUBLIC' AND attr_type_name in ('XMLTYPE','ANYDATA') THEN 'OPAQUE/XMLTYPE'
                   WHEN plt.typecode = 'PL/SQL RECORD' THEN 'PL/SQL RECORD'
                   WHEN plt.typecode = 'COLLECTION' THEN 'PL/SQL TABLE'
                 END data_type,
                 attr_type_owner as sub_type_owner,
                 attr_type_package as sub_type_package,
                 CASE WHEN attr_type_owner IS NOT NULL THEN attr_type_name END sub_type_name,
                 CASE WHEN attr_type_owner IS NULL THEN attr_type_name END pls_type
            FROM all_plsql_type_attrs pta
            LEFT JOIN all_plsql_types plt
              ON plt.owner = pta.attr_type_owner
             AND plt.package_name = pta.attr_type_package
             AND plt.type_name = pta.attr_type_name
           UNION ALL
          SELECT pct.owner,
                 pct.package_name,
                 pct.type_name,
                 NULL AS argument_name,
                 1 AS position,
                 CASE
                   WHEN elem_type_owner IS NULL THEN elem_type_name
                   WHEN elem_type_owner = 'PUBLIC' AND elem_type_name in ('XMLTYPE','ANYDATA') THEN 'OPAQUE/XMLTYPE'
                   WHEN plt.typecode = 'PL/SQL RECORD' THEN 'PL/SQL RECORD'
                   WHEN plt.typecode = 'COLLECTION' THEN 'PL/SQL TABLE'
                 END data_type,
                 elem_type_owner as sub_type_owner,
                 elem_type_package as sub_type_package,
                 CASE WHEN elem_type_owner IS NOT NULL THEN elem_type_name END sub_type_name,
                 CASE WHEN elem_type_owner IS NULL THEN elem_type_name END pls_type
            FROM all_plsql_coll_types pct
            LEFT JOIN all_plsql_types plt
              ON plt.owner = pct.elem_type_owner
             AND plt.package_name = pct.elem_type_package
             AND plt.type_name = pct.elem_type_name
           UNION ALL
          SELECT NULL as owner,
                 NULL as package_name,
                 NULL as type_name,
                 NULL as argument_name,
                 0 as position,
                 arg.data_type,
                 arg.type_owner,
                 arg.type_name,
                 arg.type_subname,
                 arg.pls_type
            FROM all_arguments arg
           WHERE arg.owner = in_package_owner
             AND arg.package_name = in_package_name
             AND arg.object_name = in_procedure_name
             AND overload IS NULL
             AND data_level = 0
          ) a
   START WITH position = 0
          AND owner IS NULL
          AND package_name IS NULL
          AND type_name IS NULL
   CONNECT BY PRIOR sub_type_owner = a.owner
          AND PRIOR sub_type_package = a.package_name
          AND PRIOR sub_type_name = a.type_name
    ORDER SIBLINGS BY a.position
  $ELSE  
  SELECT arg.sequence
       , arg.data_level
       , arg.argument_name
       , arg.position
       , CASE 
           WHEN arg.data_type = 'TABLE' THEN 'PL/SQL TABLE'
           WHEN arg.type_owner = 'PUBLIC' AND arg.type_name in ('XMLTYPE','ANYDATA') THEN 'OPAQUE/XMLTYPE' 
           ELSE arg.data_type 
         END as data_type 
       , arg.type_owner
       , arg.type_name
       , arg.type_subname
       , arg.pls_type
       , NULL AS full_name 
    FROM all_arguments arg
   WHERE arg.owner = in_package_owner 
     AND arg.package_name = in_package_name
     AND arg.object_name = in_procedure_name
     AND overload IS NULL
   ORDER BY arg.sequence  
  $END 
  ;
  
  g_last_name_arr   arrays.gt_xlstr_arr;              
  g_last_seq_arr    arrays.gt_str_arr;              
  g_indent          VARCHAR2(100);
  $IF dbms_db_version.version >= 12 $THEN
  gc_schema         CONSTANT arrays.gt_name := $$PLSQL_UNIT_OWNER;
  $ELSE
  gc_schema         CONSTANT arrays.gt_name := SYS_CONTEXT('USERENV','CURRENT_USER');
  $END  
  
  -- Private declarations
  FUNCTION quote_name(in_name IN VARCHAR2)
  RETURN VARCHAR2
  AS
    l_name VARCHAR2(100);
  BEGIN
    IF in_name LIKE '"%"' THEN
      l_name := in_name;
    $IF arrays.gc_long_name_supported $THEN
    ELSIF REGEXP_LIKE(in_name, '^[A-Z][A-Z0-9_$#]{0,127}$') THEN
    $ELSE
    ELSIF REGEXP_LIKE(in_name, '^[A-Z][A-Z0-9_$#]{0,29}$') THEN
    $END
      l_name  := in_name;
    ELSIF in_name IS NULL THEN 
      l_name := NULL;
    ELSE
      l_name := '"'||in_name||'"';
    END IF;
    RETURN l_name;
  END quote_name;
  
  -- Return full PL/SQL type name
  FUNCTION get_type_full_name(
   in_pls_type      IN VARCHAR2,
   in_type_owner    IN VARCHAR2,
   in_type_name     IN VARCHAR2,
   in_type_subname  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(4000);
  BEGIN
    IF in_pls_type IS NOT NULL THEN
      l_result := quote_name(in_pls_type);
    ELSE
      IF in_type_owner IS NOT NULL THEN
        l_result := quote_name(in_type_owner); 
      END IF;
      IF in_type_name IS NOT NULL THEN
        IF l_result IS NOT NULL THEN
          l_result := l_result ||'.'||quote_name(in_type_name); 
        ELSE
          l_result := quote_name(in_type_name); 
        END IF;
      END IF;    
      IF in_type_subname IS NOT NULL THEN
        IF l_result IS NOT NULL THEN
          l_result := l_result ||'.'||quote_name(in_type_subname); 
        ELSE
          l_result := quote_name(in_type_subname); 
        END IF;
      END IF;
    END IF;
    RETURN l_result;    
  END get_type_full_name;

  -- get sql for individual filed
  FUNCTION get_field_value_sql( 
    in_arg_rec     IN gt_type_desc_rec
  )
  RETURN VARCHAR2
  AS          
    l_sql        VARCHAR2(32767);
    l_value      VARCHAR2(4000) := 'NULL';
    l_index_type VARCHAR2(1000);
  BEGIN               
    IF in_arg_rec.data_level <= g_last_seq_arr.LAST THEN
      l_sql := l_sql||g_indent|| 'l_indx'||g_last_seq_arr(g_last_seq_arr.LAST)||' := '||g_last_name_arr(g_last_name_arr.LAST)||'.NEXT(l_indx'||g_last_seq_arr(g_last_seq_arr.LAST)||');';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'END LOOP;';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'END;';
      g_last_seq_arr.DELETE(g_last_seq_arr.LAST);
      g_last_name_arr.DELETE(g_last_name_arr.LAST);
    END IF;
    CASE in_arg_rec.data_type 
    WHEN 'PL/SQL TABLE' THEN
      l_sql := l_sql||g_indent||'DECLARE';
      l_sql := l_sql||g_indent||'  l_indx'||in_arg_rec.sequence||' VARCHAR2(255);';
      l_sql := l_sql||g_indent||'BEGIN';
      g_indent := g_indent||'  ';
      l_value := 'NULL';
      l_index_type := gc_schema||'.xml_utils.get_type_name('||in_arg_rec.full_name||'.FIRST)';
      g_last_seq_arr(in_arg_rec.data_level) := in_arg_rec.sequence;
      g_last_name_arr(in_arg_rec.data_level) := in_arg_rec.full_name;
    WHEN 'PL/SQL RECORD' THEN
      l_value := 'NULL';
      l_index_type := 'NULL';
    ELSE
      l_index_type := 'NULL';
      CASE
      WHEN in_arg_rec.pls_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'CLOB', 'NCLOB', 'LONG') THEN
        l_value := in_arg_rec.full_name;
      WHEN in_arg_rec.pls_type IN ('NUMBER', 'PLS_INTEGER', 'BINARY_INTEGER') THEN
        l_value := 'TO_CHAR('||in_arg_rec.full_name||')';
      WHEN in_arg_rec.pls_type = 'DATE' THEN
        l_value := 'TO_CHAR('||in_arg_rec.full_name||', ''YYYY-MM-DD HH24:MI:SS'')';
      WHEN in_arg_rec.pls_type LIKE 'TIMESTAMP(%)' THEN
        l_value := 'TO_CHAR('||in_arg_rec.full_name||', ''YYYY-MM-DD HH24:MI:SS.FF9'')';
      WHEN in_arg_rec.pls_type = 'BOOLEAN' THEN
        l_value := 'CASE WHEN '||in_arg_rec.full_name||' = TRUE THEN ''TRUE'' WHEN '||in_arg_rec.full_name||' = FALSE THEN ''FALSE'' ELSE ''NULL'' END';
      WHEN in_arg_rec.pls_type LIKE 'INTERVAL DAY TO SECOND' THEN
        l_value := 'TO_CHAR('||in_arg_rec.full_name||')';
      WHEN in_arg_rec.pls_type LIKE 'INTERVAL YEAR TO DAY' THEN
        l_value := 'TO_CHAR('||in_arg_rec.full_name||')';
      ELSE  
        l_value := 'NULL';
      END CASE;
    END CASE; 
    l_sql := l_sql||g_indent||'l_node := '||gc_schema||'.xml_utils.get_field_value(';
    l_sql := l_sql||g_indent||'            in_dom_doc     => l_domdoc,'; 
    l_sql := l_sql||g_indent||'            io_parent_node => l_parent_node_arr('||in_arg_rec.data_level||'),';
    l_sql := l_sql||g_indent||'            in_name        => '''||in_arg_rec.argument_name||''',';
    IF in_arg_rec.argument_name IS NULL AND g_last_seq_arr.LAST IS NOT NULL 
    THEN
      l_sql := l_sql||g_indent||'            in_index       => '||'l_indx'||g_last_seq_arr(g_last_seq_arr.LAST)||',';
    ELSE
      l_sql := l_sql||g_indent||'            in_index       => NULL,';
    END IF;  
    l_sql := l_sql||g_indent||'            in_index_type  => '||l_index_type||',';
    l_sql := l_sql||g_indent||'            in_data_type   => '''||in_arg_rec.data_type||''',';
    l_sql := l_sql||g_indent||'            in_pls_type    => '''||get_type_full_name(in_arg_rec.pls_type, in_arg_rec.type_owner, in_arg_rec.type_name, in_arg_rec.type_subname)||''',';
    l_sql := l_sql||g_indent||'            in_value       => '||l_value;
    l_sql := l_sql||g_indent||'          );';
    l_sql := l_sql||g_indent||'l_parent_node_arr('||TO_CHAR(in_arg_rec.data_level + 1)||') := l_node;';
    IF in_arg_rec.data_type = 'PL/SQL TABLE' THEN
      l_sql := l_sql||g_indent||'l_indx'||in_arg_rec.sequence||' := '||in_arg_rec.full_name||'.FIRST;';
      l_sql := l_sql||g_indent||'WHILE l_indx'||in_arg_rec.sequence||' IS NOT NULL LOOP';                              
      g_indent := g_indent||'  ';
    END IF;  
    RETURN l_sql;    
  END get_field_value_sql; 
                        
                        
  -- get sql for individual filed to assign value
  FUNCTION get_xml_value_sql( 
    in_arg_rec     IN gt_type_desc_rec
  )
  RETURN VARCHAR2
  AS          
    l_sql        VARCHAR2(32767);
  BEGIN               
    IF in_arg_rec.data_level <= g_last_seq_arr.LAST THEN
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'END;';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'END LOOP;';
      g_last_seq_arr.DELETE(g_last_seq_arr.LAST);
    END IF;
    IF in_arg_rec.argument_name IS NOT NULL THEN
      l_sql := l_sql||g_indent||'l_node := xml_utils.find_by_name(l_nodes('||TO_CHAR(in_arg_rec.data_level-1)||'), '''||in_arg_rec.argument_name||''');';
    ELSE
      l_sql := l_sql||g_indent||'l_node := l_nodes('||TO_CHAR(in_arg_rec.data_level-1)||').children_nodes(i'||g_last_seq_arr(in_arg_rec.data_level-1)||');';
      l_sql := l_sql||g_indent||'l_indx'||g_last_seq_arr(in_arg_rec.data_level-1)||' := l_node.indx;';
    END IF;
    CASE in_arg_rec.data_type 
    WHEN 'PL/SQL TABLE' THEN
      l_sql := l_sql||g_indent||'l_nodes('||TO_CHAR(in_arg_rec.data_level)||') := xml_utils.get_children_nodes(l_node);';
      l_sql := l_sql||g_indent||'FOR i'||in_arg_rec.sequence||' IN 1..l_nodes('||TO_CHAR(in_arg_rec.data_level)||').children_nodes.count LOOP'; 
      g_indent := g_indent||'  ';
      l_sql := l_sql||g_indent||'DECLARE';
      l_sql := l_sql||g_indent||'  l_indx'||in_arg_rec.sequence||' VARCHAR2(255);';
      l_sql := l_sql||g_indent||'BEGIN';
      g_indent := g_indent||'  ';
      g_last_seq_arr(in_arg_rec.data_level) := in_arg_rec.sequence;
    WHEN 'PL/SQL RECORD' THEN
      l_sql := l_sql||g_indent||'l_nodes('||in_arg_rec.data_level||') := xml_utils.get_children_nodes(l_node);';
    ELSE
      CASE
      WHEN in_arg_rec.pls_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'CLOB', 'NCLOB', 'LONG') THEN
        l_sql := l_sql||g_indent||in_arg_rec.full_name ||' := xml_utils.get_str_value(l_node);';
      WHEN in_arg_rec.pls_type IN ('NUMBER', 'PLS_INTEGER', 'BINARY_INTEGER') THEN
        l_sql := l_sql||g_indent||in_arg_rec.full_name||' := xml_utils.get_num_value(l_node);';
      WHEN in_arg_rec.pls_type = 'DATE' THEN
        l_sql := l_sql||g_indent||in_arg_rec.full_name||' := xml_utils.get_date_value(l_node);';
      WHEN in_arg_rec.pls_type LIKE 'TIMESTAMP(%)' THEN
        l_sql := l_sql||g_indent||in_arg_rec.full_name||' := xml_utils.get_time_value(l_node);';
      WHEN in_arg_rec.pls_type = 'BOOLEAN' THEN
        l_sql := l_sql||g_indent||in_arg_rec.full_name||' := xml_utils.get_bool_value(l_node);';
      WHEN in_arg_rec.pls_type LIKE 'INTERVAL DAY TO SECOND' THEN
        l_sql := l_sql||g_indent||in_arg_rec.full_name||' := xml_utils.get_dsinterval_value(l_node);';
      WHEN in_arg_rec.pls_type LIKE 'INTERVAL YEAR TO MONTH' THEN
        l_sql := l_sql||g_indent||in_arg_rec.full_name||' := xml_utils.get_yminterval_value(l_node);';
      ELSE  
        NULL;
      END CASE;
    END CASE; 
    RETURN l_sql;    
  END get_xml_value_sql; 

  PROCEDURE read_attributes(
    in_node       IN dbms_xmldom.DOMNode,
    io_node_rec   IN OUT NOCOPY gt_node_rec 
  )
  AS
    l_attributes dbms_xmldom.DOMNamedNodeMap;
    l_attr       dbms_xmldom.DOMNode;
    l_attr_cnt   PLS_INTEGER;
    l_attr_name  VARCHAR2(4000);
    l_attr_value VARCHAR2(4000);
  BEGIN
    l_attributes := dbms_xmldom.getAttributes(in_node);
    l_attr_cnt := dbms_xmldom.getLength(l_attributes);
    FOR i IN 0..l_attr_cnt-1 LOOP
      l_attr := dbms_xmldom.item(l_attributes, i);
      l_attr_name := dbms_xmldom.getNodeName(l_attr);
      l_attr_value := dbms_xmldom.getNodeValue(l_attr);
      CASE l_attr_name 
      WHEN 'data_type' THEN 
        io_node_rec.data_type := l_attr_value; 
      WHEN 'pls_type' THEN 
        io_node_rec.pls_type := l_attr_value;
      WHEN 'index' THEN
        io_node_rec.indx := l_attr_value;   
      WHEN 'index_type' THEN
        io_node_rec.indx_type := l_attr_value;   
      ELSE
        NULL;   
      END CASE; 
    END LOOP; 
  END read_attributes;
  
  PROCEDURE read_element(
    in_node       IN dbms_xmldom.DOMNode,
    io_node_rec   IN OUT NOCOPY gt_node_rec 
  )
  AS
    l_text_node    dbms_xmldom.DOMNode;
    l_istream      sys.utl_CharacterInputStream;  
    l_chunksize    PLS_INTEGER;  
    l_buf          VARCHAR2(32767);  
  BEGIN
    io_node_rec.name  := dbms_xmldom.getNodeName(in_node);
    l_text_node := dbms_xmldom.getFirstChild(in_node);

    IF io_node_rec.data_type = 'CLOB' THEN
      io_node_rec.value := null; 
      l_istream := dbms_xmldom.getNodeValueAsCharacterStream(l_text_node);  
      l_chunksize := 32000;
      IF l_istream.handle IS NOT NULL THEN
        LOOP   
          -- read chunk from DOM node :  
          l_buf := null;
          l_istream.read(l_buf, l_chunksize, FALSE);
          -- write CLOB in chunk of <chunksize> :  
          io_node_rec.value := io_node_rec.value || l_buf;
          EXIT WHEN l_chunksize < 32000;
        END LOOP;  
      END IF;
     
    ELSE
      io_node_rec.value := dbms_xmldom.getNodeValue(l_text_node);
    END IF;
  END read_element;  

  -- Public declarations
  FUNCTION get_record_structure(
    in_package_owner  IN VARCHAR2,
    in_package_name   IN VARCHAR2,
    in_procedure_name IN VARCHAR2
  )
  RETURN gt_type_desc_arr                             
  AS                              
    l_arg_arr      gt_type_desc_arr;
    l_parent_arr   gt_type_desc_arr;
  BEGIN
    -- get list of all record attributes 
    OPEN g_arg_cur(
      in_package_owner  => in_package_owner,
      in_package_name   => in_package_name,
      in_procedure_name => in_procedure_name 
    );          
    
    BEGIN     
      FETCH g_arg_cur 
       BULK COLLECT 
       INTO l_arg_arr;
    EXCEPTION
      WHEN OTHERS THEN
        CLOSE g_arg_cur;
        RAISE;
    END;
    CLOSE g_arg_cur;

    IF l_arg_arr.COUNT > 0 THEN
      l_arg_arr(1).argument_name := 'RESULT';
    END IF;

    FOR i IN 1..l_arg_arr.COUNT LOOP
      IF i > 1 AND l_arg_arr(i).data_level > l_arg_arr(i-1).data_level THEN                
        -- set current parent prev record
        l_parent_arr(l_arg_arr(i).data_level) := l_arg_arr(i-1);
      END IF;
      IF l_parent_arr.LAST > 0 AND
         l_parent_arr.EXISTS(l_arg_arr(i).data_level)
      THEN
        IF l_parent_arr(l_arg_arr(i).data_level).data_type = 'PL/SQL TABLE' THEN
          l_arg_arr(i).full_name := l_parent_arr(l_arg_arr(i).data_level).full_name||'(l_indx'||l_parent_arr(l_arg_arr(i).data_level).sequence||')';
          IF l_arg_arr(i).argument_name IS NOT NULL THEN
            l_arg_arr(i).full_name := l_parent_arr(l_arg_arr(i).data_level).full_name||'.'||quote_name(l_arg_arr(i).argument_name);
          END IF;
        ELSE
          IF l_arg_arr(i).argument_name IS NOT NULL THEN
            l_arg_arr(i).full_name := l_parent_arr(l_arg_arr(i).data_level).full_name||'.'||quote_name(l_arg_arr(i).argument_name);
          ELSE
            l_arg_arr(i).full_name := l_parent_arr(l_arg_arr(i).data_level).full_name;
          END IF;
        END IF;   
      ELSE
        l_arg_arr(i).full_name := quote_name(l_arg_arr(i).argument_name); 
      END IF;
    END LOOP;
    
    RETURN l_arg_arr;
  END get_record_structure;
  
  
  -- return type index for PL/SQL TABLE index 
  FUNCTION get_type_name(in_index IN PLS_INTEGER)
  RETURN VARCHAR2
  AS 
  BEGIN
    RETURN 'PLS_INTEGER';
  END get_type_name;
  
  -- return type index for PL/SQL TABLE index 
  FUNCTION get_type_name(in_index IN VARCHAR2)
  RETURN VARCHAR2
  AS 
  BEGIN
    RETURN 'VARCHAR2(255)';
  END get_type_name;

  -- return XML DOM Node
  FUNCTION get_field_value(                                   
    in_dom_doc     IN dbms_xmldom.DOMDocument, 
    io_parent_node IN OUT NOCOPY dbms_xmldom.DOMNode,
    in_name        IN VARCHAR2,
    in_index       IN VARCHAR2,
    in_index_type  IN VARCHAR2,
    in_data_type   IN VARCHAR2,
    in_pls_type    IN VARCHAR2,
    in_value       IN CLOB
  )
  RETURN dbms_xmldom.DOMNode
  AS             
    l_name       VARCHAR2(100);
    l_element    dbms_xmldom.DOMElement;
    l_node       dbms_xmldom.DOMNode;
    l_value_node dbms_xmldom.DOMNode;
    l_ostream    sys.utl_CharacterOutputStream;  
    l_chunksize  PLS_INTEGER;  
    l_offset     PLS_INTEGER := 1;  
    l_buf        VARCHAR2(32767);  
  BEGIN
    IF in_name IS NULL THEN
      l_name := 'item';
    ELSE
      l_name := in_name;
    END IF;
    l_element := dbms_xmldom.createElement(in_dom_doc, l_name);
    IF in_name IS NULL THEN
      dbms_xmldom.setAttribute(l_element, 'index', in_index);
    END IF;                
    dbms_xmldom.setAttribute(l_element, 'name', in_name);
    dbms_xmldom.setAttribute(l_element, 'data_type', in_data_type);
    dbms_xmldom.setAttribute(l_element, 'pls_type', in_pls_type);
    IF in_index_type IS NOT NULL THEN
      dbms_xmldom.setAttribute(l_element, 'index_type', in_index_type);
    END IF;                

    l_node := dbms_xmldom.appendChild(
                n        => io_parent_node,
                newchild => dbms_xmldom.makeNode(l_element)
              );

    IF length(in_value) <= 32767 OR in_value IS NULL THEN
      l_value_node := dbms_xmldom.appendChild( 
                        n        => l_node,
                        newchild => dbms_xmldom.makeNode(dbms_xmldom.createTextNode(in_dom_doc, in_value))
                      );
    ELSE                  
      l_value_node := dbms_xmldom.appendChild( 
                        n        => l_node,
                        newchild => dbms_xmldom.makeNode(dbms_xmldom.createTextNode(in_dom_doc, null))
                      );

      l_ostream := dbms_xmldom.setNodeValueAsCharacterStream(l_value_node);  
      l_chunksize := dbms_lob.getchunksize(in_value);  
   
      LOOP  
        BEGIN  
          -- read BLOB in chunk of <chunksize> :  
          dbms_lob.read(in_value, l_chunksize, l_offset, l_buf);  
        EXCEPTION  
          WHEN NO_DATA_FOUND THEN  
            EXIT;  
        END;  
        -- write chunk to DOM node :  
        l_ostream.write(l_buf, l_chunksize);  
        l_offset := l_offset + l_chunksize;  
      END LOOP;  
     
      l_ostream.flush();  
      l_ostream.close();  
    
    END IF;

    RETURN l_node;
  END get_field_value;
             
  -- Read XML Node and return gt_node_rec
  FUNCTION get_node(
    in_node       IN dbms_xmldom.DOMNode
  )
  RETURN gt_node_rec
  AS
    l_node_rec  gt_node_rec;
  BEGIN
    l_node_rec.dom_node := in_node;
    read_attributes(in_node, l_node_rec);
    read_element(in_node, l_node_rec);
    RETURN l_node_rec;
  END get_node;
  
  -- Read children nodes 
  FUNCTION get_children_nodes(
    in_node_rec   IN gt_node_rec
  )
  RETURN gt_nodes_rec
  AS
    l_indx      PLS_INTEGER;
    l_nodes_rec gt_nodes_rec;
    l_node_rec  gt_node_rec;
    l_node      dbms_xmldom.DOMNode;
  BEGIN
    l_nodes_rec.parent_node := in_node_rec;
    l_indx := 1;
    l_node := dbms_xmldom.getFirstChild(in_node_rec.dom_node);
    WHILE NOT dbms_xmldom.isNull(l_node) LOOP
      l_node_rec := get_node(l_node);
      l_nodes_rec.children_nodes(l_indx) := l_node_rec;
      IF l_node_rec.name IS NOT NULL THEN
        l_nodes_rec.names_indx(l_node_rec.name) := l_indx;
      END IF;  
      l_indx := l_indx + 1;
      l_node := dbms_xmldom.getNextSibling(l_node);
    END LOOP; 
    RETURN l_nodes_rec;
  END get_children_nodes;
  
  -- Find children node by name 
  FUNCTION find_by_name(
    in_nodes_rec IN gt_nodes_rec,
    in_name      IN VARCHAR2
  )
  RETURN gt_node_rec
  AS
    l_indx      PLS_INTEGER;
    l_node_rec  gt_node_rec;
  BEGIN
    IF in_nodes_rec.names_indx.EXISTS(in_name) THEN
      l_indx := in_nodes_rec.names_indx(in_name);
      l_node_rec := in_nodes_rec.children_nodes(l_indx);
    END IF;
    RETURN l_node_rec;  
  END find_by_name;

  -- Return Node value as string 
  FUNCTION get_str_value(
    in_node_rec IN gt_node_rec
  )
  RETURN CLOB
  AS
  BEGIN
    RETURN in_node_rec.value;
  END get_str_value;
  
  -- Return Node value as number 
  FUNCTION get_num_value(
    in_node_rec IN gt_node_rec
  )
  RETURN NUMBER
  AS
  BEGIN
    RETURN TO_NUMBER(in_node_rec.value);
  END get_num_value;
  
  -- Return Node value as date 
  FUNCTION get_date_value(
    in_node_rec IN gt_node_rec
  )
  RETURN DATE
  AS
  BEGIN
    RETURN TO_DATE(in_node_rec.value, 'YYYY-MM-DD HH24:MI:SS');
  END get_date_value;
  
  -- Return Node value as timestamp
  FUNCTION get_time_value(
    in_node_rec IN gt_node_rec
  )
  RETURN TIMESTAMP
  AS
  BEGIN
    RETURN TO_TIMESTAMP(in_node_rec.value, 'YYYY-MM-DD HH24:MI:SS.FF9');
  END get_time_value;
  
  -- Return Node value as boolean 
  FUNCTION get_bool_value(
    in_node_rec IN gt_node_rec
  )
  RETURN BOOLEAN
  AS
    l_result BOOLEAN;
  BEGIN
    CASE in_node_rec.value
    WHEN 'TRUE' THEN 
      l_result := TRUE; 
    WHEN 'FALSE' THEN 
      l_result := FALSE;
    ELSE
      l_result := NULL;
    END CASE;
    RETURN l_result;   
  END get_bool_value;

  -- Return Node value as DS interval 
  FUNCTION get_dsinterval_value(
    in_node_rec IN gt_node_rec
  )
  RETURN dsinterval_unconstrained
  AS
  BEGIN
    RETURN CAST(CAST(in_node_rec.value AS VARCHAR2) AS INTERVAL DAY TO SECOND);
  END get_dsinterval_value;

  -- Return Node value as YM interval 
  FUNCTION get_yminterval_value(
    in_node_rec IN gt_node_rec
  )
  RETURN yminterval_unconstrained
  AS
  BEGIN
    RETURN CAST(CAST(in_node_rec.value AS VARCHAR2) AS INTERVAL YEAR TO MONTH);
  END get_yminterval_value;

  -- return SQL for convertion PL/SQL record to XML
  FUNCTION get_rec_to_xml_sql(
    in_arg_arr     IN gt_type_desc_arr,
    in_getter_name IN VARCHAR2
  ) 
  RETURN CLOB
  AS  
    l_sql CLOB;       
  BEGIN      
    g_last_seq_arr.DELETE;
    g_last_name_arr.DELETE;
    g_indent := CHR(10);
    IF in_arg_arr.COUNT > 0 THEN
      l_sql := 'DECLARE';
      g_indent := g_indent||'  ';
      l_sql := l_sql||g_indent||in_arg_arr(1).full_name||'    '||quote_name(in_arg_arr(1).type_owner)||'.'||quote_name(in_arg_arr(1).type_name)||'.'||quote_name(in_arg_arr(1).type_subname)||';';
      l_sql := l_sql||g_indent||'l_domdoc           dbms_xmldom.DOMDocument;';
      l_sql := l_sql||g_indent||'l_root_node        dbms_xmldom.DOMNode;';
      l_sql := l_sql||g_indent||'l_node             dbms_xmldom.DOMNode;';
      l_sql := l_sql||g_indent||'TYPE t_node_arr IS TABLE OF dbms_xmldom.DOMNode INDEX BY PLS_INTEGER;';
      l_sql := l_sql||g_indent||'l_parent_node_arr  t_node_arr;';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'BEGIN';
      g_indent := g_indent||'  ';
      -- Create an empty XML document
      l_sql := l_sql||g_indent||'l_domdoc := dbms_xmldom.newDomDocument;';
      -- Create a root node
      l_sql := l_sql||g_indent||'l_root_node := dbms_xmldom.makeNode(l_domdoc);';
      l_sql := l_sql||g_indent||'l_parent_node_arr(0) := l_root_node;';
      -- Read record
      l_sql := l_sql||g_indent||in_arg_arr(1).full_name||' := '||in_getter_name||';';

      FOR i IN 1..in_arg_arr.COUNT LOOP
        l_sql := l_sql||get_field_value_sql(in_arg_arr(i));
      END LOOP;           

      IF g_last_seq_arr.LAST IS NOT NULL AND 
         g_last_name_arr.LAST IS NOT NULL 
      THEN
        l_sql := l_sql||g_indent|| 'l_indx'||g_last_seq_arr(g_last_seq_arr.LAST)||' := '||g_last_name_arr(g_last_name_arr.LAST)||'.NEXT(l_indx'||g_last_seq_arr(g_last_seq_arr.LAST)||');';
        g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
        l_sql := l_sql||g_indent||'END LOOP;';
        g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
        l_sql := l_sql||g_indent||'END;';
      END IF;
      l_sql := l_sql||g_indent||':out_value := dbms_xmldom.getXmlType(l_domdoc);';
      l_sql := l_sql||g_indent||'dbms_xmldom.freeDocument(l_domdoc);';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'EXCEPTION';
      g_indent := g_indent||'  ';
      l_sql := l_sql||g_indent||'WHEN OTHERS THEN';
      g_indent := g_indent||'  ';
      l_sql := l_sql||g_indent||'dbms_xmldom.freeDocument(l_domdoc);';
      l_sql := l_sql||g_indent||'RAISE;';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-4);   
      l_sql := l_sql||g_indent||'END;';
      l_sql := l_sql||g_indent;
    END IF;                                     
    RETURN l_sql;
  END get_rec_to_xml_sql;
 
  -- return SQL for convertion XML to PL/SQL record
  FUNCTION get_xml_to_rec_sql(
    in_arg_arr     IN gt_type_desc_arr
  ) 
  RETURN CLOB
  AS  
    l_sql CLOB;       
  BEGIN      
    g_last_seq_arr.DELETE;
    g_last_name_arr.DELETE;
    g_indent := CHR(10);
    IF in_arg_arr.COUNT > 0 THEN
      l_sql := 'DECLARE';
      g_indent := g_indent||'  ';
      l_sql := l_sql||g_indent||in_arg_arr(1).full_name||'    '||quote_name(in_arg_arr(1).type_owner)||'.'||quote_name(in_arg_arr(1).type_name)||'.'||quote_name(in_arg_arr(1).type_subname)||';';
      l_sql := l_sql||g_indent||'l_xml              XMLType;';
      l_sql := l_sql||g_indent||'l_domdoc           dbms_xmldom.DOMDocument;';
      l_sql := l_sql||g_indent||'l_domnode          dbms_xmldom.DOMNode;';
      l_sql := l_sql||g_indent||'l_node             xml_utils.gt_node_rec;';
      l_sql := l_sql||g_indent||'l_nodes            xml_utils.gt_nodes_arr;';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'BEGIN';
      g_indent := g_indent||'  ';

      -- assign XML from input param
      l_sql := l_sql||g_indent||'l_xml := :in_xml;';
      -- Create an empty XML document
      l_sql := l_sql||g_indent||'l_domdoc := dbms_xmldom.newDomDocument(l_xml);';
      -- Create a root node
      l_sql := l_sql||g_indent||'BEGIN';
      g_indent := g_indent||'  ';
      l_sql := l_sql||g_indent||'l_domnode := dbms_xmldom.makeNode(l_domdoc);';
      l_sql := l_sql||g_indent||'l_node := xml_utils.get_node(l_domnode);';
      l_sql := l_sql||g_indent||'l_nodes(-1) := xml_utils.get_children_nodes(l_node);';
      
      FOR i IN 1..in_arg_arr.COUNT LOOP
        l_sql := l_sql||get_xml_value_sql(in_arg_arr(i));
      END LOOP;           

      IF g_last_seq_arr.LAST IS NOT NULL  
      THEN
        l_sql := l_sql||g_indent||'END;';
        g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
        l_sql := l_sql||g_indent||'END LOOP;';
        g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
        g_last_seq_arr.DELETE(g_last_seq_arr.LAST);
      END IF;
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'EXCEPTION';
      l_sql := l_sql||g_indent||' WHEN OTHERS THEN';
      l_sql := l_sql||g_indent||'   dbms_xmldom.freeDocument(l_domdoc);';
      l_sql := l_sql||g_indent||'   RAISE;';
      l_sql := l_sql||g_indent||'END;';
      l_sql := l_sql||g_indent||'dbms_xmldom.freeDocument(l_domdoc);';
      l_sql := l_sql||g_indent||':out_value := '||in_arg_arr(1).full_name||';';
      g_indent := SUBSTR(g_indent, 1, LENGTH(g_indent)-2);   
      l_sql := l_sql||g_indent||'END;';
      l_sql := l_sql||g_indent;
    END IF;                                     
    RETURN l_sql;
  END get_xml_to_rec_sql;


  -- Build and return SQL converting PL/SQL variable into XML
  FUNCTION get_record_to_xml_sql(
    in_package_owner  IN VARCHAR2,
    in_package_name   IN VARCHAR2,
    in_getter_name    IN VARCHAR2
  )
  RETURN CLOB
  AS
    l_arg_arr gt_type_desc_arr;
  BEGIN
    l_arg_arr := get_record_structure(
                   in_package_owner  => in_package_owner,
                   in_package_name   => in_package_name,
                   in_procedure_name => in_getter_name
                 );    
    RETURN get_rec_to_xml_sql(l_arg_arr, quote_name(in_package_owner)||'.'||quote_name(in_package_name)||'.'||quote_name(in_getter_name));
  END get_record_to_xml_sql;


  -- Build and return SQL converting XMLType into PL/SQL record
  FUNCTION get_xml_to_record_sql(
    in_package_owner  IN VARCHAR2,
    in_package_name   IN VARCHAR2,
    in_setter_name    IN VARCHAR2
  )
  RETURN CLOB
  AS
    l_arg_arr gt_type_desc_arr;
  BEGIN
    l_arg_arr := get_record_structure(
                   in_package_owner  => in_package_owner,
                   in_package_name   => in_package_name,
                   in_procedure_name => in_setter_name
                 );
    RETURN get_xml_to_rec_sql(l_arg_arr);                
  END get_xml_to_record_sql;


END xml_utils;
/