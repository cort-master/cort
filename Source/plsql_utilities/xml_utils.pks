CREATE OR REPLACE PACKAGE xml_utils
AUTHID CURRENT_USER 
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
  21.00   | Rustam Kafarov    | Added support for Oracle versions 18, 19. Compiled as AUTHID CURRENT_USER
  ----------------------------------------------------------------------------------------------------------------------  
*/

  TYPE gt_type_desc_rec IS RECORD (
    sequence        NUMBER,
    data_level      NUMBER,
    argument_name   VARCHAR2(255),
    position        NUMBER,
    data_type       VARCHAR(255),
    type_owner      arrays.gt_name,
    type_name       arrays.gt_name,
    type_subname    arrays.gt_name,
    pls_type        VARCHAR2(255),
    full_name       VARCHAR2(32767)
  );

  TYPE gt_type_desc_arr IS TABLE OF gt_type_desc_rec INDEX BY PLS_INTEGER;



  TYPE gt_node_rec IS RECORD(
    dom_node  dbms_xmldom.DOMNode,
    name      VARCHAR2(255),
    value     CLOB,
    data_type VARCHAR2(255), 
    pls_type  VARCHAR2(255),
    indx      VARCHAR2(255),
    indx_type VARCHAR2(30)
  );
  TYPE gt_node_arr IS TABLE OF gt_node_rec INDEX BY PLS_INTEGER;

  TYPE gt_nodes_rec IS RECORD(
    parent_node     gt_node_rec,
    children_nodes  gt_node_arr,
    names_indx      arrays.gt_int_indx        
  );
  TYPE gt_nodes_arr IS TABLE OF gt_nodes_rec INDEX BY PLS_INTEGER;

  FUNCTION get_record_structure(
    in_package_owner  IN VARCHAR2,
    in_package_name   IN VARCHAR2,
    in_procedure_name IN VARCHAR2
  )
  RETURN gt_type_desc_arr;                             

  -- return type index for PL/SQL TABLE index 
  FUNCTION get_type_name(in_index IN PLS_INTEGER)
  RETURN VARCHAR2;

  -- return type index for PL/SQL TABLE index 
  FUNCTION get_type_name(in_index IN VARCHAR2)
  RETURN VARCHAR2;

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
  RETURN dbms_xmldom.DOMNode;
  
  -- Read XML Node and return gt_node_rec
  FUNCTION get_node(
    in_node       IN dbms_xmldom.DOMNode
  )
  RETURN gt_node_rec;
  
  -- Read children nodes 
  FUNCTION get_children_nodes(
    in_node_rec   IN gt_node_rec
  )
  RETURN gt_nodes_rec;
   
  -- Find children node by name 
  FUNCTION find_by_name(
    in_nodes_rec IN gt_nodes_rec,
    in_name      IN VARCHAR2
  )
  RETURN gt_node_rec;
  
  -- Return Node value as string 
  FUNCTION get_str_value(
    in_node_rec IN gt_node_rec
  )
  RETURN CLOB;  

  -- Return Node value as number 
  FUNCTION get_num_value(
    in_node_rec IN gt_node_rec
  )
  RETURN NUMBER;  
  
  -- Return Node value as timestamp
  FUNCTION get_time_value(
    in_node_rec IN gt_node_rec
  )
  RETURN TIMESTAMP;
  
  -- Return Node value as date 
  FUNCTION get_date_value(
    in_node_rec IN gt_node_rec
  )
  RETURN DATE;
  
  -- Return Node value as boolean 
  FUNCTION get_bool_value(
    in_node_rec IN gt_node_rec
  )
  RETURN BOOLEAN;

  -- return SQL for convertion XML to PL/SQL record
  FUNCTION get_xml_to_rec_sql(
    in_arg_arr     IN gt_type_desc_arr
  ) 
  RETURN CLOB;

  -- Return SQL for dynamic sql to convert PL/SQL record into XMLType
  FUNCTION get_record_to_xml_sql(
    in_package_owner  IN VARCHAR2,
    in_package_name   IN VARCHAR2,
    in_getter_name    IN VARCHAR2
  )
  RETURN CLOB;


  -- Return SQL for dynamic sql to convert XMLType into PL/SQL record
  FUNCTION get_xml_to_record_sql(
    in_package_owner  IN VARCHAR2,
    in_package_name   IN VARCHAR2,
    in_setter_name    IN VARCHAR2
  )
  RETURN CLOB;
  
END xml_utils;
/