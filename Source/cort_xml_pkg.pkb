CREATE OR REPLACE PACKAGE BODY cort_xml_pkg 
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
  Description: XML utils wrapper for CORT records.     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support of indexes
  16.00   | Rustam Kafarov    | Added support of sequences and types
  19.00   | Rustam Kafarov    | Used cort_exec_pkg API for dynamic execution
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- dynamic SQL for cort_exe_pkg.gt_table_rec record  
  g_table_rec_to_xml_sql  CLOB;
  g_print_table_rec_sql   CLOB;
  g_xml_to_table_rec_sql  CLOB;
  
  g_index_rec_to_xml_sql  CLOB;
  g_print_index_rec_sql   CLOB;
  g_xml_to_index_rec_sql  CLOB;

  g_sequence_rec_to_xml_sql  CLOB;
--  g_xml_to_sequence_rec_sql  CLOB;

  g_type_rec_to_xml_sql   CLOB;
--  g_xml_to_type_rec_sql   CLOB;

  g_params_rec_to_xml_sql CLOB;
  g_print_params_rec_sql  CLOB;
  g_xml_to_params_rec_sql CLOB;

  g_clob_arr_to_xml_sql   CLOB;
  g_print_clob_arr_sql    CLOB;
  g_xml_to_clob_arr_sql   CLOB;

  g_table_rec             cort_exec_pkg.gt_table_rec;
  g_index_rec             cort_exec_pkg.gt_index_rec;
  g_sequence_rec          cort_exec_pkg.gt_sequence_rec;
  g_type_rec              cort_exec_pkg.gt_type_rec;
  g_params_rec            cort_params_pkg.gt_params_rec;
  g_clob_arr              arrays.gt_clob_arr;
  
  -- getter for dynamic SQL
  FUNCTION get_table_rec
  RETURN cort_exec_pkg.gt_table_rec 
  AS
  BEGIN
    RETURN g_table_rec;
  END get_table_rec;
  
  -- getter for dynamic SQL
  FUNCTION get_index_rec
  RETURN cort_exec_pkg.gt_index_rec 
  AS
  BEGIN
    RETURN g_index_rec;
  END get_index_rec;
  
  FUNCTION get_sequence_rec
  RETURN cort_exec_pkg.gt_sequence_rec
  AS
  BEGIN
    RETURN g_sequence_rec;
  END get_sequence_rec;
        
  FUNCTION get_type_rec
  RETURN cort_exec_pkg.gt_type_rec
  AS
  BEGIN
    RETURN g_type_rec;
  END get_type_rec;
        
  -- setter for dynamic SQL
  PROCEDURE set_table_rec(in_value IN cort_exec_pkg.gt_table_rec) 
  AS
  BEGIN
    g_table_rec := in_value;
  END set_table_rec;
  
  -- setter for dynamic SQL
  PROCEDURE set_index_rec(in_value IN cort_exec_pkg.gt_index_rec) 
  AS
  BEGIN
    g_index_rec := in_value;
  END set_index_rec;
  
  PROCEDURE set_sequence_rec(in_value IN cort_exec_pkg.gt_sequence_rec)
  AS
  BEGIN
    g_sequence_rec := in_value;
  END set_sequence_rec;

  PROCEDURE set_type_rec(in_value IN cort_exec_pkg.gt_type_rec)
  AS
  BEGIN
    g_type_rec := in_value;
  END set_type_rec;
  
  --getter-function for cort_params_pkg.gt_params_rec type
  FUNCTION get_params_rec
  RETURN cort_params_pkg.gt_params_rec
  AS
  BEGIN
    RETURN g_params_rec;
  END get_params_rec;
        
  -- setter for dynamic SQL
  PROCEDURE set_params_rec(in_value IN cort_params_pkg.gt_params_rec)
  AS
  BEGIN
    g_params_rec := in_value;
  END set_params_rec;

  -- getter for dynamic SQL
  FUNCTION get_clob_arr
  RETURN arrays.gt_clob_arr 
  AS
  BEGIN
    RETURN g_clob_arr;
  END get_clob_arr;
  
  -- setter for dynamic SQL
  PROCEDURE set_clob_arr(in_value IN arrays.gt_clob_arr) 
  AS
  BEGIN
    g_clob_arr := in_value;
  END set_clob_arr;


  -- write gt_table_rec record to xml
  PROCEDURE write_to_xml(
    in_value IN  cort_exec_pkg.gt_table_rec,
    out_xml  OUT NOCOPY XMLType
  )  
  AS
  BEGIN          
    IF g_table_rec_to_xml_sql IS NULL THEN
      g_table_rec_to_xml_sql := xml_utils.get_record_to_xml_sql(
                                  in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                  in_package_name   => 'CORT_XML_PKG',
                                  in_getter_name    => 'GET_TABLE_REC'
                                );
    END IF;
    g_table_rec := in_value;
    cort_exec_pkg.execute_immediate_xml(
      in_sql  => g_table_rec_to_xml_sql, 
      out_xml => out_xml
    );
  END write_to_xml;

  PROCEDURE write_to_xml(
    in_value IN  cort_exec_pkg.gt_sequence_rec,
    out_xml  OUT NOCOPY XMLType
  )  
  AS
  BEGIN          
    IF g_sequence_rec_to_xml_sql IS NULL THEN
      g_sequence_rec_to_xml_sql := xml_utils.get_record_to_xml_sql(
                                     in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                     in_package_name   => 'CORT_XML_PKG',
                                     in_getter_name    => 'GET_SEQUENCE_REC'
                                   );
    END IF;
    g_sequence_rec := in_value;
    cort_exec_pkg.execute_immediate_xml( 
      in_sql  => g_sequence_rec_to_xml_sql,
      out_xml => out_xml
    );
  END write_to_xml;

  PROCEDURE write_to_xml(
    in_value IN  cort_exec_pkg.gt_type_rec,
    out_xml  OUT NOCOPY XMLType
  )  
  AS
  BEGIN          
    IF g_type_rec_to_xml_sql IS NULL THEN
      g_type_rec_to_xml_sql := xml_utils.get_record_to_xml_sql(
                                 in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                 in_package_name   => 'CORT_XML_PKG',
                                 in_getter_name    => 'GET_TYPE_REC'
                               );
    END IF;
    g_type_rec := in_value;
    cort_exec_pkg.execute_immediate_xml(
      in_sql  => g_type_rec_to_xml_sql, 
      out_xml => out_xml
    );
  END write_to_xml;

  -- function wrapper 
  FUNCTION get_as_xml(
    in_value IN  cort_exec_pkg.gt_table_rec
  )
  RETURN CLOB
  AS
    l_result XMLType;
  BEGIN
    write_to_xml(in_value, l_result);
    RETURN l_result.GetClobVal();
  END get_as_xml;

  FUNCTION get_as_xml(
    in_value IN  cort_exec_pkg.gt_sequence_rec
  )
  RETURN CLOB
  AS
    l_result XMLType;
  BEGIN
    write_to_xml(in_value, l_result);
    RETURN l_result.GetClobVal();
  END get_as_xml;

  FUNCTION get_as_xml(
    in_value IN  cort_exec_pkg.gt_type_rec
  )
  RETURN CLOB
  AS
    l_result XMLType;
  BEGIN
    write_to_xml(in_value, l_result);
    RETURN l_result.GetClobVal();
  END get_as_xml;


  -- write gt_index_rec record to xml
  PROCEDURE write_to_xml(
    in_value IN  cort_exec_pkg.gt_index_rec,
    out_xml  OUT NOCOPY XMLType
  )  
  AS
  BEGIN          
    IF g_index_rec_to_xml_sql IS NULL THEN
      g_index_rec_to_xml_sql := xml_utils.get_record_to_xml_sql(
                                  in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                  in_package_name   => 'CORT_XML_PKG',
                                  in_getter_name    => 'GET_INDEX_REC'
                                );
    END IF;
    g_index_rec := in_value;
    cort_exec_pkg.execute_immediate_xml(
      in_sql  => g_index_rec_to_xml_sql,
      out_xml => out_xml
    );

  END write_to_xml;

  -- write gt_clob_arr record to xml
  PROCEDURE write_to_xml(
    in_value IN  arrays.gt_clob_arr, 
    out_xml  OUT NOCOPY XMLType
  )
  AS
  BEGIN          
    IF g_clob_arr_to_xml_sql IS NULL THEN
      g_clob_arr_to_xml_sql := xml_utils.get_record_to_xml_sql(
                                 in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                 in_package_name   => 'CORT_XML_PKG',
                                 in_getter_name    => 'GET_CLOB_ARR'
                               );
    END IF;
    g_clob_arr := in_value;
    cort_exec_pkg.execute_immediate_xml(
      in_sql  => g_clob_arr_to_xml_sql,
      out_xml => out_xml
    );
  END write_to_xml;
  
  -- read gt_table_rec record from xml
  PROCEDURE read_from_xml(
    in_value IN  XMLType,
    out_rec  OUT NOCOPY cort_exec_pkg.gt_table_rec
  )  
  AS
  BEGIN          
    IF g_xml_to_table_rec_sql IS NULL THEN
      g_xml_to_table_rec_sql := xml_utils.get_xml_to_record_sql(
                                  in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                  in_package_name   => 'CORT_XML_PKG',
                                  in_setter_name    => 'SET_TABLE_REC'
                                );
    END IF;
    cort_exec_pkg.execute_immediate_xml(
      in_sql  => g_xml_to_table_rec_sql,
      in_xml  => in_value
    );
    out_rec := g_table_rec;
  END read_from_xml;
 
  -- read gt_index_rec record from xml
  PROCEDURE read_from_xml(
    in_value IN  XMLType,
    out_rec  OUT NOCOPY cort_exec_pkg.gt_index_rec
  )  
  AS
  BEGIN          
    IF g_xml_to_index_rec_sql IS NULL THEN
      g_xml_to_index_rec_sql := xml_utils.get_xml_to_record_sql(
                                  in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                  in_package_name   => 'CORT_XML_PKG',
                                  in_setter_name    => 'SET_INDEX_REC'
                                );
    END IF;
    cort_exec_pkg.execute_immediate_xml(
      in_sql  => g_xml_to_index_rec_sql,
      in_xml  => in_value
    );
    out_rec := g_index_rec;
  END read_from_xml;
 
  -- read gt_clob_arr record from xml
  PROCEDURE read_from_xml(
    in_value IN  XMLType,
    out_arr  OUT NOCOPY arrays.gt_clob_arr
  )  
  AS
  BEGIN          
    IF g_xml_to_clob_arr_sql IS NULL THEN
      g_xml_to_clob_arr_sql := xml_utils.get_xml_to_record_sql(
                                 in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                 in_package_name   => 'CORT_XML_PKG',
                                 in_setter_name    => 'SET_CLOB_ARR'
                               );
    END IF;
    cort_exec_pkg.execute_immediate_xml(
      in_sql  => g_xml_to_clob_arr_sql,
      in_xml  => in_value
    );
    out_arr := g_clob_arr;
  END read_from_xml;
 
  -- print values in gt_table_rec
  PROCEDURE print_table_rec(
    in_value IN cort_exec_pkg.gt_table_rec
  )
  AS
  BEGIN
    IF g_print_table_rec_sql IS NULL THEN
      g_print_table_rec_sql := xml_utils.get_print_record_sql(
                                 in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                 in_package_name   => 'CORT_XML_PKG',
                                 in_getter_name    => 'GET_TABLE_REC'
                               );
    END IF;
    g_table_rec := in_value;
    cort_exec_pkg.execute_immediate(g_print_table_rec_sql, FALSE, FALSE);
  END print_table_rec;

  -- print values in gt_index_rec
  PROCEDURE print_index_rec(
    in_value IN cort_exec_pkg.gt_index_rec
  )
  AS
  BEGIN
    IF g_print_index_rec_sql IS NULL THEN
      g_print_index_rec_sql := xml_utils.get_print_record_sql(
                                 in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                 in_package_name   => 'CORT_XML_PKG',
                                 in_getter_name    => 'GET_INDEX_REC'
                               );
    END IF;
    g_index_rec := in_value;
    cort_exec_pkg.execute_immediate(g_print_index_rec_sql, FALSE, FALSE);
  END print_index_rec;

  -- print values in gt_params_rec
  PROCEDURE print_params_rec(
    in_value IN cort_params_pkg.gt_params_rec
  )
  AS
  BEGIN
    IF g_print_params_rec_sql IS NULL THEN
      g_print_params_rec_sql := xml_utils.get_print_record_sql(
                                  in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                  in_package_name   => 'CORT_XML_PKG',
                                  in_getter_name    => 'GET_PARAMS_REC'
                                );
    END IF;
    g_params_rec := in_value;
    cort_exec_pkg.execute_immediate(g_print_params_rec_sql, FALSE, FALSE);
  END print_params_rec;

  -- print values in gt_clob_arr
  PROCEDURE print_clob_arr(
    in_value IN arrays.gt_clob_arr
  )
  AS
  BEGIN
    IF g_print_clob_arr_sql IS NULL THEN
      g_print_clob_arr_sql := xml_utils.get_print_record_sql(
                                in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                                in_package_name   => 'CORT_XML_PKG',
                                in_getter_name    => 'GET_CLOB_ARR'
                              );
    END IF;
    g_clob_arr := in_value;
    cort_exec_pkg.execute_immediate(g_print_clob_arr_sql, FALSE, FALSE);
  END print_clob_arr;

END cort_xml_pkg;
/