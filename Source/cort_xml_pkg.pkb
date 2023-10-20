CREATE OR REPLACE PACKAGE BODY cort_xml_pkg 
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
  g_table_rec_sql         CLOB;
  g_sequence_rec_sql      CLOB;
  g_type_rec_sql          CLOB;

  g_table_rec             cort_exec_pkg.gt_table_rec;
  g_sequence_rec          cort_exec_pkg.gt_sequence_rec;
  g_type_rec              cort_exec_pkg.gt_type_rec;
  
  -- getter for dynamic SQL
  FUNCTION get_table_rec
  RETURN cort_exec_pkg.gt_table_rec 
  AS
  BEGIN
    RETURN g_table_rec;
  END get_table_rec;
  
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
        
  -- write gt_table_rec record to xml
  FUNCTION get_table_xml(
    in_value IN  cort_exec_pkg.gt_table_rec
  ) 
  RETURN XMLType
  AS
    l_result XMLType;
  BEGIN          
    IF g_table_rec_sql IS NULL THEN
      g_table_rec_sql := xml_utils.get_record_to_xml_sql(
                           in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                           in_package_name   => 'CORT_XML_PKG',
                           in_getter_name    => 'GET_TABLE_REC'
                         );
    END IF;
    g_table_rec := in_value;
    cort_log_pkg.execute(
      in_sql  => g_table_rec_sql,
      in_echo => FALSE
    );
    BEGIN
      EXECUTE IMMEDIATE g_table_rec_sql USING OUT l_result;
    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.update_exec_time;
        cort_log_pkg.error('Error in converting cort_exec_pkg.gt_table_rec PL/SQL record to XML', g_table_rec_sql);
        RAISE;
    END;
    cort_log_pkg.update_exec_time;
    RETURN l_result;
  END get_table_xml;

  FUNCTION get_sequence_xml(
    in_value IN  cort_exec_pkg.gt_sequence_rec
  ) 
  RETURN XMLType
  AS
    l_result XMLType;
  BEGIN          
    IF g_sequence_rec_sql IS NULL THEN
      g_sequence_rec_sql := xml_utils.get_record_to_xml_sql(
                              in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                              in_package_name   => 'CORT_XML_PKG',
                              in_getter_name    => 'GET_SEQUENCE_REC'
                            );
    END IF;
    g_sequence_rec := in_value;
    cort_log_pkg.execute(
      in_sql  => g_sequence_rec_sql,
      in_echo => FALSE
    );
    BEGIN
      EXECUTE IMMEDIATE g_sequence_rec_sql USING OUT l_result;
    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.update_exec_time;
        cort_log_pkg.error('Error in converting cort_exec_pkg.gt_sequence_rec PL/SQL record to XML', g_sequence_rec_sql);
        RAISE;
    END;
    cort_log_pkg.update_exec_time;
    RETURN l_result;
  END get_sequence_xml;

  FUNCTION get_type_xml(
    in_value IN  cort_exec_pkg.gt_type_rec
  ) 
  RETURN XMLType
  AS
    l_result XMLType;
  BEGIN          
    IF g_type_rec_sql IS NULL THEN
      g_type_rec_sql := xml_utils.get_record_to_xml_sql(
                          in_package_owner  => SYS_CONTEXT('USERENV','CURRENT_USER'),
                          in_package_name   => 'CORT_XML_PKG',
                          in_getter_name    => 'GET_TYPE_REC'
                        );
    END IF;
    g_type_rec := in_value;
    cort_log_pkg.execute(
      in_sql  => g_type_rec_sql,
      in_echo => FALSE
    );
    BEGIN
      EXECUTE IMMEDIATE g_type_rec_sql USING OUT l_result;
    EXCEPTION
      WHEN OTHERS THEN
        cort_log_pkg.update_exec_time;
        cort_log_pkg.error('Error in converting cort_exec_pkg.gt_type_rec PL/SQL record to XML', g_type_rec_sql);
        RAISE;
    END;
    cort_log_pkg.update_exec_time;
    RETURN l_result;
  END get_type_xml;


END cort_xml_pkg;
/