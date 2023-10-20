CREATE OR REPLACE PACKAGE cort_xml_pkg 
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
  21.00   | Rustam Kafarov    | Added support for 18, 19 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- getter for dynamic SQL
  FUNCTION get_table_rec
  RETURN cort_exec_pkg.gt_table_rec; 
  
  FUNCTION get_sequence_rec
  RETURN cort_exec_pkg.gt_sequence_rec;
  
  FUNCTION get_type_rec
  RETURN cort_exec_pkg.gt_type_rec;

  -- XML conversion functions  
  FUNCTION get_table_xml(
    in_value IN  cort_exec_pkg.gt_table_rec
  )
  RETURN XMLTYPE;  

  FUNCTION get_sequence_xml(
    in_value IN  cort_exec_pkg.gt_sequence_rec
  )
  RETURN XMLTYPE;  

  FUNCTION get_type_xml(
    in_value IN  cort_exec_pkg.gt_type_rec
  )
  RETURN XMLTYPE;  
  
END cort_xml_pkg;
/