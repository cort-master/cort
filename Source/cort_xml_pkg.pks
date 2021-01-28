CREATE OR REPLACE PACKAGE cort_xml_pkg 
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
  ----------------------------------------------------------------------------------------------------------------------  
*/


  --getter-function for cort_exec_pkg.gt_table_rec type
  FUNCTION get_table_rec
  RETURN cort_exec_pkg.gt_table_rec;
        
  -- getter for dynamic SQL
  FUNCTION get_index_rec
  RETURN cort_exec_pkg.gt_index_rec; 
  
  FUNCTION get_sequence_rec
  RETURN cort_exec_pkg.gt_sequence_rec;
        
  FUNCTION get_type_rec
  RETURN cort_exec_pkg.gt_type_rec;
        
  -- setter for dynamic SQL
  PROCEDURE set_table_rec(in_value IN cort_exec_pkg.gt_table_rec);
  
  PROCEDURE set_index_rec(in_value IN cort_exec_pkg.gt_index_rec); 

  PROCEDURE set_sequence_rec(in_value IN cort_exec_pkg.gt_sequence_rec);

  PROCEDURE set_type_rec(in_value IN cort_exec_pkg.gt_type_rec);
  
  --getter-function for cort_params_pkg.gt_params_rec type
  FUNCTION get_params_rec
  RETURN cort_params_pkg.gt_params_rec;
        
  -- setter for dynamic SQL
  PROCEDURE set_params_rec(in_value IN cort_params_pkg.gt_params_rec);

  -- getter for dynamic SQL
  FUNCTION get_clob_arr
  RETURN arrays.gt_clob_arr; 
  
  -- setter for dynamic SQL
  PROCEDURE set_clob_arr(in_value IN arrays.gt_clob_arr); 

  -- write record to xml
  PROCEDURE write_to_xml(
    in_value IN  cort_exec_pkg.gt_table_rec,
    out_xml  OUT NOCOPY XMLType
  );  

  -- function wrapper 
  FUNCTION get_as_xml(
    in_value IN  cort_exec_pkg.gt_table_rec
  )
  RETURN CLOB;  

  FUNCTION get_as_xml(
    in_value IN  cort_exec_pkg.gt_sequence_rec
  )
  RETURN CLOB;  

  FUNCTION get_as_xml(
    in_value IN  cort_exec_pkg.gt_type_rec
  )
  RETURN CLOB;  
  
  -- write record to xml
  PROCEDURE write_to_xml(
    in_value IN  cort_exec_pkg.gt_index_rec,
    out_xml  OUT NOCOPY XMLType
  );  

  -- write gt_clob_arr record to xml
  PROCEDURE write_to_xml(
    in_value IN  arrays.gt_clob_arr, 
    out_xml  OUT NOCOPY XMLType
  );

  -- read gt_table_rec record from xml
  PROCEDURE read_from_xml(
    in_value IN  XMLType,
    out_rec  OUT NOCOPY cort_exec_pkg.gt_table_rec
  );  

  -- read gt_table_rec record from xml
  PROCEDURE read_from_xml(
    in_value IN  XMLType,
    out_rec  OUT NOCOPY cort_exec_pkg.gt_index_rec
  );  

  -- read gt_clob_arr record from xml
  PROCEDURE read_from_xml(
    in_value IN  XMLType,
    out_arr  OUT NOCOPY arrays.gt_clob_arr
  );  

  -- print values in gt_table_rec
  PROCEDURE print_table_rec(
    in_value IN cort_exec_pkg.gt_table_rec
  );

  -- print values in gt_index_rec
  PROCEDURE print_index_rec(
    in_value IN cort_exec_pkg.gt_index_rec
  );

  -- print values in gt_params_rec
  PROCEDURE print_params_rec(
    in_value IN cort_params_pkg.gt_params_rec
  );
  
  -- print values in gt_clob_arr
  PROCEDURE print_clob_arr(
    in_value IN arrays.gt_clob_arr
  );
  
END cort_xml_pkg;
/