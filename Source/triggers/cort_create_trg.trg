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
  Description: Trigger to intercept table, index and sequence creation events
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support for sequences and indexes
  15.00   | Rustam Kafarov    | Added support for objects (TYPE)
  17.00   | Rustam Kafarov    | Removed option for entire database
  ----------------------------------------------------------------------------------------------------------------------  
*/

-- This type of trigger does not support replace option

CREATE TRIGGER cort_create_trg INSTEAD OF CREATE ON SCHEMA
WHEN (
      (cort_trg_pkg.get_status = 'ENABLED') AND 
      (ora_dict_obj_type IN ('TABLE','INDEX','SEQUENCE','TYPE','TYPE BODY')) AND
      (ora_dict_obj_owner NOT IN ('SYS','SYSTEM')) AND
      (cort_trg_pkg.get_execution_mode(ora_dict_obj_type) = 'REPLACE')
     )
BEGIN
  cort_trg_pkg.instead_of_create;
END;  
/
