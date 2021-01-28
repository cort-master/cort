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

  Description: View returning importing stats
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  17.00   | Rustam Kafarov    | Created view
  ----------------------------------------------------------------------------------------------------------------------  
*/

CREATE OR REPLACE VIEW cort_import_stat
AS
SELECT statid, type, version, flags, 
       nvl(sys_context('AVALON','TARGET_TABLE_NAME'),c1) as c1, 
       case 
         when sys_context('AVALON','TARGET_PARTITION_LEVEL') = 'PARTITION' then
           nvl(sys_context('AVALON','TARGET_PARTITION_NAME'),c2) 
         else c2
       end as c2, 
       case 
         when sys_context('AVALON','TARGET_PARTITION_LEVEL') = 'PARTITION' then
           null
         when sys_context('AVALON','TARGET_PARTITION_LEVEL') = 'SUBPARTITION' then
           nvl(sys_context('AVALON','TARGET_PARTITION_NAME'),c3)
         else c3
       end  as c3, 
       c4, c5, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, d1, r1, r2, ch1, cl1,  
       c1 as source_table_name,
       c2 as source_partition_name,
       c3 as source_subpartition_name
  FROM cort_stat
;
