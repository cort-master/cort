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
  Description: Before insert for each row trigger on cort.plan_table$ to capture event of EXPLAIN PLAN FOR command
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Extended explain plan functionality for applications like TOAD and SQLDeveloper
  ----------------------------------------------------------------------------------------------------------------------  
*/

CREATE OR REPLACE TRIGGER cort_before_xplan_trg BEFORE INSERT ON PLAN_TABLE FOR EACH ROW
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  IF cort_trg_pkg.get_status = 'ENABLED' THEN
    cort_trg_pkg.before_insert_xplan(
      io_id           => :new.id,
      io_parent_id    => :new.parent_id,
      io_depth        => :new.depth,  
      io_operation    => :new.operation,
      in_statement_id => :new.statement_id,
      in_plan_id      => :new.plan_id,
      in_timestamp    => :new.timestamp, 
      out_other_xml   => :new.other_xml  
    );
  END IF;
  COMMIT;
END;
/
