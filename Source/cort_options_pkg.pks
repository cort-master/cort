CREATE OR REPLACE PACKAGE cort_options_pkg 
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
  Description: Package for conditional compilation constants     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | To support CC for single schema/entire database modes or for explain plan option
  15.00   | Rustam Kafarov    | Added conditional context using
  16.00   | Rustam Kafarov    | Moved conditional context using to config_params_pkg
  17.00   | Rustam Kafarov    | Separated CC constants (options) from configurable params
  22.00   | Rustam Kafarov    | Added param for auto-renaming type and creating synonyms with original name for them 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  gc_use_context             CONSTANT BOOLEAN := false;
  gc_explain_plan            CONSTANT BOOLEAN := true;
  gc_threading               CONSTANT BOOLEAN := true;
  gc_rename_types            CONSTANT BOOLEAN := false;
  
END cort_options_pkg;
/