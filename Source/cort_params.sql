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
  Description: Script for population cort_params table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Changed params: VERSION->RELEASE, ROLLBACK->REVERT. Removed param LOG (now logging is always on)
  15.00   | Rustam Kafarov    | Added IGNORE_ORDER param
  16.00   | Rustam Kafarov    | Added DEFERRED_DATA_COPY param
  17.00   | Rustam Kafarov    | Added TIMING param
  18.03   | Rustam Kafarov    | Changed params for STATS
  19.00   | Rustam Kafarov    | Overwite default param values for each session
  ----------------------------------------------------------------------------------------------------------------------  
*/

-- CORT params

INSERT INTO cort_applications VALUES('DEFAULT', '.*', 'DEFAULT');

COMMIT;
