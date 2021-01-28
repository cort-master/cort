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
  Description: Script for cort_params table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | Table for custom param default values 
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_PARAMS ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_PARAMS') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  


CREATE TABLE cort_params (
  user_name                 VARCHAR2(128) NOT NULL,
  param_name                VARCHAR2(30)  NOT NULL CHECK(param_name = UPPER(param_name)),
  default_value             VARCHAR2(4000),
  CONSTRAINT cort_params_pk PRIMARY KEY (user_name, param_name)
)
ORGANIZATION INDEX
OVERFLOW;
