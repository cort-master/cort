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
  Description: Script for cort_type_synonyms table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  22.00   | Rustam Kafarov    | Table for mapping between synonyms with real names for types and real generated type names  
  ----------------------------------------------------------------------------------------------------------------------  
*/

---- TABLE CORT_TYPE_SYNONYMS ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_TYPE_SYNONYMS') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  


CREATE TABLE cort_type_synonyms(
  job_id                         TIMESTAMP(6)  NOT NULL,
  synonym_owner                  VARCHAR2(128) NOT NULL,
  synonym_name                   VARCHAR2(128) NOT NULL,
  type_name                      VARCHAR2(128) NOT NULL,
  CONSTRAINT cort_type_synonyms_pk PRIMARY KEY (synonym_owner, synonym_name, job_id)
)
ORGANIZATION INDEX
;
 

