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
  Description: Script for cort_releases table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.00   | Rustam Kafarov    | table for storing all releases for given application. It also stores indicator for current release 
  ----------------------------------------------------------------------------------------------------------------------  
*/


---- TABLE CORT_RELEASES ----

BEGIN
  FOR X IN (SELECT * FROM user_tables WHERE table_name = 'CORT_RELEASES') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE '||x.table_name||' CASCADE CONSTRAINT';
  END LOOP;
END;
/  


CREATE TABLE cort_releases(
  application                    VARCHAR2(20)  NOT NULL,
  release                        VARCHAR2(20)  NOT NULL CHECK (release not in ('<current>', '<unknown>')),
  start_date                     DATE          NOT NULL,
  end_date                       DATE,
  status                         AS (NVL2(end_date, 'RELEASED', 'CURRENT')),
  CONSTRAINT cort_releases_pk
    PRIMARY KEY (application, release),
  CONSTRAINT cort_releases_uk
    UNIQUE (application, start_date),
  CONSTRAINT cort_releases_uk2
    UNIQUE (application, end_date),
  CONSTRAINT cort_releases_application_fk
    FOREIGN KEY (application) REFERENCES cort_applications(application)
);
