/*
PL/SQL Utilities - Predefined SQL Arrays

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
  Description: Predefined SQL Arrays
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  18.09   | Rustam Kafarov    | Defined generic SQL arrays
  ----------------------------------------------------------------------------------------------------------------------
*/

create or replace type number_array force as table of number
/

create or replace type varchar2_array force as table of varchar2(32767)
/

create or replace type date_array force as table of date
/

create or replace type timestamp_array as table of timestamp(9)
/

create or replace type interval_ym_array force as table of yminterval_unconstrained
/

create or replace type interval_ds_array force as table of dsinterval_unconstrained
/

create or replace type raw_array force as table of raw(32767)
/

create or replace TYPE clob_array force as table of clob
/

create or replace type blob_array force as table of blob
/

create or replace type xml_array force as table of xmltype
/

