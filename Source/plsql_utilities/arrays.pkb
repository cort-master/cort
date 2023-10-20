CREATE OR REPLACE PACKAGE BODY arrays 
AS

/*
PL/SQL Utilities - Predefined PL/SQL Collection types 

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
  Description: PL/SQL Collection types - associative arrays indexed by PLS_INTEGER, indexed by VARCHAR2(32767) and nested tables.     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  18.09   | Rustam Kafarov    | Added init functions for indx arrays
  ----------------------------------------------------------------------------------------------------------------------  
*/

  FUNCTION init_str_indx(in_indices IN gt_xlstr_tab, in_values IN gt_str_tab DEFAULT NULL) RETURN gt_str_indx
  AS
    l_result  gt_str_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_str_indx;

  FUNCTION init_mstr_indx(in_indices IN gt_xlstr_tab, in_values IN gt_mstr_tab DEFAULT NULL) RETURN gt_mstr_indx
  AS
    l_result  gt_mstr_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_mstr_indx;

  FUNCTION init_lstr_indx(in_indices IN gt_xlstr_tab, in_values IN gt_lstr_tab DEFAULT NULL) RETURN gt_lstr_indx
  AS
    l_result  gt_lstr_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_lstr_indx;

  FUNCTION init_xlstr_indx(in_indices IN gt_xlstr_tab, in_values IN gt_xlstr_tab DEFAULT NULL) RETURN gt_xlstr_indx
  AS
    l_result  gt_xlstr_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_xlstr_indx;

  FUNCTION init_date_indx(in_indices IN gt_xlstr_tab, in_values IN gt_date_tab DEFAULT NULL) RETURN gt_date_indx
  AS
    l_result  gt_date_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_date_indx;

  FUNCTION init_num_indx(in_indices IN gt_xlstr_tab, in_values IN gt_num_tab DEFAULT NULL) RETURN gt_num_indx
  AS
    l_result  gt_num_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_num_indx;

  FUNCTION init_int_indx(in_indices IN gt_xlstr_tab, in_values IN gt_int_tab DEFAULT NULL) RETURN gt_int_indx
  AS
    l_result  gt_int_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_int_indx;

  FUNCTION init_timestamp_indx(in_indices IN gt_xlstr_tab, in_values IN gt_timestamp_tab DEFAULT NULL) RETURN gt_timestamp_indx
  AS
    l_result  gt_timestamp_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_timestamp_indx;

  FUNCTION init_timestamp_tz_indx(in_indices IN gt_xlstr_tab, in_values IN gt_timestamp_tz_tab DEFAULT NULL) RETURN gt_timestamp_tz_indx
  AS
    l_result  gt_timestamp_tz_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_timestamp_tz_indx;

  FUNCTION init_timestamp_ltz_indx(in_indices IN gt_xlstr_tab, in_values IN gt_timestamp_ltz_tab DEFAULT NULL) RETURN gt_timestamp_ltz_indx
  AS
    l_result  gt_timestamp_ltz_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_timestamp_ltz_indx;

  FUNCTION init_char_indx(in_indices IN gt_xlstr_tab, in_values IN gt_char_tab DEFAULT NULL) RETURN gt_char_indx
  AS
    l_result  gt_char_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_char_indx;

  FUNCTION init_raw_indx(in_indices IN gt_xlstr_tab, in_values IN gt_raw_tab DEFAULT NULL) RETURN gt_raw_indx
  AS
    l_result  gt_raw_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_raw_indx;

  FUNCTION init_lraw_indx(in_indices IN gt_xlstr_tab, in_values IN gt_lraw_tab DEFAULT NULL) RETURN gt_lraw_indx
  AS
    l_result  gt_lraw_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_lraw_indx;

  FUNCTION init_xlraw_indx(in_indices IN gt_xlstr_tab, in_values IN gt_xlraw_tab DEFAULT NULL) RETURN gt_xlraw_indx
  AS
    l_result  gt_xlraw_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_xlraw_indx;
  
  FUNCTION init_interval_ym_indx(in_indices IN gt_xlstr_tab, in_values IN gt_interval_ym_tab DEFAULT NULL) RETURN gt_interval_ym_indx
  AS
    l_result  gt_interval_ym_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_interval_ym_indx;

  FUNCTION init_interval_ds_indx(in_indices IN gt_xlstr_tab, in_values IN gt_interval_ds_tab DEFAULT NULL) RETURN gt_interval_ds_indx
  AS
    l_result  gt_interval_ds_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_interval_ds_indx;

  FUNCTION init_clob_indx(in_indices IN gt_xlstr_tab, in_values IN gt_clob_tab DEFAULT NULL) RETURN gt_clob_indx
  AS
    l_result  gt_clob_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_clob_indx;

  FUNCTION init_blob_indx(in_indices IN gt_xlstr_tab, in_values IN gt_blob_tab) RETURN gt_blob_indx
  AS
    l_result  gt_blob_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_blob_indx;


  FUNCTION init_xml_indx(in_indices IN gt_xlstr_tab, in_values IN gt_xml_tab) RETURN gt_xml_indx
  AS
    l_result  gt_xml_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_xml_indx;

  FUNCTION init_rowid_indx(in_indices IN gt_xlstr_tab, in_values IN gt_rowid_tab DEFAULT NULL) RETURN gt_rowid_indx
  AS
    l_result  gt_rowid_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_rowid_indx;

  FUNCTION init_urowid_indx(in_indices IN gt_xlstr_tab, in_values IN gt_urowid_tab DEFAULT NULL) RETURN gt_urowid_indx
  AS
    l_result  gt_urowid_indx;
  BEGIN
    IF in_indices IS NOT NULL THEN
      IF in_values IS NOT NULL THEN 
        IF in_values.COUNT <> in_indices.COUNT THEN
          raise_application_error(-20000,'Number of elemnts for in_indices and in_values must match');
        END IF;
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_values(i); 
        END LOOP; 
      ELSE
        FOR i IN 1..in_indices.COUNT LOOP
          l_result(in_indices(i)) := in_indices(i); 
        END LOOP; 
      END IF;
    END IF;
    RETURN l_result;
  END init_urowid_indx;


END arrays;
/