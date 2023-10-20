CREATE OR REPLACE PACKAGE arrays 
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
  14.01   | Rustam Kafarov    | Main functionality
  15.00   | Rustam Kafarov    | Added rowid arrays
  18.09   | Rustam Kafarov    | Added init functions for indx arrays
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  $IF (dbms_db_version.version = 12 and dbms_db_version.release >= 2) or (dbms_db_version.version > 12) $THEN
    gc_long_name_supported    CONSTANT BOOLEAN := TRUE;
    gc_name_max_length        CONSTANT PLS_INTEGER := 128;
    gc_full_name_max_length   CONSTANT PLS_INTEGER := 261;
    SUBTYPE gt_name           IS VARCHAR2(128);
    SUBTYPE gt_full_name      IS VARCHAR2(261);
  $ELSE
    gc_long_name_supported    CONSTANT BOOLEAN := FALSE;
    gc_name_max_length        CONSTANT PLS_INTEGER := 30;
    gc_full_name_max_length   CONSTANT PLS_INTEGER := 65;
    SUBTYPE gt_name           IS VARCHAR2(30);
    SUBTYPE gt_full_name      IS VARCHAR2(65);
  $END

  TYPE gt_name_arr           IS TABLE OF gt_name                            INDEX BY PLS_INTEGER;
  TYPE gt_str_arr            IS TABLE OF VARCHAR2(30)                       INDEX BY PLS_INTEGER;
  TYPE gt_mstr_arr           IS TABLE OF VARCHAR2(256)                      INDEX BY PLS_INTEGER;
  TYPE gt_lstr_arr           IS TABLE OF VARCHAR2(4000)                     INDEX BY PLS_INTEGER;
  TYPE gt_xlstr_arr          IS TABLE OF VARCHAR2(32767)                    INDEX BY PLS_INTEGER;
  TYPE gt_date_arr           IS TABLE OF DATE                               INDEX BY PLS_INTEGER;
  TYPE gt_num_arr            IS TABLE OF NUMBER                             INDEX BY PLS_INTEGER;
  TYPE gt_int_arr            IS TABLE OF PLS_INTEGER                        INDEX BY PLS_INTEGER;
  TYPE gt_timestamp_arr      IS TABLE OF TIMESTAMP(9)                       INDEX BY PLS_INTEGER;
  TYPE gt_timestamp_tz_arr   IS TABLE OF TIMESTAMP(9) WITH TIME ZONE        INDEX BY PLS_INTEGER;
  TYPE gt_timestamp_ltz_arr  IS TABLE OF TIMESTAMP(9) WITH LOCAL TIME ZONE  INDEX BY PLS_INTEGER;
  TYPE gt_char_arr           IS TABLE OF CHAR(1)                            INDEX BY PLS_INTEGER;
  TYPE gt_raw_arr            IS TABLE OF RAW(256)                           INDEX BY PLS_INTEGER;
  TYPE gt_lraw_arr           IS TABLE OF RAW(4000)                          INDEX BY PLS_INTEGER;
  TYPE gt_xlraw_arr          IS TABLE OF RAW(32767)                         INDEX BY PLS_INTEGER;
  TYPE gt_interval_ym_arr    IS TABLE OF INTERVAL YEAR(9) TO MONTH          INDEX BY PLS_INTEGER;
  TYPE gt_interval_ds_arr    IS TABLE OF INTERVAL DAY(9) TO SECOND (9)      INDEX BY PLS_INTEGER;
  TYPE gt_clob_arr           IS TABLE OF CLOB                               INDEX BY PLS_INTEGER;
  TYPE gt_blob_arr           IS TABLE OF BLOB                               INDEX BY PLS_INTEGER;
  TYPE gt_xml_arr            IS TABLE OF XMLType                            INDEX BY PLS_INTEGER;
  TYPE gt_rowid_arr          IS TABLE OF ROWID                              INDEX BY PLS_INTEGER;
  TYPE gt_urowid_arr         IS TABLE OF UROWID                             INDEX BY PLS_INTEGER;

  TYPE gt_name_indx          IS TABLE OF gt_name                            INDEX BY VARCHAR2(32767);
  TYPE gt_str_indx           IS TABLE OF VARCHAR2(30)                       INDEX BY VARCHAR2(32767);
  TYPE gt_mstr_indx          IS TABLE OF VARCHAR2(256)                      INDEX BY VARCHAR2(32767);
  TYPE gt_lstr_indx          IS TABLE OF VARCHAR2(4000)                     INDEX BY VARCHAR2(32767);
  TYPE gt_xlstr_indx         IS TABLE OF VARCHAR2(32767)                    INDEX BY VARCHAR2(32767);
  TYPE gt_date_indx          IS TABLE OF DATE                               INDEX BY VARCHAR2(32767);
  TYPE gt_num_indx           IS TABLE OF NUMBER                             INDEX BY VARCHAR2(32767);
  TYPE gt_int_indx           IS TABLE OF PLS_INTEGER                        INDEX BY VARCHAR2(32767);
  TYPE gt_timestamp_indx     IS TABLE OF TIMESTAMP(9)                       INDEX BY VARCHAR2(32767);
  TYPE gt_timestamp_tz_indx  IS TABLE OF TIMESTAMP(9) WITH TIME ZONE        INDEX BY VARCHAR2(32767);
  TYPE gt_timestamp_ltz_indx IS TABLE OF TIMESTAMP(9) WITH LOCAL TIME ZONE  INDEX BY VARCHAR2(32767);
  TYPE gt_char_indx          IS TABLE OF CHAR(1)                            INDEX BY VARCHAR2(32767);
  TYPE gt_raw_indx           IS TABLE OF RAW(256)                           INDEX BY VARCHAR2(32767);
  TYPE gt_lraw_indx          IS TABLE OF RAW(4000)                          INDEX BY VARCHAR2(32767);
  TYPE gt_xlraw_indx         IS TABLE OF RAW(32767)                         INDEX BY VARCHAR2(32767);
  TYPE gt_interval_ym_indx   IS TABLE OF INTERVAL YEAR(9) TO MONTH          INDEX BY VARCHAR2(32767);
  TYPE gt_interval_ds_indx   IS TABLE OF INTERVAL DAY(9) TO SECOND (9)      INDEX BY VARCHAR2(32767);
  TYPE gt_clob_indx          IS TABLE OF CLOB                               INDEX BY VARCHAR2(32767);
  TYPE gt_blob_indx          IS TABLE OF BLOB                               INDEX BY VARCHAR2(32767);
  TYPE gt_xml_indx           IS TABLE OF XMLType                            INDEX BY VARCHAR2(32767);
  TYPE gt_rowid_indx         IS TABLE OF ROWID                              INDEX BY VARCHAR2(32767);
  TYPE gt_urowid_indx        IS TABLE OF UROWID                             INDEX BY VARCHAR2(32767);

  TYPE gt_name_tab           IS TABLE OF gt_name;
  TYPE gt_str_tab            IS TABLE OF VARCHAR2(30);
  TYPE gt_mstr_tab           IS TABLE OF VARCHAR2(256);
  TYPE gt_lstr_tab           IS TABLE OF VARCHAR2(4000);
  TYPE gt_xlstr_tab          IS TABLE OF VARCHAR2(32767);
  TYPE gt_date_tab           IS TABLE OF DATE;
  TYPE gt_num_tab            IS TABLE OF NUMBER;
  TYPE gt_int_tab            IS TABLE OF PLS_INTEGER;
  TYPE gt_timestamp_tab      IS TABLE OF TIMESTAMP(9);
  TYPE gt_timestamp_tz_tab   IS TABLE OF TIMESTAMP(9) WITH TIME ZONE;
  TYPE gt_timestamp_ltz_tab  IS TABLE OF TIMESTAMP(9) WITH LOCAL TIME ZONE;
  TYPE gt_char_tab           IS TABLE OF CHAR(1);
  TYPE gt_raw_tab            IS TABLE OF RAW(256);
  TYPE gt_lraw_tab           IS TABLE OF RAW(4000);
  TYPE gt_xlraw_tab          IS TABLE OF RAW(32767);
  TYPE gt_interval_ym_tab    IS TABLE OF INTERVAL YEAR(9) TO MONTH;
  TYPE gt_interval_ds_tab    IS TABLE OF INTERVAL DAY(9) TO SECOND (9);
  TYPE gt_clob_tab           IS TABLE OF CLOB;
  TYPE gt_blob_tab           IS TABLE OF BLOB;
  TYPE gt_xml_tab            IS TABLE OF XMLType;
  TYPE gt_rowid_tab          IS TABLE OF ROWID;
  TYPE gt_urowid_tab         IS TABLE OF UROWID;
  
  FUNCTION init_str_indx           (in_indices IN gt_xlstr_tab, in_values IN gt_str_tab DEFAULT NULL) RETURN gt_str_indx;
  FUNCTION init_mstr_indx          (in_indices IN gt_xlstr_tab, in_values IN gt_mstr_tab DEFAULT NULL) RETURN gt_mstr_indx;
  FUNCTION init_lstr_indx          (in_indices IN gt_xlstr_tab, in_values IN gt_lstr_tab DEFAULT NULL) RETURN gt_lstr_indx;
  FUNCTION init_xlstr_indx         (in_indices IN gt_xlstr_tab, in_values IN gt_xlstr_tab DEFAULT NULL) RETURN gt_xlstr_indx;
  FUNCTION init_date_indx          (in_indices IN gt_xlstr_tab, in_values IN gt_date_tab DEFAULT NULL) RETURN gt_date_indx;
  FUNCTION init_num_indx           (in_indices IN gt_xlstr_tab, in_values IN gt_num_tab DEFAULT NULL) RETURN gt_num_indx;
  FUNCTION init_int_indx           (in_indices IN gt_xlstr_tab, in_values IN gt_int_tab DEFAULT NULL) RETURN gt_int_indx;
  FUNCTION init_timestamp_indx     (in_indices IN gt_xlstr_tab, in_values IN gt_timestamp_tab DEFAULT NULL) RETURN gt_timestamp_indx;
  FUNCTION init_timestamp_tz_indx  (in_indices IN gt_xlstr_tab, in_values IN gt_timestamp_tz_tab DEFAULT NULL) RETURN gt_timestamp_tz_indx;
  FUNCTION init_timestamp_ltz_indx (in_indices IN gt_xlstr_tab, in_values IN gt_timestamp_ltz_tab DEFAULT NULL) RETURN gt_timestamp_ltz_indx;
  FUNCTION init_char_indx          (in_indices IN gt_xlstr_tab, in_values IN gt_char_tab DEFAULT NULL) RETURN gt_char_indx;
  FUNCTION init_raw_indx           (in_indices IN gt_xlstr_tab, in_values IN gt_raw_tab DEFAULT NULL) RETURN gt_raw_indx;
  FUNCTION init_lraw_indx          (in_indices IN gt_xlstr_tab, in_values IN gt_lraw_tab DEFAULT NULL) RETURN gt_lraw_indx;
  FUNCTION init_xlraw_indx         (in_indices IN gt_xlstr_tab, in_values IN gt_xlraw_tab DEFAULT NULL) RETURN gt_xlraw_indx;
  FUNCTION init_interval_ym_indx   (in_indices IN gt_xlstr_tab, in_values IN gt_interval_ym_tab DEFAULT NULL) RETURN gt_interval_ym_indx;
  FUNCTION init_interval_ds_indx   (in_indices IN gt_xlstr_tab, in_values IN gt_interval_ds_tab DEFAULT NULL) RETURN gt_interval_ds_indx;
  FUNCTION init_clob_indx          (in_indices IN gt_xlstr_tab, in_values IN gt_clob_tab DEFAULT NULL) RETURN gt_clob_indx;
  FUNCTION init_blob_indx          (in_indices IN gt_xlstr_tab, in_values IN gt_blob_tab) RETURN gt_blob_indx;
  FUNCTION init_xml_indx           (in_indices IN gt_xlstr_tab, in_values IN gt_xml_tab) RETURN gt_xml_indx;
  FUNCTION init_rowid_indx         (in_indices IN gt_xlstr_tab, in_values IN gt_rowid_tab DEFAULT NULL) RETURN gt_rowid_indx;
  FUNCTION init_urowid_indx        (in_indices IN gt_xlstr_tab, in_values IN gt_urowid_tab DEFAULT NULL) RETURN gt_urowid_indx;


END arrays;
/