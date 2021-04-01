CREATE OR REPLACE PACKAGE cort_aux_pkg 
AS

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
  Description: Auxilary functionality executed with CORT user privileges
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support of explain plan option, single schema mode and build functionality
  15.00   | Rustam Kafarov    | Added manual execution
  17.00   | Rustam Kafarov    | Moved API enable_cort/disable_cort/get_status to cort_pkg. 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  TYPE gt_object_metadata IS TABLE OF cort_objects%ROWTYPE INDEX BY PLS_INTEGER;

  /* Converts CLOB to strings arrays dbms_sql.varchar2a */
  PROCEDURE clob_to_varchar2a(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY dbms_sql.varchar2a
  );

  /* Converts CLOB to strings arrays */
  PROCEDURE clob_to_lines(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY arrays.gt_xlstr_arr
  );
  
  /* Converts strings arrays to CLOB */
  PROCEDURE lines_to_clob(
    in_str_arr  IN arrays.gt_lstr_arr,
    in_delim    IN VARCHAR2 DEFAULT CHR(10),
    out_clob    OUT NOCOPY CLOB
  );

  -- Check if executed SQL and objects's last_ddl_time match to the last registered corresponded parameters 
  FUNCTION is_object_modified(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_sql_text     IN CLOB
  )
  RETURN BOOLEAN;

  -- Check if objects was renamed in given release  
  FUNCTION is_object_renamed(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_rename_name  IN VARCHAR2
  )
  RETURN BOOLEAN;

  PROCEDURE reverse_array(
    io_array IN OUT NOCOPY arrays.gt_clob_arr
  );

  -- delete all entries for given object from cort_objects table 
  PROCEDURE cleanup_history(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  );

  -- retrun record for the last registered change for given object
  FUNCTION get_last_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_objects%ROWTYPE;

  -- return the name of the last backup (revert) table name
  FUNCTION get_last_revert_name(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN VARCHAR2;

  FUNCTION get_object_metadata(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_application   IN VARCHAR2, 
    in_release       IN VARCHAR2, 
    in_build         IN VARCHAR2 DEFAULT NULL 
  )
  RETURN gt_object_metadata;

  -- register object change in cort_objects table
  PROCEDURE register_change(
    in_object_type    IN VARCHAR2,
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_job_rec        IN cort_jobs%ROWTYPE,
    in_sql            IN CLOB,
    in_change_type    IN NUMBER,
    in_revert_name    IN VARCHAR2,
    in_last_ddl_index IN PLS_INTEGER,
    in_frwd_stmt_arr  IN arrays.gt_clob_arr,
    in_rlbk_stmt_arr  IN arrays.gt_clob_arr,
    in_rename_name    IN VARCHAR2 DEFAULT NULL
  );

  PROCEDURE update_change(
    in_rec IN cort_objects%ROWTYPE
  );

  -- unregister object last change from cort_objects table
  PROCEDURE unregister_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_exec_time     IN TIMESTAMP
  );
  
  PROCEDURE copy_stats(
    in_source_table_name IN VARCHAR2,
    in_target_table_name IN VARCHAR2,
    in_source_part       IN VARCHAR2 DEFAULT NULL,
    in_target_part       IN VARCHAR2 DEFAULT NULL,
    in_part_level        IN VARCHAR2 DEFAULT NULL
  );
  
  -- Sets value to context
  PROCEDURE set_context(
    in_name  IN VARCHAR2,
    in_value IN VARCHAR2
  );

  -- Gets value of CORT context
  FUNCTION get_context(
    in_name  IN VARCHAR2
  )
  RETURN VARCHAR2;
  
  -- API for release management
  PROCEDURE check_application(
    in_application  IN VARCHAR2
  );
  
  -- return record for current release
  FUNCTION get_curr_release_rec(
    in_application IN VARCHAR2
  )
  RETURN cort_releases%ROWTYPE;

  FUNCTION get_prev_release_rec(
    in_application IN VARCHAR2
  )
  RETURN cort_releases%ROWTYPE;

  -- close record for current release and insert record for new release
  PROCEDURE register_new_release(
    in_application IN VARCHAR2,
    in_release     IN VARCHAR2
  );

  -- return compact unique string for current date-time  
  FUNCTION get_time_str
  RETURN VARCHAR2;
  
  -- Generates globally unique build ID
  FUNCTION gen_build_id
  RETURN VARCHAR2;

  -- Find and return active or stale build for given application
  FUNCTION find_build(in_application IN VARCHAR2)
  RETURN cort_builds%ROWTYPE;

  -- Return build record by build ID
  FUNCTION get_build_rec(
    in_build IN VARCHAR2
  )
  RETURN cort_builds%ROWTYPE;

  -- wrapper returns 'TRUE'/'FALSE' 
  FUNCTION is_session_alive(in_session_id IN VARCHAR2)
  RETURN VARCHAR2;  

  -- Adds new records into CORT_BUILDS table
  FUNCTION create_build(in_application IN VARCHAR2)
  RETURN cort_builds%ROWTYPE;

  -- Update record in CORT_BUILDS table
  PROCEDURE update_build(
    in_row IN cort_builds%ROWTYPE
  );

END cort_aux_pkg;
/