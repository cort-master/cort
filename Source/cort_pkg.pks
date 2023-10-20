CREATE OR REPLACE PACKAGE cort_pkg
AUTHID CURRENT_USER
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
  Description: API for end-user - wrappers around main procedures/functions.
  ----------------------------------------------------------------------------------------------------------------------
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added build API
  15.00   | Rustam Kafarov    | Added API for manual execution
  19.00   | Rustam Kafarov    | Revised parameters
  20.00   | Rustam Kafarov    | Added support of long names introduced in Oracle 12.2 
  21.00   | Rustam Kafarov    | Added get_cort_ddl, install, deinstall, compare_release_numbers routines
  ----------------------------------------------------------------------------------------------------------------------
*/

  /* This package will be granted to public */

  -- API to enable/disable CORT permanently

  -- permanently enable CORT
  PROCEDURE enable;

  -- permanently disable CORT
  PROCEDURE disable;

  -- get CORT status (ENABLED/DISABLED/DISABLED FOR SESSION)
  FUNCTION get_status
  RETURN VARCHAR2;

  -- API for revert DDL changes

  -- Revert the latest change for given object (overloaded)
  PROCEDURE revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  );

  -- Wrapper for tables
  PROCEDURE revert_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  );

  -- revert last change in current session
  PROCEDURE revert(
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  );

  -- resume pending changes for given object
  PROCEDURE resume(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );

  -- API to modify database objects

  -- drop object if it exists
  PROCEDURE drop_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_purge        IN BOOLEAN DEFAULT NULL -- NULL -  take from session paream
  );

  -- Wrapper for tables
  PROCEDURE drop_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_purge        IN BOOLEAN DEFAULT NULL -- NULL -  take from session paream
  );

  -- Warpper for tables
  PROCEDURE drop_revert_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_purge        IN BOOLEAN DEFAULT NULL -- NULL -  take from session paream
  );

  -- disable all foreign keys on given table
  PROCEDURE disable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );

  -- enable all foreign keys on given table
  PROCEDURE enable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_validate   IN BOOLEAN DEFAULT TRUE
  );

  -- Rename table and it's constraints and indexes
  PROCEDURE rename_table(
    in_table_name IN VARCHAR2,
    in_new_name   IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );

  -- API for release management

  -- start new release
  PROCEDURE start_release(
    in_release     IN VARCHAR2
  );

  -- return current release
  FUNCTION get_current_release
  RETURN VARCHAR2;

  -- API to biuld management

  -- start new build
  PROCEDURE start_build;

  -- finish build
  PROCEDURE end_build;

  -- fail build
  PROCEDURE fail_build;

  -- rollback all DDL changes made within build
  PROCEDURE revert_build(in_build IN VARCHAR2 DEFAULT NULL);

  -- Create error table for given table
  PROCEDURE create_error_table(
    in_main_table_name IN VARCHAR2,
    in_err_table_name  IN VARCHAR2,
    in_table_owner     IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_system_columns  IN arrays.gt_str_tab DEFAULT NULL
  );

  -- Return DDL with /*# OR REPLACE */ hint for existing table
  FUNCTION get_cort_ddl(
    in_table_name          IN VARCHAR2, 
    in_table_owner         IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_all_partitions      IN BOOLEAN DEFAULT TRUE,
    in_segment_attributes  IN BOOLEAN DEFAULT FALSE,
    in_storage             IN BOOLEAN DEFAULT FALSE, 
    in_tablespace          IN BOOLEAN DEFAULT FALSE,
    in_size_byte_keyword   IN BOOLEAN DEFAULT FALSE
  )
  RETURN CLOB;
  
  -- Self installation API

  -- drop CORT triggers
  PROCEDURE drop_cort_triggers;

  -- create CORT triggers
  PROCEDURE create_cort_triggers;
  
  -- install cort synonyms, triggers, grants into separate schema 
  PROCEDURE install;
  
  -- deinstall cort synonyms, triggers, grants from separate schema 
  PROCEDURE uninstall;
  
  -- compares release numbers and returns -1 if release1 less than release2, +1 if release1 more than release2, otherwise 0
  FUNCTION compare_release_numbers(in_release1 IN VARCHAR2, in_release2 in VARCHAR2) RETURN NUMBER;
  
END cort_pkg;
/ 