CREATE OR REPLACE PACKAGE BODY cort_trg_pkg
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
  Description: functionality called from create trigger     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support for new objects types and explain plan functionality extension
  15.00   | Rustam Kafarov    | Added alternative to CONTEXT using
  17.00   | Rustam Kafarov    | Added FOR REPLACE option for RENAME DDL. Removed hard references
  20.00   | Rustam Kafarov    | Added support of long names in Oracle 12.2 
  21.00   | Rustam Kafarov    | Fixed regexp for single line comment 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_last_id             NUMBER := 0;
  
  -- parses main CORT hint
  FUNCTION is_replace_mode(
    in_sql         IN CLOB
  )
  RETURN BOOLEAN
  AS
    l_prfx           VARCHAR2(1);
    l_create_expr    VARCHAR2(20);
    l_regexp         VARCHAR2(1000);
  BEGIN
    l_prfx := '#';
    l_create_expr := '^\s*CREATE\s*';
    l_regexp := '('||l_create_expr||'\/\*'||l_prfx||'\s*OR\s+REPLACE\W)|'||
                '('||l_create_expr|| '--' ||l_prfx||'[ \t]*OR[ \t]+REPLACE\W)';
    RETURN REGEXP_INSTR(in_sql, l_regexp, 1, 1, 0, 'imn') = 1;
  END is_replace_mode;

 -- Function returns currently executing ddl statement. It could be called only from DDL triggers
  FUNCTION ora_dict_ddl
  RETURN CLOB
  AS
    l_ddl_arr   dbms_standard.ora_name_list_t;
    l_ddl       CLOB;
    l_cnt       PLS_INTEGER;
  BEGIN
    l_cnt := ora_sql_txt(l_ddl_arr);
    IF l_ddl_arr IS NOT NULL THEN
      FOR i IN 1..l_cnt LOOP
        -- TRIM(CHR(0) is workaroung to remove trailing #0 symbol. This symbol breask down convertion into XML 
        l_ddl := l_ddl || TRIM(CHR(0) FROM l_ddl_arr(i));
      END LOOP;
    END IF;
    -- TRIM(CHR(10) is workaroung to remove leading #10 symbol added (sometimnes?!) by Command Windon of PL/SQL Developer 
    RETURN TRIM(chr(10) from l_ddl);
  END ora_dict_ddl;
  
  -- Gets value of CORT context
  FUNCTION get_context(
    in_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(4000);
  BEGIN
    EXECUTE IMMEDIATE '  BEGIN
    :out := cort_aux_pkg.get_context(
      in_name    => :in_name
    );
  END;' 
    USING OUT l_result, IN in_name;
    RETURN l_result;
  END get_context;

  FUNCTION get_status
  RETURN VARCHAR2
  AS
    l_status VARCHAR2(10);
  BEGIN
    IF get_context('DISABLED_FOR_SESSION') = 'TRUE' THEN
      l_status := 'DISABLED';
    ELSE
      l_status := 'ENABLED';
    END IF;  
    RETURN l_status;
  END get_status;

  -- Returns 'REPLACE' if there is #OR REPLACE hint in given DDL or if this parameter is turned on for session.
  -- Otherwise returns 'CREATE'. It could be called only from DDL triggers
  FUNCTION get_execution_mode(
    in_object_type IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(10);
    l_ddl    clob;
  BEGIN
    l_ddl := ora_dict_ddl;
    CASE  
    WHEN in_object_type IN ('TABLE', 'SEQUENCE', 'TYPE', 'INDEX', 'TYPE BODY') THEN 
      IF is_replace_mode(l_ddl)  
      THEN
        l_result := 'REPLACE';
      ELSE
        l_result := 'CREATE';
      END IF;
    ELSE 
      IF (SYS_CONTEXT('USERENV', 'CLIENT_INFO') LIKE 'CORT_BUILD=%') THEN
        l_result := 'REPLACE';
      ELSE
        l_result := 'CREATE';
      END IF;
    END CASE;  
    RETURN l_result;
  END get_execution_mode;
  
  -- called from instead of trigger
  PROCEDURE instead_of_create
  AS
  BEGIN
    EXECUTE IMMEDIATE q'{  BEGIN
    cort_event_exec_pkg.instead_of_create(
      in_object_type    => :in_ora_dict_obj_type,
      in_object_name    => :in_ora_dict_obj_name,
      in_object_owner   => :in_ora_dict_obj_owner,
      in_sql            => :in_ora_dict_ddl
    );
  END;}' USING ora_dict_obj_type,
               ora_dict_obj_name,
               ora_dict_obj_owner,               
               ora_dict_ddl;
  END instead_of_create;
  
  -- called from before create trigger
  PROCEDURE before_create
  AS
  BEGIN
    EXECUTE IMMEDIATE q'{  BEGIN
    cort_event_exec_pkg.before_create(
      in_object_type    => :in_ora_dict_obj_type,
      in_object_name    => :in_ora_dict_obj_name,
      in_object_owner   => :in_ora_dict_obj_owner,
      in_sql            => :in_ora_dict_ddl
    );
  END;}' USING ora_dict_obj_type,
               ora_dict_obj_name,
               NVL(ora_dict_obj_owner,'"NULL"'),
               ora_dict_ddl;     
  END before_create;

  -- called from row-level trigger on plan table
  PROCEDURE before_insert_xplan(
    io_id           IN OUT INTEGER,
    io_parent_id    IN OUT INTEGER,
    io_depth        IN OUT INTEGER,  
    io_operation    IN OUT VARCHAR2, 
    in_statement_id IN VARCHAR2,
    in_plan_id      IN NUMBER,
    in_timestamp    IN TIMESTAMP, 
    out_other_xml   OUT NOCOPY CLOB,  
    out_revert_ddl  OUT NOCOPY VARCHAR2
  )
  AS
  BEGIN
    IF io_id = 0 THEN
      g_last_id := 0;
      IF io_operation = 'CREATE TABLE STATEMENT' THEN
        EXECUTE IMMEDIATE q'{  BEGIN
    cort_event_exec_pkg.explain_cort_sql(
      in_statement_id  => :in_statement_id,
      in_plan_id       => :in_plan_id,
      in_timestamp     => :in_timestamp,
      out_sql          => :out_other_xml,
      out_last_id      => :g_last_id
    );
  END;}' USING IN in_statement_id, IN in_plan_id, IN in_timestamp, OUT out_other_xml, OUT g_last_id;  
        io_operation := 'CREATE OR REPLACE TABLE (CORT) STATEMENT plan_id ='||to_char(in_plan_id);
        out_revert_ddl := 'REVERT DDL';
      END IF;
    ELSE
      IF g_last_id > 0 AND io_parent_id = 0 THEN 
        io_parent_id := g_last_id;
      END IF;     
    END IF;
  END before_insert_xplan;

  -- called from before DDL trigger for tables
  PROCEDURE lock_object
  AS
    l_lock_error VARCHAR2(1024);
  BEGIN
    BEGIN
      EXECUTE IMMEDIATE q'{  BEGIN
      cort_event_exec_pkg.lock_object(
        in_object_type    => :in_ora_dict_obj_type,
        in_object_name    => :in_ora_dict_obj_name,
        in_object_owner   => :in_ora_dict_obj_owner,
        out_lock_error    => :out_lock_error
      );
    END;}' USING ora_dict_obj_type,
                 ora_dict_obj_name,
                 NVL(ora_dict_obj_owner,'"NULL"'),
             OUT l_lock_error;
    EXCEPTION
      WHEN OTHERS THEN
        l_lock_error := NULL; 
    END;
    IF l_lock_error IS NOT NULL THEN
      raise_application_error(-20000, l_lock_error);
    END IF;         
  END lock_object;


END cort_trg_pkg;
/