CREATE OR REPLACE PACKAGE BODY cort_aux_pkg 
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
  Description: Auxilary functionality executed with CORT user privileges
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support of explain plan option, single schema mode and build functionality
  15.00   | Rustam Kafarov    | Added manual execution
  17.00   | Rustam Kafarov    | Moved API enable_cort/disable_cort/get_status to cort_pkg. 
  19.00   | Rustam Kafarov    | Revised parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

      
  /* Converts CLOB to strings arrays dbms_sql.varchar2a */
  PROCEDURE clob_to_varchar2a(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY dbms_sql.varchar2a
  )
  AS
    l_len      PLS_INTEGER;
    l_offset   PLS_INTEGER;
    l_amount   PLS_INTEGER;
    l_str      VARCHAR2(32767);
  BEGIN
    l_offset := 1;
    l_amount := 32767;
    l_len := dbms_lob.getlength(in_clob);
    WHILE l_offset <= l_len LOOP
      l_str := dbms_lob.substr(in_clob,l_amount,l_offset);
      out_str_arr(out_str_arr.COUNT+1) := l_str;
      l_offset := l_offset + l_amount;
    END LOOP;
  END clob_to_varchar2a;

  /* Converts CLOB to strings arrays dbmsoutput_linesarray */
  PROCEDURE clob_to_lines(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY arrays.gt_xlstr_arr
  )
  AS
    l_len      PLS_INTEGER;
    l_offset   PLS_INTEGER;
    l_amount   PLS_INTEGER;
    l_str      VARCHAR2(32767);
    l_leftover VARCHAR2(32767);
    l_cnt      PLS_INTEGER;
    l_pos      PLS_INTEGER;
  BEGIN
    l_offset := 1;
    l_amount := 32700;
    l_len := dbms_lob.getlength(in_clob);
    l_cnt := 0;
    l_leftover := NULL;
    WHILE l_offset <= l_len LOOP
      l_amount := 32700 - nvl(length(l_leftover),0);
      l_str := null;
      l_str := dbms_lob.substr(in_clob,l_amount,l_offset);
      l_str := l_leftover||l_str;
      l_pos := INSTR(l_str, CHR(10), -1);
      IF l_pos > 0 THEN
        l_leftover := SUBSTR(l_str, l_pos+1);
        l_str := SUBSTR(l_str, 1, l_pos-1);
      ELSE
        l_leftover := null;  
      END IF;
      l_offset := l_offset + l_amount;
      l_cnt := l_cnt + 1;
      out_str_arr(l_cnt) := l_str;
    END LOOP;
    IF l_leftover IS NOT NULL THEN
      l_cnt := l_cnt + 1;
      out_str_arr(l_cnt) := l_leftover;
    END IF;  
  END clob_to_lines;


  PROCEDURE output(in_value IN CLOB)
  AS
    l_lines      arrays.gt_xlstr_arr;
  BEGIN
    clob_to_lines(
      in_clob     => in_value,
      out_str_arr => l_lines
    );
    dbms_output.enable(buffer_size => 1000000);
    FOR i IN 1..l_lines.COUNT LOOP
      dbms_output.put_line(l_lines(i));
    END LOOP;
  END output;


  /* Converts strings arrays dbmsoutput_linesarray to CLOB */

  PROCEDURE lines_to_clob(
    in_str_arr  IN arrays.gt_lstr_arr,
    in_delim    IN VARCHAR2 DEFAULT CHR(10),
    out_clob    OUT NOCOPY CLOB
  )
  AS
  BEGIN
    FOR i IN 1..in_str_arr.COUNT LOOP
      out_clob := out_clob||in_str_arr(i);
      IF i < in_str_arr.COUNT THEN
        out_clob := out_clob||in_delim;
      END IF;  
    END LOOP;
  END lines_to_clob;
  
  PROCEDURE reverse_array(
    io_array IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_indx_frwd PLS_INTEGER;
    l_indx_bkwd PLS_INTEGER;
    l_swap      CLOB;
  BEGIN
    l_indx_frwd := io_array.FIRST;
    l_indx_bkwd := io_array.LAST;
    WHILE l_indx_frwd < l_indx_bkwd AND 
          l_indx_frwd IS NOT NULL AND 
          l_indx_bkwd IS NOT NULL 
    LOOP
      l_swap := io_array(l_indx_frwd);
      io_array(l_indx_frwd) := io_array(l_indx_bkwd);
      io_array(l_indx_bkwd) := l_swap; 
      l_indx_frwd := io_array.NEXT(l_indx_frwd);
      l_indx_bkwd := io_array.PRIOR(l_indx_bkwd);
    END LOOP;
  END reverse_array;

  PROCEDURE cleanup_history(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DELETE FROM cort_objects 
     WHERE object_owner = in_object_owner 
       AND object_name = in_object_name 
       AND object_type = in_object_type
       AND NVL(build,'NULL') <> NVL(SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12),'{null}');

    COMMIT;
  END cleanup_history;

  FUNCTION get_last_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_objects%ROWTYPE
  AS
    l_rec              cort_objects%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec 
        FROM (SELECT * 
                FROM cort_objects 
               WHERE object_owner = in_object_owner 
                 AND object_name = in_object_name 
                 AND object_type = in_object_type
               ORDER BY job_id DESC NULLS LAST
             )
       WHERE ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;    
  
    RETURN l_rec;
  END get_last_change; 

  FUNCTION get_last_revert_name(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_rec    cort_objects%ROWTYPE;
  BEGIN
    l_rec := get_last_change(
               in_object_type   => in_object_type,
               in_object_name   => in_object_name,
               in_object_owner  => in_object_owner
             );
  
    RETURN l_rec.revert_name;
  END get_last_revert_name; 

  FUNCTION get_object_changes(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_application   IN VARCHAR2, 
    in_release       IN VARCHAR2, 
    in_build         IN VARCHAR2 DEFAULT NULL 
  )
  RETURN gt_object_changes
  AS
    l_object_metadata  gt_object_changes;
  BEGIN
    SELECT *
      BULK COLLECT 
      INTO l_object_metadata 
      FROM cort_objects 
     WHERE object_owner = nvl(in_object_owner,object_owner)  
       AND object_name = nvl(in_object_name,object_name) 
       AND object_type = nvl(in_object_type,object_type)
       AND nvl(application, '<unknown>') = nvl(in_application, '<unknown>') 
       AND nvl(release, '<unknown>') = nvl(in_release, '<unknown>')
       AND nvl2(in_build, build, '<unknown>') = nvl(in_build, '<unknown>')
     ORDER BY job_id;
     
    RETURN l_object_metadata;
  END get_object_changes; 

  -- register object change in cort_objects, cort_sql tables 
  PROCEDURE register_change(
    in_job_rec        IN cort_jobs%ROWTYPE,
    in_change_type    IN NUMBER,
    in_revert_name    IN VARCHAR2 DEFAULT NULL
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_new_rec       cort_objects%ROWTYPE;
    l_change_type   VARCHAR2(30);
  BEGIN
   
    IF in_change_type > cort_comp_pkg.gc_result_nochange OR
      -- actual change
       get_last_change(
         in_object_type  => in_job_rec.object_type,
         in_object_name  => in_job_rec.object_name,
         in_object_owner => in_job_rec.object_owner
       ).job_id IS NULL
       -- no previous history
    THEN
      l_change_type := cort_comp_pkg.get_result_name(in_change_type);

      l_new_rec.job_id := in_job_rec.job_id;
      l_new_rec.object_owner := in_job_rec.object_owner; 
      l_new_rec.object_name := in_job_rec.object_name; 
      l_new_rec.object_type := in_job_rec.object_type;
      l_new_rec.last_ddl_text := in_job_rec.sql_text; 
      l_new_rec.last_ddl_time := SYSTIMESTAMP; 
      l_new_rec.change_params := cort_params_pkg.write_to_xml(cort_exec_pkg.g_params);  
      l_new_rec.application := cort_exec_pkg.g_run_params.application.get_value; 
      l_new_rec.release := cort_exec_pkg.g_run_params.release.get_value;  
      l_new_rec.build := cort_exec_pkg.g_run_params.build.get_value; 
      l_new_rec.change_type := l_change_type;
      l_new_rec.revert_name := in_revert_name;
      l_new_rec.prev_synonym := cort_exec_pkg.get_prev_synonym_name(in_job_rec.object_name);

      INSERT INTO cort_objects VALUES l_new_rec;
      
      IF in_job_rec.object_type = 'TYPE' AND in_revert_name IS NOT NULL THEN
        INSERT 
          INTO cort_type_synonyms(job_id, synonym_owner, synonym_name, type_name)
        VALUES (in_job_rec.job_id, in_job_rec.object_owner, in_job_rec.object_name, in_revert_name);      
      END IF;
    ELSE 
      -- just update DDL text and DDL time 
      UPDATE cort_objects
         SET last_ddl_text = in_job_rec.sql_text, 
             last_ddl_time = SYSTIMESTAMP
       WHERE object_type  = in_job_rec.object_type
         AND object_name  = in_job_rec.object_name
         AND object_owner = in_job_rec.object_owner
         AND job_id in (SELECT max(job_id) FROM cort_objects
                         WHERE object_type  = in_job_rec.object_type
                           AND object_name  = in_job_rec.object_name
                           AND object_owner = in_job_rec.object_owner); 
    END IF;
    
    COMMIT;
  END register_change;
  

  -- return array of change sql assosiated with given job
  FUNCTION get_change_sql(
    in_job_id         IN TIMESTAMP
  ) 
  RETURN cort_exec_pkg.gt_change_arr
  AS
    l_result cort_exec_pkg.gt_change_arr;
  BEGIN
    SELECT group_type, 
           change_sql, 
           revert_sql,
           change_sql as display_sql, 
           status, 
           start_time, 
           end_time, 
           revert_start_time, 
           revert_end_time, 
           threadable, 
           error_msg,
           rowid
      BULK COLLECT
      INTO l_result 
      FROM cort_sql
     WHERE job_id = in_job_id
     ORDER by seq_num;
      
    RETURN l_result;  
  END get_change_sql;


  PROCEDURE unregister_change(
    in_job_id         IN TIMESTAMP,
    in_object_type    IN VARCHAR2,
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DELETE FROM cort_objects 
     WHERE job_id = in_job_id
       AND object_type  = in_object_type
       AND object_name  = in_object_name
       AND object_owner = in_object_owner;

    -- for types synonyms
    IF in_object_type = 'TYPE' THEN
      -- revert last change
      DELETE FROM cort_type_synonyms
       WHERE job_id = in_job_id
         AND synonym_name  = in_object_name
         AND synonym_owner  = in_object_owner; 
    END IF;
    
    COMMIT;
  END unregister_change;

  -- Check if objects was renamed in given release  
  FUNCTION is_object_renamed(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_rename_name  IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt           NUMBER;
    l_result        BOOLEAN;
    l_change_name   VARCHAR2(30);
  BEGIN
    l_result := TRUE;
    l_change_name := cort_comp_pkg.get_result_name(cort_comp_pkg.gc_result_rename);
    SELECT COUNT(*)
      INTO l_cnt 
      FROM cort_objects 
     WHERE object_owner = in_object_owner  
       AND object_name = in_object_name 
       AND object_type = in_object_type
       AND revert_name = in_rename_name
       and change_type = l_change_name
       AND nvl(release, '<unknown>') = nvl(cort_pkg.get_current_release, '<unknown>');

    RETURN l_cnt > 0;
  END is_object_renamed;

  PROCEDURE copy_stats(
    in_source_table_name IN VARCHAR2,
    in_target_table_name IN VARCHAR2,
    in_source_part       IN VARCHAR2 DEFAULT NULL,
    in_target_part       IN VARCHAR2 DEFAULT NULL,
    in_part_level        IN VARCHAR2 DEFAULT NULL
  )
  AS
    l_col_list VARCHAR2(4000);
    l_sql      VARCHAR2(32767);
  BEGIN
    FOR x IN (SELECT column_name
                FROM user_tab_columns
               WHERE table_name = 'CORT_STAT'
                 AND column_name NOT IN ('C1', 'C2', 'C3')
               ORDER BY column_id
             ) 
    LOOP
      l_col_list := l_col_list||', '||x.column_name;
    END LOOP; 
    
    IF in_source_part IS NOT NULL AND in_target_part IS NOT NULL THEN
      IF in_part_level = 'PARTITION' THEN
        l_sql := '
        INSERT INTO cort_stat(c1, c2, c3'||l_col_list||')
        SELECT :in_target_table_name as c1, :in_target_part as c2, null as c3'||l_col_list||' 
          FROM cort_stat  
         WHERE c1 = :in_source_table_name
           and c2 = :in_source_part';
      ELSE

        l_sql := '
        INSERT INTO cort_stat(c1, c2, c3'||l_col_list||')
        SELECT :in_target_table_name as c1, null as c2, :in_target_part as c3'||l_col_list||' 
          FROM cort_stat  
         WHERE c1 = :in_source_table_name
           and c3 = :in_source_part';
      END IF;     
      EXECUTE IMMEDIATE l_sql USING in_target_table_name, in_target_part, in_source_table_name, in_source_part;  
      COMMIT;
    ELSE
      l_sql := '
      INSERT INTO cort_stat(c1, c2, c3'||l_col_list||')
      SELECT :in_target_table_name as c1, c2, c3'||l_col_list||' 
        FROM cort_stat  
       WHERE c1 = :in_source_table_name';

      EXECUTE IMMEDIATE l_sql USING in_target_table_name, in_source_table_name;  
      COMMIT;
    END IF;  
  END copy_stats;


  -- write value to CORT context table
  PROCEDURE write_context(
    in_name  IN VARCHAR2,
    in_value IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_context
       SET value = in_value
     WHERE name = UPPER(in_name);
    IF SQL%ROWCOUNT = 0 THEN
      INSERT INTO cort_context(name, value)
      VALUES(UPPER(in_name), in_value);
    END IF; 
    COMMIT;
  END write_context;

  -- Gets value from CORT context table
  FUNCTION read_context(
    in_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_value VARCHAR2(4000);
  BEGIN
    BEGIN
      SELECT value
        INTO l_value
        FROM cort_context
       WHERE name = UPPER(in_name);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_value := null;
    END;
    RETURN l_value;
  END read_context;

  -- Sets value to CORT context
  PROCEDURE set_context(
    in_name  IN VARCHAR2,
    in_value IN VARCHAR2
  )
  AS
    l_sql CLOB;
  BEGIN
    $IF cort_options_pkg.gc_use_context $THEN
      l_sql := 'begin '||cort_params_pkg.gc_context_setter||'; end;';       
      EXECUTE IMMEDIATE l_sql USING in_name, in_value;
    $ELSE  
      write_context(in_name, in_value);
    $END
  END set_context;

  -- Gets value of CORT context
  FUNCTION get_context(
    in_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    $IF cort_options_pkg.gc_use_context $THEN
      RETURN SYS_CONTEXT(cort_params_pkg.gc_context_name, in_name);
    $ELSE
      RETURN read_context(in_name);
    $END    
  END get_context;

  -- check application name is registered
  PROCEDURE check_application(
    in_application  IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_cnt number;
  BEGIN
    SELECT count(*)
      INTO l_cnt
      FROM cort_applications
     WHERE application = UPPER(in_application);
      
    IF l_cnt = 0 THEN
      --cort_exec_pkg.raise_error( 'Application  '||in_application||' is not registered in CORT_APPLICATIONS table');
      INSERT INTO cort_applications(application, application_name) VALUES(in_application, 'auto-created stub record');
    END IF; 
    COMMIT;
  END check_application;


  FUNCTION get_curr_release_rec(
    in_application IN VARCHAR2
  )
  RETURN cort_releases%ROWTYPE
  AS
    l_rec cort_releases%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_releases
       WHERE application = in_application
         AND end_date IS NULL;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    RETURN l_rec;
  END get_curr_release_rec;
  
  
  FUNCTION get_prev_release_rec(
    in_application IN VARCHAR2
  )
  RETURN cort_releases%ROWTYPE
  AS
    l_rec cort_releases%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_releases
       WHERE application = in_application
         AND end_date = (SELECT start_date 
                           FROM cort_releases 
                          WHERE application = in_application
                            AND end_date IS NULL);   
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    RETURN l_rec;
  END get_prev_release_rec;
  

  PROCEDURE register_new_release(
    in_application IN VARCHAR2,
    in_release     IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_sysdate date := sysdate;
  BEGIN
    UPDATE cort_releases
       SET end_date = l_sysdate
     WHERE application = in_application
       AND end_date is null;
    INSERT INTO cort_releases(application, release, start_date)
    VALUES (in_application, in_release, l_sysdate);
    
    COMMIT;
  END register_new_release;
  
  
  -- return compact unique string for current date-time  
  FUNCTION get_time_str
  RETURN VARCHAR2
  AS
    l_sysdate TIMESTAMP(2) := SYSTIMESTAMP;
  BEGIN
    RETURN TO_CHAR((TO_NUMBER(TO_CHAR(l_sysdate, 'J')) - TO_NUMBER(TO_CHAR(date'2001-01-01', 'J')))*8640000 + TO_NUMBER(TO_CHAR(l_sysdate, 'SSSSS'))*100 + TO_NUMBER(TO_CHAR(l_sysdate,'FF2')),'fm0XXXXXXXXX');     
  END get_time_str;

  -- Generates globally unique build ID
  FUNCTION gen_build_id
  RETURN VARCHAR2
  AS
  BEGIN                          --14                 4   
    RETURN TO_CHAR(systimestamp, 'yyyymmddhh24miss')||TO_CHAR(SYS_CONTEXT('USERENV','SID'),'fm0XXX');
  END gen_build_id;
  
  -- Find and return runnning or stale build for given application
  FUNCTION find_build(in_application IN VARCHAR2)
  RETURN cort_builds%ROWTYPE
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec 
        FROM cort_builds
       WHERE application = in_application
         AND DECODE(STATUS, 'RUNNING', '0', build) = '0'
         AND session_id = dbms_session.unique_session_id; 
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;  
    RETURN l_rec;
  END find_build;
  
  -- Return build record by build ID
  FUNCTION get_build_rec(
    in_build IN VARCHAR2
  )
  RETURN cort_builds%ROWTYPE
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec 
        FROM cort_builds
       WHERE build = in_build; 
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;  
    RETURN l_rec;
  END get_build_rec;
  
  -- wrapper returns 'TRUE'/'FALSE' 
  FUNCTION is_session_alive(in_session_id IN VARCHAR2)
  RETURN VARCHAR2 --  
  AS
  BEGIN
    RETURN CASE WHEN dbms_session.is_session_alive(in_session_id) THEN 'TRUE' ELSE 'FALSE' END; 
  END is_session_alive; 

  -- Adds new record into CORT_BUILDS table
  FUNCTION create_build(in_application IN VARCHAR2)
  RETURN cort_builds%ROWTYPE
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_rec cort_builds%ROWTYPE;
  BEGIN
    UPDATE cort_builds b
       SET status = 'STALE',
           end_time = SYSDATE
     WHERE status = 'RUNNING'
       AND cort_aux_pkg.is_session_alive(b.session_id) = 'FALSE';

    l_rec.build := gen_build_id;
    l_rec.application := in_application;
    l_rec.release := get_curr_release_rec(in_application).release;
    l_rec.status := 'RUNNING';
    l_rec.start_time := SYSDATE; 
    l_rec.session_id := dbms_session.unique_session_id;
    l_rec.username := user;
    l_rec.osuser := SYS_CONTEXT('USERENV','OS_USER'); 
    l_rec.machine := SYS_CONTEXT('USERENV','HOST');
    l_rec.terminal:= SYS_CONTEXT('USERENV','TERMINAL');
    l_rec.module := SYS_CONTEXT('USERENV','MODULE');
    INSERT INTO cort_builds VALUES l_rec;
    COMMIT;
    RETURN l_rec;
  END create_build;

  -- Update record in CORT_BUILDS table
  PROCEDURE update_build(
    in_row IN cort_builds%ROWTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE cort_builds 
       SET ROW = in_row
     WHERE build = in_row.build;
    COMMIT;
  END update_build;


  PROCEDURE obtain_grants(in_user_name in VARCHAR2)
  AS
    l_sql                varchar2(4000);
  BEGIN
    FOR x IN (SELECT object_name, object_type
                FROM (SELECT DISTINCT object_name, object_type
                        FROM user_procedures
                       WHERE object_name like 'CORT%'
                         AND object_type = 'PACKAGE'
                       UNION ALL
                      SELECT DISTINCT object_name, object_type
                        FROM user_objects
                       WHERE object_name like 'CORT%'
                         AND object_type IN ('TABLE','VIEW')
                         AND object_name IN (
                          'CORT_OBJECTS',
                          'CORT_JOBS',
                          'CORT_SQL',
                          'CORT_LOG',
                          'CORT_APPLICATIONS',
                          'CORT_RELEASES',
                          'CORT_BUILDS',
                          'CORT_RECENT_JOBS',
                          'CORT_RECENT_LOG',
                          'CORT_RECENT_OBJECTS',
                          'CORT_USER_PARAMS'
                        )
                      )
             ) 
    LOOP
      IF x.object_type = 'PACKAGE' THEN     
        l_sql := 'GRANT EXECUTE ON '||x.object_name||' TO "'||in_user_name||'"';
      ELSIF x.object_name in ('CORT_APPLICATIONS', 'CORT_RELEASES', 'CORT_BUILDS', 'CORT_USER_PARAMS') THEN
        l_sql := 'GRANT SELECT, INSERT, UPDATE, DELETE ON '||x.object_name||' TO "'||in_user_name||'"';
      ELSE
        l_sql := 'GRANT SELECT ON '||x.object_name||' TO "'||in_user_name||'"';
      END IF;  
      dbms_output.put_line(l_sql);
      execute immediate l_sql;
    END LOOP;

    
  END obtain_grants;

  -- revoke all grants on cort objects
  PROCEDURE revoke_grants(in_user_name in VARCHAR2)
  AS
    l_sql                varchar2(4000);
  BEGIN
    FOR x IN (SELECT *
                FROM user_tab_privs
               WHERE owner = SYS_CONTEXT('userenv', 'current_user')
                 AND table_name like 'CORT%'
                 AND grantee = in_user_name
                 AND grantor = SYS_CONTEXT('userenv', 'current_user')
                 AND table_name NOT IN ('CORT_AUX_PKG', 'CORT_PKG')
             ) 
    LOOP 
      l_sql := 'REVOKE '||x.privilege||' ON "'||x.owner||'".'||x.table_name||' FROM '||x.grantee; 
      dbms_output.put_line(l_sql);
      execute immediate l_sql;
    END LOOP;
  END;

  PROCEDURE save_sql(
    io_change_sql_arr IN OUT NOCOPY cort_exec_pkg.gt_change_arr
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_rec            cort_jobs%ROWTYPE;
    l_seq_num_arr    arrays.gt_num_arr;
    l_rowid_arr      arrays.gt_rowid_arr;
    l_indx           PLS_INTEGER;
  BEGIN
    l_rec := cort_exec_pkg.get_job_rec;
/*
    SELECT NVL(MAX(seq_num),0)
      INTO l_last_cnt
      FROM cort_sql
     WHERE job_id = l_rec.job_id; 
*/    
    FOR i IN 1..io_change_sql_arr.COUNT LOOP 
      IF io_change_sql_arr(i).row_id IS NULL THEN
--        cort_exec_pkg.debug('Change # '||i||' has logged under seq_num = '||i);
        l_seq_num_arr(i) := i;
      END IF;  
    END LOOP;  
--    g_sql_counter := g_sql_counter + in_change_sql_arr.COUNT;
    
    IF l_seq_num_arr.COUNT > 0 THEN 
      FORALL i IN INDICES OF l_seq_num_arr 
        INSERT INTO cort_sql(
          job_id, 
          object_type, 
          object_owner, 
          object_name, 
          seq_num, 
          status, 
          group_type, 
          change_sql, 
          revert_sql, 
          start_time, 
          end_time, 
          revert_start_time, 
          revert_end_time, 
          threadable, 
          error_msg)
        VALUES(
          l_rec.job_id, 
          l_rec.object_type, 
          l_rec.object_owner, 
          l_rec.object_name, 
          l_seq_num_arr(i), 
          io_change_sql_arr(i).status, 
          io_change_sql_arr(i).group_type, 
          io_change_sql_arr(i).change_sql, 
          io_change_sql_arr(i).revert_sql, 
          io_change_sql_arr(i).start_time, 
          io_change_sql_arr(i).end_time, 
          io_change_sql_arr(i).revert_start_time, 
          io_change_sql_arr(i).revert_end_time, 
          io_change_sql_arr(i).threadable, 
          io_change_sql_arr(i).error_msg)
        RETURNING rowid 
             BULK COLLECT 
             INTO l_rowid_arr;

      l_indx := 1;
      FOR i IN l_seq_num_arr.FIRST..l_seq_num_arr.LAST LOOP
        IF l_seq_num_arr.EXISTS(i) THEN
--          cort_exec_pkg.debug('Change # '||i||' has logged under rowid = '||l_rowid_arr(l_indx));
          io_change_sql_arr(i).row_id := l_rowid_arr(l_indx);
          l_indx := l_indx + 1;
        END IF;    
      END LOOP;
    END IF;
    
    COMMIT;      
  END save_sql;
      
  PROCEDURE read_sql(
    io_change_sql_arr IN OUT NOCOPY cort_exec_pkg.gt_change_arr
  )
  AS
  BEGIN
    FOR i IN 1..io_change_sql_arr.COUNT LOOP 
      IF io_change_sql_arr(i).row_id IS NOT NULL THEN
        SELECT status,
               start_time,
               end_time,
               revert_start_time,
               revert_end_time,
               error_msg
          INTO io_change_sql_arr(i).status,
               io_change_sql_arr(i).start_time, 
               io_change_sql_arr(i).end_time,
               io_change_sql_arr(i).revert_start_time, 
               io_change_sql_arr(i).revert_end_time,
               io_change_sql_arr(i).error_msg
           FROM cort_sql
         WHERE rowid = io_change_sql_arr(i).row_id
           AND io_change_sql_arr(i).status = 'REGISTERED';
      END IF;  
    END LOOP;  
  END read_sql;


  PROCEDURE update_sql(
    in_change_sql_arr IN cort_exec_pkg.gt_change_arr
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    FORALL i IN 1..in_change_sql_arr.COUNT 
      UPDATE cort_sql
         SET status            = in_change_sql_arr(i).status,
             start_time        = in_change_sql_arr(i).start_time, 
             end_time          = in_change_sql_arr(i).end_time,
             revert_start_time = in_change_sql_arr(i).revert_start_time, 
             revert_end_time   = in_change_sql_arr(i).revert_end_time,
             error_msg         = in_change_sql_arr(i).error_msg
       WHERE rowid = in_change_sql_arr(i).row_id;
    COMMIT;      
  END update_sql;


  PROCEDURE update_sql(
    in_change_sql_rec IN cort_exec_pkg.gt_change_rec
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    IF in_change_sql_rec.row_id IS NOT NULL THEN 
      UPDATE cort_sql
         SET status            = in_change_sql_rec.status,
             start_time        = in_change_sql_rec.start_time, 
             end_time          = in_change_sql_rec.end_time,
             revert_start_time = in_change_sql_rec.revert_start_time, 
             revert_end_time   = in_change_sql_rec.revert_end_time,
             error_msg         = in_change_sql_rec.error_msg
       WHERE rowid = in_change_sql_rec.row_id;
    END IF;   
    COMMIT;      
  END update_sql;


   FUNCTION get_explain_sql(in_xplan IN VARCHAR2) RETURN VARCHAR2
   AS
    l_sql_id       VARCHAR2(30);
    l_sql_arr      arrays.gt_lstr_arr;
    l_sql_text     CLOB;
   BEGIN
     $IF cort_options_pkg.gc_explain_plan $THEN
     BEGIN
       SELECT sql_id
         INTO l_sql_id
         FROM (SELECT sql_id,
                      MAX(DECODE(piece, 0, sql_text))||
                      MAX(DECODE(piece, 1, sql_text))||
                      MAX(DECODE(piece, 2, sql_text))||
                      MAX(DECODE(piece, 3, sql_text))||
                      MAX(DECODE(piece, 4, sql_text))||
                      MAX(DECODE(piece, 5, sql_text))||
                      MAX(DECODE(piece, 6, sql_text))||
                      MAX(DECODE(piece, 7, sql_text))||
                      MAX(DECODE(piece, 8, sql_text))||
                      MAX(DECODE(piece, 9, sql_text)) as sql_text
                 FROM v$sqltext_with_newlines
                WHERE command_type = 50
                GROUP BY sql_id
              )
        WHERE REGEXP_LIKE(sql_text, in_xplan, 'i');
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         l_sql_id := NULL;
       WHEN TOO_MANY_ROWS THEN
         l_sql_id := NULL;
     END;

     IF l_sql_id IS NULL THEN
       RETURN NULL;
     END IF;

     SELECT sql_text
        BULK COLLECT
        INTO l_sql_arr
       FROM v$sqltext_with_newlines
      WHERE command_type = 50
        AND sql_id = l_sql_id
      ORDER BY piece;

     lines_to_clob(
       in_str_arr  => l_sql_arr,
       in_delim    => NULL,
       out_clob    => l_sql_text
     );
     
     RETURN l_sql_text; 
     $ELSE
     RETURN NULL;
     $END
   END get_explain_sql;
      

END cort_aux_pkg;
/
