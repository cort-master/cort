CREATE OR REPLACE PACKAGE BODY cort_aux_pkg 
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
  19.00   | Rustam Kafarov    | Revised parameters 
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_counter             PLS_INTEGER := 0;


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
  
  -- return compact unique string for current date-time  
  FUNCTION get_systimestamp
  RETURN TIMESTAMP
  AS
    l_systime TIMESTAMP(9);
  BEGIN
    g_counter := g_counter + 1;
    IF g_counter > 999999 THEN
      g_counter := 0;
    END IF; 

    l_systime := SYSTIMESTAMP + TO_DSINTERVAL('PT0.'||TO_CHAR(g_counter, 'fm000000009')||'S');

    RETURN l_systime;
  END get_systimestamp;

  FUNCTION get_object_last_ddl_time(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN DATE
  AS
    l_last_ddl_time DATE;
  BEGIN
    BEGIN
      SELECT last_ddl_time
        INTO l_last_ddl_time 
        FROM all_objects
       WHERE owner = in_object_owner
         AND object_name = in_object_name
         AND object_type = in_object_type
         AND subobject_name IS NULL;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_last_ddl_time := NULL;
    END;
    RETURN l_last_ddl_time;
  END get_object_last_ddl_time;

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
               ORDER BY exec_time DESC NULLS LAST
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
    l_revert_name    cort_objects.revert_name%TYPE;
  BEGIN
    BEGIN
      SELECT revert_name
        INTO l_revert_name 
        FROM (SELECT * 
                FROM cort_objects 
               WHERE object_owner = in_object_owner 
                 AND object_name = in_object_name 
                 AND object_type = in_object_type
                 AND revert_name IS NOT NULL
               ORDER BY exec_time DESC
             )
       WHERE ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;    
  
    RETURN l_revert_name;
  END get_last_revert_name; 

  FUNCTION get_object_metadata(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_application   IN VARCHAR2, 
    in_release       IN VARCHAR2, 
    in_build         IN VARCHAR2 DEFAULT NULL 
  )
  RETURN gt_object_metadata
  AS
    l_object_metadata  gt_object_metadata;
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
     ORDER BY exec_time;
     
    RETURN l_object_metadata;
  END get_object_metadata; 

  PROCEDURE update_change(
    in_rec IN cort_objects%ROWTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
      -- update sql text and las_ddl_index
    UPDATE cort_objects 
       SET sql_text = in_rec.sql_text,
           application = in_rec.application, 
           release = in_rec.release,
           build = in_rec.build,
           last_ddl_index = in_rec.last_ddl_index,
           last_ddl_time = in_rec.last_ddl_time 
     WHERE object_owner = in_rec.object_owner 
       AND object_name = in_rec.object_name 
       AND object_type = in_rec.object_type
       AND exec_time = in_rec.exec_time;

    COMMIT;
  END update_change;

  -- register object change in cort-metadata table
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
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_old_rec       cort_objects%ROWTYPE;
    l_new_rec       cort_objects%ROWTYPE;
    l_xml           XMLType;
    l_change_type   VARCHAR2(30);
    l_last_ddl_time TIMESTAMP WITH TIME ZONE;
  BEGIN
    l_change_type := cort_comp_pkg.get_result_name(in_change_type);
    
    l_old_rec := get_last_change(
                   in_object_owner => in_object_owner,
                   in_object_type  => in_object_type,
                   in_object_name  => in_object_name
                 );
    l_last_ddl_time := get_object_last_ddl_time(
                         in_object_type  => in_object_type,
                         in_object_name  => in_object_name,
                         in_object_owner => in_object_owner
                       );
    IF in_change_type > cort_comp_pkg.gc_result_nochange OR -- actual change
       l_old_rec.object_name IS NULL -- no history for the object
    THEN
      l_new_rec.object_owner := in_object_owner; 
      l_new_rec.object_name := in_object_name; 
      l_new_rec.object_type := in_object_type;
      l_new_rec.sid := in_job_rec.sid;
      l_new_rec.exec_time := get_systimestamp;
      l_new_rec.sql_text := in_sql; 
      l_new_rec.last_ddl_time := greatest(systimestamp, l_last_ddl_time); 
      l_new_rec.change_type := l_change_type;
      l_new_rec.application := in_job_rec.application;
      l_new_rec.release := in_job_rec.release;
      l_new_rec.build := in_job_rec.build; 
      l_new_rec.revert_name := in_revert_name;
      l_new_rec.last_ddl_index := in_last_ddl_index;
      l_new_rec.rename_name := in_rename_name;
        

      IF in_change_type > cort_comp_pkg.gc_result_nochange AND
         in_frwd_stmt_arr.COUNT > 0 
      THEN
        cort_xml_pkg.write_to_xml(
          in_value => in_frwd_stmt_arr,
          out_xml  => l_xml 
        );
        l_new_rec.forward_ddl := l_xml.getClobVal();
      END IF;
          
      IF in_change_type > cort_comp_pkg.gc_result_nochange AND
         in_rlbk_stmt_arr.COUNT > 0 
      THEN
        cort_xml_pkg.write_to_xml(  
          in_value => in_rlbk_stmt_arr,
          out_xml  => l_xml 
        );
        l_new_rec.revert_ddl := l_xml.getClobVal();
      END IF;
        
      INSERT INTO cort_objects VALUES l_new_rec;
    ELSE
      -- just update sql text
      l_old_rec.sql_text := in_sql; 
      l_old_rec.sid := dbms_session.unique_session_id;
      l_old_rec.last_ddl_time := GREATEST(SYSTIMESTAMP, l_last_ddl_time); 
      l_old_rec.release := cort_pkg.get_current_release;  
      l_old_rec.build := SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12); 
      update_change(l_old_rec);
    END IF;
    
    COMMIT;
  END register_change;
  
  PROCEDURE unregister_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_exec_time     IN TIMESTAMP
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DELETE FROM cort_objects 
     WHERE object_owner = in_object_owner 
       AND object_name = in_object_name 
       AND object_type = in_object_type
       AND exec_time = in_exec_time;

    COMMIT;
  END unregister_change;

  -- Check if executed SQL and objects's last_ddl_time match to the last registered corresponded parameters 
  FUNCTION is_object_modified(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_sql_text     IN CLOB
  )
  RETURN BOOLEAN
  AS
    l_rec           cort_objects%ROWTYPE;
    l_result        BOOLEAN;
  BEGIN
    l_result := TRUE;
    l_rec := get_last_change(
               in_object_owner => in_object_owner,
               in_object_type  => in_object_type,
               in_object_name  => in_object_name
             );
    IF l_rec.sql_text = in_sql_text THEN
      IF l_rec.last_ddl_time = get_object_last_ddl_time(
                                  in_object_owner => in_object_owner, 
                                  in_object_name  => in_object_name,
                                  in_object_type  => in_object_type      
                                ) 
      THEN
        update_change(l_rec);
        l_result := FALSE;
      END IF;
    END IF;
    RETURN l_result;
  END is_object_modified;
  
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
  BEGIN
    l_result := TRUE;
    SELECT COUNT(*)
      INTO l_cnt 
      FROM cort_objects 
     WHERE object_owner = in_object_owner  
       AND object_name = in_object_name 
       AND object_type = in_object_type
       AND rename_name = in_rename_name
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
    l_cnt number;
  BEGIN
    SELECT count(*)
      INTO l_cnt
      FROM cort_applications
     WHERE application = UPPER(in_application);
      
    IF l_cnt = 0 THEN
      cort_exec_pkg.raise_error( 'Application  '||in_application||' is not registered in CORT_APPLICATIONS table');
    END IF; 
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
  BEGIN
    RETURN get_time_str||TO_CHAR(SYS_CONTEXT('USERENV','SID'),'fm0XXXXX');
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


END cort_aux_pkg;
/
