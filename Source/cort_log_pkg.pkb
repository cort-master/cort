CREATE OR REPLACE PACKAGE BODY cort_log_pkg 
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
  Description: Logging API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Removed dependency on v$session
  15.00   | Rustam Kafarov    | Added default session_id
  17.00   | Rustam Kafarov    | Added columns current_schema, new_name; Replaced TIMESTAMP with timestamp_tz_unconstrained
  19.00   | Rustam Kafarov    | Added logging procedures for different log types
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_counter         PLS_INTEGER := 0;
  g_job_rec         cort_jobs%ROWTYPE;
  g_row_id          rowid;
  g_start_time      TIMESTAMP;
  g_timer_rowid_arr arrays.gt_rowid_indx;
  g_timer_time_arr  arrays.gt_timestamp_indx;
  g_error_stack     CLOB;
  
  PROCEDURE init_log(
    in_job_rec  IN cort_jobs%ROWTYPE
  )
  AS
  BEGIN
    g_job_rec := in_job_rec;
    g_counter := 0; 
    g_row_id := null;
    g_timer_rowid_arr.DELETE;
    g_timer_time_arr.DELETE;
    g_error_stack := NULL;
  END init_log;  
  
  -- returns error stack
  FUNCTION get_error_stack
  RETURN CLOB
  AS
  BEGIN
    RETURN g_error_stack;
  END get_error_stack;


  -- Logs every CORT high level operation
  PROCEDURE log_job(
    in_status   IN VARCHAR2,
    in_job_rec  IN cort_jobs%ROWTYPE
  )
  AS
  PRAGMA autonomous_transaction;
    l_log_rec     cort_job_log%ROWTYPE;
  BEGIN
    g_job_rec := in_job_rec;
    g_counter := g_counter + 1;
    IF g_counter >= 1000 THEN
      g_counter := 0;
    END IF;
    l_log_rec.job_log_time        := SYSTIMESTAMP + TO_DSINTERVAL('PT0.'||TO_CHAR(g_counter, 'fm000000009')||'S');
    l_log_rec.sid                 := NVL(in_job_rec.sid, dbms_session.unique_session_id);
    l_log_rec.action              := in_job_rec.action;
    l_log_rec.status              := in_status;
    l_log_rec.job_owner           := in_job_rec.job_owner;
    l_log_rec.job_name            := in_job_rec.job_name;
    l_log_rec.job_sid             := in_job_rec.job_sid; 
    l_log_rec.job_time            := in_job_rec.job_time; 
    l_log_rec.object_type         := in_job_rec.object_type; 
    l_log_rec.object_owner        := in_job_rec.object_owner;
    l_log_rec.object_name         := in_job_rec.object_name; 
    l_log_rec.sql_text            := in_job_rec.sql_text;
    l_log_rec.new_name            := in_job_rec.new_name;
    l_log_rec.current_schema      := in_job_rec.current_schema;
    l_log_rec.application         := in_job_rec.application;     
    l_log_rec.release             := in_job_rec.release;        
    l_log_rec.build               := in_job_rec.build;          
    l_log_rec.session_params      := in_job_rec.session_params;
    l_log_rec.parent_object_type  := in_job_rec.parent_object_type;
    l_log_rec.parent_object_owner := in_job_rec.parent_object_owner; 
    l_log_rec.parent_object_name  := in_job_rec.parent_object_name;  
    l_log_rec.output              := in_job_rec.output;
    l_log_rec.session_id          := in_job_rec.session_id;    
    l_log_rec.username            := in_job_rec.username;      
    l_log_rec.osuser              := in_job_rec.osuser;        
    l_log_rec.machine             := in_job_rec.machine;       
    l_log_rec.terminal            := in_job_rec.terminal;      
    l_log_rec.module              := in_job_rec.module;        
    l_log_rec.error_code          := in_job_rec.error_code;
    l_log_rec.error_message       := in_job_rec.error_message;
    l_log_rec.error_backtrace     := in_job_rec.error_backtrace;
    l_log_rec.error_stack         := g_error_stack;
    
    -- insert new record into log table 
    INSERT INTO cort_job_log
    VALUES l_log_rec; 
    
    COMMIT; 
  END log_job;

  -- Generic logging function
  FUNCTION int_log(
    in_log_type       IN VARCHAR2,
    in_text           IN VARCHAR2,
    in_details        IN CLOB,
    in_error_message  IN VARCHAR2 DEFAULT NULL,
    in_error_stack    IN VARCHAR2 DEFAULT dbms_utility.format_error_backtrace,
    in_call_stack     IN VARCHAR2 DEFAULT dbms_utility.format_call_stack,
    in_execution_time IN INTERVAL DAY TO SECOND DEFAULT NULL
    
  )
  RETURN rowid
  AS
  PRAGMA autonomous_transaction;
    l_log_rec      cort_log%ROWTYPE;
    l_rowid        rowid;
  BEGIN
    g_counter := g_counter + 1;
    IF g_counter >= 1000 THEN
      g_counter := 0;
    END IF;

    l_log_rec.log_time        := SYSTIMESTAMP + TO_DSINTERVAL('PT0.'||TO_CHAR(g_counter, 'fm000000009')||'S');
    l_log_rec.sid             := NVL(g_job_rec.sid, dbms_session.unique_session_id);
    l_log_rec.job_time        := g_job_rec.job_time;
    l_log_rec.action          := g_job_rec.action;
    l_log_rec.object_owner    := g_job_rec.object_owner;
    l_log_rec.object_name     := g_job_rec.object_name; 
    l_log_rec.log_type        := in_log_type;
    l_log_rec.text            := in_text;
    l_log_rec.details         := in_details;
    l_log_rec.error_message   := in_error_message;
    l_log_rec.error_stack     := in_error_stack;
    l_log_rec.call_stack      := in_call_stack;

    l_log_rec.execution_time  := in_execution_time; 
    
    INSERT INTO cort_log
    VALUES l_log_rec 
    RETURNING rowid INTO l_rowid;
    
    COMMIT;
    
    RETURN l_rowid;
  END int_log;
  
  PROCEDURE execute(
    in_text     IN CLOB
  )
  AS
  BEGIN
    g_row_id := int_log(
                  in_log_type     => 'EXECUTE',
                  in_text         => substr(in_text, 1, 4000),
                  in_details      => in_text 
                );
    g_start_time := systimestamp;            
  END execute;

  PROCEDURE update_exec_time
  AS
  PRAGMA autonomous_transaction;
  BEGIN
    UPDATE cort_log
       SET execution_time = systimestamp - g_start_time
     WHERE rowid = g_row_id;
        
    g_row_id := NULL;
    g_start_time := NULL;
    
    COMMIT;
  END  update_exec_time;


  PROCEDURE test(
    in_text  IN CLOB
  )
  AS
    l_row_id       rowid;
  BEGIN
    l_row_id := int_log(
                  in_log_type     => 'TEST',
                  in_text         => substr(in_text, 1, 4000),
                  in_details      => in_text 
                );
  END test;

  PROCEDURE revert(
    in_text  IN CLOB
  )
  AS
    l_row_id       rowid;
  BEGIN
    l_row_id := int_log(
                  in_log_type     => 'REVERT',
                  in_text         => substr(in_text, 1, 4000),
                  in_details      => in_text 
                );
  END revert;


  PROCEDURE echo(
    in_text      IN CLOB
  )
  AS
    l_row_id       rowid;
  BEGIN
    l_row_id := int_log(
                  in_log_type  => 'ECHO',
                  in_text      => substr(in_text, 1, 4000),
                  in_details   => in_text
                );
  END echo;

  PROCEDURE error(
    in_text      IN VARCHAR2,
    in_details   IN CLOB DEFAULT NULL
  )
  AS
    l_row_id       rowid;
  BEGIN
    g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
    
    l_row_id := int_log(
                  in_log_type      => 'ERROR',
                  in_text          => in_text,
                  in_details       => in_details,
                  in_error_message => sqlerrm 
                );
                
  END error;

  PROCEDURE debug(
    in_text      IN VARCHAR2,
    in_details   IN CLOB DEFAULT NULL
  )
  AS
    l_row_id       rowid;
  BEGIN
    l_row_id := int_log(
                  in_log_type     => 'DEBUG',
                  in_text         => in_text, 
                  in_details      => in_details
                );
  END debug;

  PROCEDURE start_timer(
    in_text      IN VARCHAR2
  )
  AS
  BEGIN
    g_timer_rowid_arr(in_text) := 
      int_log(
        in_log_type     => 'TIMER (START)',
        in_text         => in_text,
        in_details      => null 
      );
    g_timer_time_arr(in_text) := systimestamp;            
  END start_timer;
  
  PROCEDURE stop_timer(
    in_text      IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
    l_rowid        rowid;
    l_exec_time    INTERVAL DAY TO SECOND(9);
  BEGIN
    IF g_timer_time_arr.EXISTS(in_text) THEN
      
      l_exec_time := systimestamp - g_timer_time_arr(in_text);
    
      UPDATE cort_log
         SET execution_time = l_exec_time
       WHERE rowid = g_timer_rowid_arr(in_text);
          
      g_timer_rowid_arr(in_text) := NULL;
      g_timer_time_arr(in_text) := NULL;
      
      l_rowid := int_log(
                   in_log_type       => 'TIMER (END)',
                   in_text           => in_text,
                   in_details        => null,
                   in_execution_time => l_exec_time 
                 );
    END IF;  
    
    COMMIT;
  END stop_timer;

END cort_log_pkg;
/
