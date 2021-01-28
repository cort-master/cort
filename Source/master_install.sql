/*
CORT - Oracle database DevOps tool

Copyright (C) 2013  Softcraft Ltd - Rustam Kafarov

www.cort.tech
master@@cort.tech

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
  Description: standard install script. 
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  19.07   | Rustam Kafarov    | Simple full install script
  21.01   | Rustam Kafarov    | included PL/SQL Utilities
  ----------------------------------------------------------------------------------------------------------------------  
*/

   

-- PL/SQL utilities
@plsql_utilities/arrays.pks
@plsql_utilities/partition_utils.pks
@plsql_utilities/xml_utils.pks

@plsql_utilities/arrays.pkb
@plsql_utilities/partition_utils.pkb
@plsql_utilities/xml_utils.pkb


@drop_triggers.sql

-- tables 
@tables/cort_applications.sql
@tables/cort_builds.sql
@tables/cort_context.sql
@tables/cort_job_control.sql
@tables/cort_job_log.sql
@tables/cort_jobs.sql
@tables/cort_lob.sql
@tables/cort_log.sql
@tables/cort_objects.sql
@tables/cort_params.sql
@tables/cort_releases.sql
@tables/cort_stat.sql
@tables/plan_table.sql

-- params
@cort_params.sql

-- Types
@cort_param_obj.tps
@cort_param_obj.tpb

-- Package specs
@cort_options_pkg.pks
@cort_log_pkg.pks
@cort_params_pkg.pks
@cort_exec_pkg.pks
@cort_comp_pkg.pks
@cort_parse_pkg.pks
@cort_xml_pkg.pks
@cort_aux_pkg.pks
@cort_pkg.pks
@cort_session_pkg.pks
@cort_trg_pkg.pks
@cort_job_pkg.pks
@cort_event_exec_pkg.pks

-- Package bodies
@cort_log_pkg.pkb
@cort_params_pkg.pkb
@cort_exec_pkg.pkb
@cort_comp_pkg.pkb
@cort_parse_pkg.pkb
@cort_xml_pkg.pkb
@cort_aux_pkg.pkb
@cort_pkg.pkb
@cort_session_pkg.pkb
@cort_trg_pkg.pkb
@cort_job_pkg.pkb
@cort_event_exec_pkg.pkb

-- Views
@cort_recent_objects.sql
@cort_recent_jobs.sql

-- Triggers
@cort_create_trg.trg
@cort_before_create_trg.trg
@cort_lock_object_trg.trg
@cort_before_xplan_trg.trg

exit