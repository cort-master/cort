@echo off


REM   CORT - Oracle database DevOps tool
REM 
REM   Copyright (C) 2013  Softcraft Ltd - Rustam Kafarov
REM 
REM   www.cort.tech
REM   master@cort.tech
REM 
REM   This program is free software: you can redistribute it and/or modify
REM   it under the terms of the GNU General Public License as published by
REM   the Free Software Foundation, either version 3 of the License, or
REM   (at your option) any later version.
REM 
REM   This program is distributed in the hope that it will be useful,
REM   but WITHOUT ANY WARRANTY; without even the implied warranty of
REM   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
REM   GNU General Public License for more details.
REM 
REM   You should have received a copy of the GNU General Public License
REM   along with this program.  If not, see <http://www.gnu.org/licenses/>.

ECHO Required privileges:
ECHO   CREATE SESSION, CREATE TABLE, CREATE INDEX, CREATE PROCEDURE, CREATE TRIGGER, CREATE TYPE, CREATE VIEW, CREATE JOB
ECHO   EXECUTE ON SYS.DBMS_LOB, SYS.DBMS_LOCK, SYS.DBMS_SESSION, SYS.DBMS_SQL, SYS.DBML_RLS
ECHO   SELECT ON SYS.V_$PARAMETER, SYS.GV_SESSION
ECHO Optional privileges: 
ECHO   SELECT ON GV_$SQLTEXT_WITH_NEWLINES (for explain plan support)
ECHO   SELECT ON DBA_OBJECTS, DBA_SEGMENTS, DBA_CLU_COLUMNS (for creating lobs/cluster tables in different schema) 

for /D %%I in (%0%) do (
  set root_path=%%~dpI
)

SET default_instance=orcl
SET default_schema=cort_git

SET db_inst=%default_instance%
SET cort_user=%default_schema%

ECHO Enter Database service/instance name
SET /P db_inst="[%default_instance%] > "

ECHO Enter user for CORT schema
SET /P cort_user="[%default_schema%] > " 

ECHO Connecting as %cort_user% ...

cd "%root_path%Source

sqlplus %cort_user%@%db_inst% @master_install.sql

echo.
echo.
pause