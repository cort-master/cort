@echo off
REM   
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

set connection_string=cort/cort@orcl

cd source

sqlplus %connection_string% @master_install.sql

echo.
echo.
pause