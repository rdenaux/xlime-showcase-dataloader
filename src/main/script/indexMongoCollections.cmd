@REM This file runs a .js script that adds indexes for xlimeres. A mongod instance is expected to be running.
@REM WARNING: if you execute the script while writing operations are running on the database they will be blocked
@REM          until the indexing is completed, and it might take a while.
@echo Calling js script
@REM Using default port and database path. Change them if needed.
START "JS" mongo localhost:27017/xlimeres indexes.js