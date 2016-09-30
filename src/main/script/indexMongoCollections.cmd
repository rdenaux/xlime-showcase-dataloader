@REM Place this script on mongoDB installation directory. It runs an instance of mongo daemon and a .js script that adds indexes for xlimeres. Change --dbpath arguments if needed.
@echo Connecting to mongo
START "Connecting" mongod.exe --dbpath "C:\Program Files\MongoDB\data"
@echo Calling js script
START "JS" mongo localhost:27017/xlimeres indexes.js