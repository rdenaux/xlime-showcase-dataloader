#!/bin/sh
#Place this script on mongoDB installation directory. It runs an instance of mongo daemon and runs a JS script that adds indexes to xlimeres' collections
mongod --dbpath "$MONGO_HOME/data"
mongo localhost:27017/xlimeres indexes.js