#!/bin/sh
#This file runs a JS script that adds indexes to xlimeres' collections. A mongod instance is expected to be running.
#WARNING: if you execute the script while writing operations are running on the database they will be blocked
#		  until the indexing is completed, and it might take a while.
mongo localhost:27017/xlimeres indexes.js