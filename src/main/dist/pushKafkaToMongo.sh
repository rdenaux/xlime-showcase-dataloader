#!/bin/sh
# start java app, running various Kafka consumers with configuration file
echo "Starting xLiMe Kafka consumers to push various beans to Mongo"
java -server -Xss128k -Xms128m -Xmx256m -Dname=socmedia-loader -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-all-to-mongo-config.properties"
