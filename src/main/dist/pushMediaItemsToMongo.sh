#!/bin/sh
# start java app, running a Kafka consumer with configuration file
java -server -Xss128k -Xms128m -Xmx256m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-soc-media-to-mongo-config.properties"
